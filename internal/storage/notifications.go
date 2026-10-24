package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"example.com/max-festival-bot/internal/maxapi"
	"github.com/jackc/pgx/v5"
)

// ApproveDraw runs in the inbox transaction, together with the administrator's reply.
func ApproveDraw(ctx context.Context, tx pgx.Tx, id, admin int64, now time.Time, grandResponse int, grandRecipient string) (string, error) {
	var status, kind string
	var partner *int64
	err := tx.QueryRow(ctx, `SELECT status,kind,partner_id FROM draws WHERE id=$1 FOR UPDATE`, id).Scan(&status, &kind, &partner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "Розыгрыш не найден.", nil
	}
	if err != nil {
		return "", err
	}
	if status != "pending_approval" {
		return fmt.Sprintf("Розыгрыш #%d уже утвержден (%s).", id, status), nil
	}
	var sealed bool
	if err = tx.QueryRow(ctx, `SELECT selection_snapshot IS NOT NULL FROM draws WHERE id=$1`, id).Scan(&sealed); err != nil {
		return "", err
	}
	if !sealed {
		return "Результат розыгрыша не зафиксирован. Утверждение недоступно.", nil
	}
	seconds := grandResponse
	recipient := strings.TrimSpace(grandRecipient)
	name, instructions := "", "Нажми «Получить приз», чтобы подтвердить получение и оставить необходимые данные."
	if kind == "daily" {
		if err = tx.QueryRow(ctx, `SELECT name,claim_instructions,response_seconds,data_recipient FROM partners WHERE id=$1`, partner).Scan(&name, &instructions, &seconds, &recipient); err != nil {
			return "", err
		}
	}
	if seconds <= 0 {
		return "Задайте согласованный срок ответа GRAND_RESPONSE_SECONDS перед утверждением финала.", nil
	}
	if recipient == "" {
		return "Задайте получателя данных: data_recipient партнера или GRAND_DATA_RECIPIENT для финала перед утверждением.", nil
	}
	rows, err := tx.Query(ctx, `SELECT id,prize,prize_quantity FROM winners WHERE draw_id=$1 AND state='pending_notification' ORDER BY id`, id)
	if err != nil {
		return "", err
	}
	type notification struct {
		id   int64
		body []byte
	}
	var jobs []notification
	for rows.Next() {
		var winner int64
		var prize string
		var quantity int
		if err = rows.Scan(&winner, &prize, &quantity); err != nil {
			break
		}
		text := fmt.Sprintf("Ты в числе хедлайнеров дня 🔥\nТы выиграл %s от %s. Количество: %d.\nЧтобы получить приз: %s", prize, name, quantity, instructions)
		if kind == "grand" {
			text = fmt.Sprintf("Ты в числе победителей главного розыгрыша 🔥\nТвой приз: %s. Количество: %d (два билета).\n%s", prize, quantity, instructions)
		}
		if kind == "daily" {
			text += "\nНажми «Получить приз», введи имя и фамилию и подтверди согласие на передачу данных партнеру."
		}
		text += fmt.Sprintf("\nСрок ответа после доставки уведомления: %d секунд.", seconds)
		if utf8.RuneCountInString(text) > 4000 {
			rows.Close()
			return "Текст уведомления превышает лимит MAX (4000 символов). Сократите название приза, партнера или инструкцию и повторите утверждение.", nil
		}
		raw, e := json.Marshal(maxapi.Body{Text: text, Attachments: []maxapi.Attachment{maxapi.Keyboard(maxapi.Callback("Получить приз", fmt.Sprintf("claim:start:%d", winner)))}})
		if e != nil {
			err = e
			break
		}
		jobs = append(jobs, notification{winner, raw})
	}
	rowErr := rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	if rowErr != nil {
		return "", rowErr
	}
	if len(jobs) == 0 {
		return "Нет основных победителей для уведомления.", nil
	}
	if _, err = tx.Exec(ctx, `UPDATE draws SET status='approved',approved_at=$2,approved_by=$3 WHERE id=$1`, id, now, admin); err != nil {
		return "", err
	}
	for _, job := range jobs {
		if _, err = tx.Exec(ctx, `INSERT INTO winner_notifications(winner_id,body,response_seconds,data_recipient) VALUES($1,$2,$3,$4)`, job.id, job.body, seconds, recipient); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("Розыгрыш #%d утвержден. Сохранено уведомлений: %d.", id, len(jobs)), nil
}

type NotificationQueue interface {
	EnqueueWinner(context.Context, int64) error
}

// Keep redispatching until PostgreSQL records delivery, including after Redis data loss.
func (s Store) DispatchNotification(ctx context.Context, queue NotificationQueue) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id int64
	err = tx.QueryRow(ctx, `SELECT winner_id FROM winner_notifications WHERE status='pending' AND next_enqueue_at<=now() ORDER BY next_enqueue_at,winner_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = queue.EnqueueWinner(ctx, id); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE winner_notifications SET next_enqueue_at=now()+interval '1 minute' WHERE winner_id=$1`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

type WinnerSender interface {
	SendUser(context.Context, int64, int64, maxapi.Body) error
}

func (s Store) NotifyWinner(ctx context.Context, id int64, sender WinnerSender) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var status string
	var raw []byte
	var seconds int
	err = tx.QueryRow(ctx, `SELECT status,body,response_seconds FROM winner_notifications WHERE winner_id=$1 FOR UPDATE`, id).Scan(&status, &raw, &seconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if status != "pending" {
		return nil
	}
	var user, chat int64
	var state, drawStatus string
	if err = tx.QueryRow(ctx, `SELECT w.user_id,COALESCE(u.private_chat_id,0),w.state,d.status FROM winners w JOIN users u ON u.id=w.user_id JOIN draws d ON d.id=w.draw_id WHERE w.id=$1 FOR UPDATE OF w`, id).Scan(&user, &chat, &state, &drawStatus); err != nil {
		return err
	}
	if state != "pending_notification" || drawStatus != "approved" {
		return nil
	}
	var body maxapi.Body
	if err = json.Unmarshal(raw, &body); err != nil {
		return err
	}
	sendErr := sender.SendUser(ctx, user, chat, body)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if sendErr != nil {
		var api *maxapi.APIError
		permanent := errors.As(sendErr, &api) && api.Status >= 400 && api.Status < 500 && api.Status != 401 && api.Status != 408 && api.Status != 429 && api.Code != "attachment.not.ready"
		if !permanent {
			if _, err = tx.Exec(ctx, `UPDATE winner_notifications SET attempts=attempts+1 WHERE winner_id=$1`, id); err != nil {
				return err
			}
			if err = tx.Commit(ctx); err != nil {
				return err
			}
			return sendErr
		}
		if _, err = tx.Exec(ctx, `UPDATE winners SET state='unreachable' WHERE id=$1`, id); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE winner_notifications SET status='failed',attempts=attempts+1 WHERE winner_id=$1`, id); err != nil {
			return err
		}
	} else {
		// PostgreSQL time is sampled after the API succeeds, never at enqueue/approval time.
		if _, err = tx.Exec(ctx, `UPDATE winners SET state='notified',notified_at=clock_timestamp(),deadline=clock_timestamp()+$2*interval '1 second' WHERE id=$1`, id, seconds); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE winner_notifications SET status='sent',sent_at=(SELECT notified_at FROM winners WHERE id=$1),attempts=attempts+1 WHERE winner_id=$1`, id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
