package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// CompleteDraw also runs after a restart; unused reserves do not keep a settled draw open.
func (s Store) CompleteDraw(ctx context.Context) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id int64
	err = tx.QueryRow(ctx, `SELECT d.id FROM draws d WHERE d.status='approved'
 AND NOT EXISTS (SELECT 1 FROM winners w WHERE w.draw_id=d.id AND w.state IN ('pending_notification','notified','unreachable'))
 AND NOT EXISTS (SELECT 1 FROM winners w WHERE w.draw_id=d.id AND w.state='expired' AND w.reserves_exhausted_at IS NULL AND NOT EXISTS (SELECT 1 FROM winners r WHERE r.replaces_winner_id=w.id))
 ORDER BY d.id FOR UPDATE OF d SKIP LOCKED LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Expiration holds the same draw lock before assigning a reserve.
	if _, err = tx.Exec(ctx, `UPDATE draws SET status='completed' WHERE id=$1`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// A bounded page keeps administrator replies within MAX's message length limit.
func DrawStatus(ctx context.Context, tx pgx.Tx, id, after int64) (string, error) {
	var kind, status string
	var drawn time.Time
	err := tx.QueryRow(ctx, `SELECT kind,status,drawn_at FROM draws WHERE id=$1`, id).Scan(&kind, &status, &drawn)
	if errors.Is(err, pgx.ErrNoRows) {
		return "Розыгрыш не найден.", nil
	}
	if err != nil {
		return "", err
	}
	var pending, notified, claimed, expired, reserves, unreachable, exhausted int
	err = tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE state='pending_notification'),count(*) FILTER (WHERE state='notified'),count(*) FILTER (WHERE state='claimed'),count(*) FILTER (WHERE state='expired'),count(*) FILTER (WHERE state='reserve'),count(*) FILTER (WHERE state='unreachable'),count(*) FILTER (WHERE reserves_exhausted_at IS NOT NULL) FROM winners WHERE draw_id=$1`, id).Scan(&pending, &notified, &claimed, &expired, &reserves, &unreachable, &exhausted)
	if err != nil {
		return "", err
	}
	text := fmt.Sprintf("Розыгрыш #%d (%s): %s\nПроведен: %s\nОжидают отправки: %d; ожидают ответа: %d; подтвердили: %d; срок истек: %d; неактивный резерв: %d.\nНедоступные получатели: %d; мест без резерва: %d.\n", id, kind, status, drawn.UTC().Format(time.RFC3339), pending, notified, claimed, expired, reserves, unreachable, exhausted)
	rows, err := tx.Query(ctx, `SELECT w.id,w.user_id,w.role,w.state,w.deadline,w.reserves_exhausted_at IS NOT NULL,w.replaces_winner_id,COALESCE(n.status,''),COALESCE(n.attempts,0) FROM winners w LEFT JOIN winner_notifications n ON n.winner_id=w.id WHERE w.draw_id=$1 AND w.id>$2 ORDER BY w.id LIMIT 11`, id, after)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var last int64
	var n int
	for rows.Next() {
		var winner, user int64
		var role, state, delivery string
		var deadline *time.Time
		var exhausted bool
		var replaces *int64
		var attempts int
		if err = rows.Scan(&winner, &user, &role, &state, &deadline, &exhausted, &replaces, &delivery, &attempts); err != nil {
			return "", err
		}
		if n == 10 {
			text += fmt.Sprintf("Следующая страница: /draw_status %d %d\n", id, last)
			break
		}
		n++
		last = winner
		text += fmt.Sprintf("#%d user:%d %s %s", winner, user, role, state)
		if delivery != "" {
			text += fmt.Sprintf("; доставка:%s, попыток:%d", delivery, attempts)
		}
		if deadline != nil && state == "notified" {
			text += "; до " + deadline.UTC().Format(time.RFC3339)
		}
		if replaces != nil {
			text += fmt.Sprintf("; замена #%d", *replaces)
		}
		if exhausted {
			text += "; РЕЗЕРВ ИСЧЕРПАН"
		}
		text += "\n"
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if unreachable > 0 {
		text += "Недоступным получателям уведомление не доставлено; срок ответа не начат. Требуется решение по правилам акции.\n"
	}
	if exhausted > 0 {
		text += "Новая выборка автоматически не проводится; требуется решение по правилам акции."
	}
	return strings.TrimSpace(text), nil
}
