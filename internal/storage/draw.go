package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
)

// selectDaily uses unbiased partial Fisher–Yates selection. Production uses crypto/rand.
func selectDaily(users []int64, n int, source io.Reader) ([]int64, error) {
	if n < 0 || n > len(users) {
		return nil, errors.New("invalid selection size")
	}
	result := append([]int64(nil), users...)
	for i := 0; i < n; i++ {
		offset, err := rand.Int(source, big.NewInt(int64(len(result)-i)))
		if err != nil {
			return nil, err
		}
		j := i + int(offset.Int64())
		result[i], result[j] = result[j], result[i]
	}
	return result[:n], nil
}

type DailyDrawResult struct {
	ID      int64
	Message string
}

// DrawDaily commits with the caller's event transaction, including its reply.
// A partner lock serializes commands and closes the registration race.
func DrawDaily(ctx context.Context, tx pgx.Tx, partner int64, now time.Time, loc *time.Location) (DailyDrawResult, error) {
	if loc == nil {
		return DailyDrawResult{}, errors.New("festival timezone is required")
	}
	var id int64
	if partner == 0 {
		err := tx.QueryRow(ctx, `SELECT id FROM partners WHERE festival_day=$1::date`, now.In(loc).Format(time.DateOnly)).Scan(&partner)
		if errors.Is(err, pgx.ErrNoRows) {
			return DailyDrawResult{Message: "Сегодня нет партнера. Укажите /draw_daily partner_id."}, nil
		}
		if err != nil {
			return DailyDrawResult{}, err
		}
	}
	var closes time.Time
	var winners, reserves int
	var raw []byte
	err := tx.QueryRow(ctx, `SELECT entry_closes_at,winner_count,reserve_count,prizes FROM partners WHERE id=$1 FOR UPDATE`, partner).Scan(&closes, &winners, &reserves, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return DailyDrawResult{Message: "Партнер не найден."}, nil
	}
	if err != nil {
		return DailyDrawResult{}, err
	}
	var status string
	err = tx.QueryRow(ctx, `SELECT id,status FROM draws WHERE kind='daily' AND partner_id=$1`, partner).Scan(&id, &status)
	if err == nil {
		return DailyDrawResult{ID: id, Message: fmt.Sprintf("Розыгрыш #%d уже сохранен (%s). Повторный выбор не выполняется.", id, status)}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return DailyDrawResult{}, err
	}
	if now.Before(closes) {
		return DailyDrawResult{Message: "Прием заявок еще открыт. Розыгрыш доступен после закрытия."}, nil
	}
	// Restore existing durable confirmations, including permanent failures, before selection.
	// Lock ordering matches DeliverOne: outgoing first, then daily_entries.
	if _, err = tx.Exec(ctx, `UPDATE outgoing o SET status='pending',attempts=0,available_at=now()
 FROM daily_entries e WHERE o.daily_entry_id=e.id AND e.partner_id=$1
 AND e.confirmation_status<>'sent' AND o.status='failed'`, partner); err != nil {
		return DailyDrawResult{}, err
	}
	var pending int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM daily_entries WHERE partner_id=$1 AND confirmation_status<>'sent'`, partner).Scan(&pending); err != nil {
		return DailyDrawResult{}, err
	}
	if pending > 0 {
		return DailyDrawResult{Message: fmt.Sprintf("Розыгрыш отложен: %d подтверждений регистрации не доставлены. Отправка восстановлена; повторите /draw_daily %d после доставки. Если получатель недоступен, требуется решение администратора по правилам акции.", pending, partner)}, nil
	}
	rows, err := tx.Query(ctx, `SELECT e.user_id,jsonb_build_object('entry_id',e.id,'registered_at',e.registered_at,
 'subscription_checked_at',e.subscription_checked_at,'confirmed_at',e.confirmed_at,
 'partner_id',p.id,'partner_name',p.name,'festival_day',p.festival_day,
 'first_name',u.first_name,'last_name',u.last_name,'username',u.username)
 FROM daily_entries e JOIN users u ON u.id=e.user_id JOIN partners p ON p.id=e.partner_id
 WHERE e.partner_id=$1 AND e.confirmation_status='sent' AND e.confirmed_at IS NOT NULL
 AND e.subscription_checked_at<=e.registered_at AND e.registered_at<p.entry_closes_at ORDER BY e.user_id`, partner)
	if err != nil {
		return DailyDrawResult{}, err
	}
	var users []int64
	snapshots := map[int64][]byte{}
	for rows.Next() {
		var user int64
		var snapshot []byte
		if err = rows.Scan(&user, &snapshot); err != nil {
			rows.Close()
			return DailyDrawResult{}, err
		}
		users = append(users, user)
		snapshots[user] = snapshot
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return DailyDrawResult{}, err
	}
	// No partial draw: preserve configured prizes and reserve count until rules say otherwise.
	if len(users) < winners+reserves {
		return DailyDrawResult{Message: fmt.Sprintf("Недостаточно участников: %d, требуется %d основных и %d резервных. Розыгрыш не создан; необходимо согласовать правила нехватки кандидатов.", len(users), winners, reserves)}, nil
	}
	var prizes []Prize
	if err = json.Unmarshal(raw, &prizes); err != nil {
		return DailyDrawResult{}, err
	}
	byPlace := map[int]Prize{}
	for _, prize := range prizes {
		if prize.Place < 1 || prize.Place > winners || prize.Quantity < 1 || prize.Name == "" {
			return DailyDrawResult{}, errors.New("invalid prize allocation")
		}
		byPlace[prize.Place] = prize
	}
	if len(prizes) != winners || len(byPlace) != winners {
		return DailyDrawResult{}, errors.New("invalid prize allocation")
	}
	selected, err := selectDaily(users, winners+reserves, rand.Reader)
	if err != nil {
		return DailyDrawResult{}, err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO draws(kind,partner_id,status,drawn_at) VALUES('daily',$1,'pending_approval',$2) RETURNING id`, partner, now).Scan(&id); err != nil {
		return DailyDrawResult{}, err
	}
	for _, user := range users {
		if _, err = tx.Exec(ctx, `INSERT INTO draw_participants(draw_id,user_id,weight,snapshot) VALUES($1,$2,1,$3)`, id, user, snapshots[user]); err != nil {
			return DailyDrawResult{}, err
		}
	}
	for i, user := range selected {
		role, state := "primary", "pending_notification"
		var order any
		var prizeName, prizeQuantity any
		if i < winners {
			prize := byPlace[i+1]
			prizeName, prizeQuantity = prize.Name, prize.Quantity
		} else {
			// Reserve inherits a concrete prize only when activated (MR3 step 8).
			role, state, order = "reserve", "reserve", i-winners+1
		}
		if _, err = tx.Exec(ctx, `INSERT INTO winners(draw_id,user_id,role,reserve_order,prize,prize_quantity,state)
   VALUES($1,$2,$3,$4,$5,$6,$7)`, id, user, role, order, prizeName, prizeQuantity, state); err != nil {
			return DailyDrawResult{}, err
		}
	}
	return DailyDrawResult{ID: id, Message: fmt.Sprintf("Розыгрыш #%d сохранен: %d участников, %d основных и %d резервных. Статус: pending_approval. Уведомления победителям ожидают утверждения.", id, len(users), winners, reserves)}, nil
}
