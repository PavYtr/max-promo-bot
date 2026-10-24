package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
)

type weightedUser struct {
	ID     int64
	Weight int
}

// Each interval contains the user's chances. Removing the interval removes every
// chance of the selected user from subsequent primary and reserve selections.
func selectWeighted(users []weightedUser, n int, source io.Reader) ([]int64, error) {
	if n < 0 || n > len(users) {
		return nil, errors.New("invalid selection size")
	}
	remaining := append([]weightedUser(nil), users...)
	var total int64
	for _, user := range remaining {
		if user.Weight < 1 || user.Weight > 10 {
			return nil, errors.New("invalid chance weight")
		}
		total += int64(user.Weight)
	}
	selected := make([]int64, 0, n)
	for range n {
		r, err := rand.Int(source, big.NewInt(total))
		if err != nil {
			return nil, err
		}
		ticket := r.Int64()
		for i, user := range remaining {
			if ticket < int64(user.Weight) {
				selected = append(selected, user.ID)
				total -= int64(user.Weight)
				remaining = append(remaining[:i], remaining[i+1:]...)
				break
			}
			ticket -= int64(user.Weight)
		}
	}
	return selected, nil
}

// DrawGrand shares the event transaction with its durable command reply.
func DrawGrand(ctx context.Context, tx pgx.Tx, now time.Time, loc *time.Location, reserves *int) (DailyDrawResult, error) {
	if loc == nil {
		return DailyDrawResult{}, errors.New("festival timezone is required")
	}
	// A transaction-scoped lock serializes creation even before a grand row exists.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(71630492001)`); err != nil {
		return DailyDrawResult{}, err
	}
	var id int64
	var status string
	err := tx.QueryRow(ctx, `SELECT id,status FROM draws WHERE kind='grand'`).Scan(&id, &status)
	if err == nil {
		return DailyDrawResult{ID: id, Message: fmt.Sprintf("Розыгрыш #%d уже сохранен (%s). Повторный выбор не выполняется.", id, status)}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return DailyDrawResult{}, err
	}
	if reserves == nil {
		return DailyDrawResult{Message: "Задайте согласованное число резервов финала в GRAND_RESERVE_COUNT."}, nil
	}
	if *reserves < 0 || *reserves > 10000 {
		return DailyDrawResult{}, errors.New("invalid grand reserve count")
	}
	// Freeze the calendar and wait for in-flight chance grants before reading them.
	if _, err = tx.Exec(ctx, `LOCK TABLE partners,user_chances IN SHARE MODE`); err != nil {
		return DailyDrawResult{}, err
	}
	var ended bool
	if err = tx.QueryRow(ctx, `SELECT count(*)>0 AND coalesce(bool_and(festival_day<$1::date AND entry_closes_at<=$2),false) FROM partners`, now.In(loc).Format(time.DateOnly), now).Scan(&ended); err != nil {
		return DailyDrawResult{}, err
	}
	if !ended {
		return DailyDrawResult{Message: "Финальный розыгрыш доступен после завершения последнего фестивального дня. Проверьте импорт полного календаря акции."}, nil
	}
	rows, err := tx.Query(ctx, `SELECT c.user_id,count(*)::int,jsonb_build_object(
 'first_name',u.first_name,'last_name',u.last_name,'username',u.username,
 'chances',jsonb_agg(jsonb_build_object('chance_id',c.id,'festival_day',c.festival_day,
 'partner_id',c.partner_id,'partner_name',p.name,'granted_at',c.granted_at) ORDER BY c.festival_day,c.id))
 FROM user_chances c JOIN users u ON u.id=c.user_id JOIN partners p ON p.id=c.partner_id
 GROUP BY c.user_id,u.id ORDER BY c.user_id`)
	if err != nil {
		return DailyDrawResult{}, err
	}
	var users []weightedUser
	snapshots := map[int64][]byte{}
	for rows.Next() {
		var user weightedUser
		var snapshot []byte
		if err = rows.Scan(&user.ID, &user.Weight, &snapshot); err != nil {
			rows.Close()
			return DailyDrawResult{}, err
		}
		users = append(users, user)
		snapshots[user.ID] = snapshot
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return DailyDrawResult{}, err
	}
	if len(users) < 5+*reserves {
		return DailyDrawResult{Message: fmt.Sprintf("Недостаточно участников: %d, требуется 5 основных и %d резервных. Розыгрыш не создан; необходимо согласовать правила нехватки кандидатов.", len(users), *reserves)}, nil
	}
	selected, err := selectWeighted(users, 5+*reserves, rand.Reader)
	if err != nil {
		return DailyDrawResult{}, err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO draws(kind,status,drawn_at) VALUES('grand','pending_approval',$1) RETURNING id`, now).Scan(&id); err != nil {
		return DailyDrawResult{}, err
	}
	for _, user := range users {
		if _, err = tx.Exec(ctx, `INSERT INTO draw_participants(draw_id,user_id,weight,snapshot) VALUES($1,$2,$3,$4)`, id, user.ID, user.Weight, snapshots[user.ID]); err != nil {
			return DailyDrawResult{}, err
		}
	}
	for i, user := range selected {
		role, state := "primary", "pending_notification"
		var order, prize, quantity any
		if i < 5 {
			prize, quantity = "Билеты на Канье Уэста", 2
		} else {
			role, state, order = "reserve", "reserve", i-4
		}
		if _, err = tx.Exec(ctx, `INSERT INTO winners(draw_id,user_id,role,reserve_order,prize,prize_quantity,state) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, user, role, order, prize, quantity, state); err != nil {
			return DailyDrawResult{}, err
		}
	}
	if err = sealDraw(ctx, tx, id); err != nil {
		return DailyDrawResult{}, err
	}
	return DailyDrawResult{ID: id, Message: fmt.Sprintf("Финальный розыгрыш #%d сохранен: %d участников, 5 основных по 2 билета и %d резервных. Статус: pending_approval. Уведомления победителям ожидают утверждения.", id, len(users), *reserves)}, nil
}

// Preserve the original allocation even after reserves inherit a vacated prize.
func sealDraw(ctx context.Context, tx pgx.Tx, id int64) error {
	_, err := tx.Exec(ctx, `UPDATE draws SET selection_snapshot=(SELECT jsonb_agg(jsonb_build_object(
 'winner_id',id,'user_id',user_id,'role',role,'reserve_order',reserve_order,
 'prize',prize,'prize_quantity',prize_quantity) ORDER BY id) FROM winners WHERE draw_id=$1) WHERE id=$1`, id)
	return err
}
