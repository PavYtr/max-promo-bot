package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"example.com/max-festival-bot/internal/maxapi"
	"github.com/jackc/pgx/v5"
)

type Claim struct {
	ID        int64
	Recipient string
	Deadline  time.Time
}

// The row lock is shared with expiration; time is checked after acquiring it.
func OpenClaim(ctx context.Context, tx pgx.Tx, id, user int64, grandRecipient string) (Claim, bool, error) {
	c := Claim{ID: id}
	var state, status string
	var owner int64
	var deadline *time.Time
	err := tx.QueryRow(ctx, `SELECT w.user_id,w.state,d.status,w.deadline,COALESCE(n.data_recipient,p.data_recipient,$2) FROM winners w JOIN draws d ON d.id=w.draw_id LEFT JOIN partners p ON p.id=d.partner_id LEFT JOIN winner_notifications n ON n.winner_id=w.id WHERE w.id=$1 FOR UPDATE OF w`, id, grandRecipient).Scan(&owner, &state, &status, &deadline, &c.Recipient)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	// Pending winners have no deadline.
	if err != nil {
		return c, false, err
	}
	if deadline == nil {
		return c, false, nil
	}
	c.Deadline = *deadline
	var timely bool
	if err = tx.QueryRow(ctx, `SELECT $1::timestamptz > clock_timestamp()`, c.Deadline).Scan(&timely); err != nil {
		return c, false, err
	}
	return c, owner == user && state == "notified" && status == "approved" && timely && c.Recipient != "", nil
}

func ConfirmClaim(ctx context.Context, tx pgx.Tx, c Claim, first, last string) (bool, error) {
	tag, err := tx.Exec(ctx, `UPDATE winners SET state='claimed',first_name=$2,last_name=$3,data_recipient=$4,consent_at=clock_timestamp() WHERE id=$1 AND state='notified' AND deadline>clock_timestamp()`, c.ID, first, last, c.Recipient)
	return tag.RowsAffected() == 1, err
}

// Expiration and the next reserve's durable notification commit together.
func (s Store) ExpireWinner(ctx context.Context) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id, draw int64
	err = tx.QueryRow(ctx, `SELECT id,draw_id FROM winners WHERE state='notified' AND deadline<=clock_timestamp() ORDER BY deadline,id LIMIT 1`).Scan(&id, &draw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `SELECT id FROM draws WHERE id=$1 FOR UPDATE`, draw); err != nil {
		return false, err
	}
	var state, prize string
	var quantity int
	var due bool
	if err = tx.QueryRow(ctx, `SELECT state,prize,prize_quantity,deadline<=clock_timestamp() FROM winners WHERE id=$1 FOR UPDATE`, id).Scan(&state, &prize, &quantity, &due); err != nil {
		return false, err
	}
	if state != "notified" || !due {
		return true, tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `UPDATE winners SET state='expired' WHERE id=$1`, id); err != nil {
		return false, err
	}
	var reserve int64
	err = tx.QueryRow(ctx, `SELECT id FROM winners WHERE draw_id=$1 AND state='reserve' ORDER BY reserve_order LIMIT 1 FOR UPDATE`, draw).Scan(&reserve)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err = tx.Exec(ctx, `UPDATE winners SET reserves_exhausted_at=clock_timestamp() WHERE id=$1`, id); err != nil {
			return false, err
		}
		return true, tx.Commit(ctx)
	}
	if err != nil {
		return false, err
	}
	var raw []byte
	var seconds int
	var recipient *string
	if err = tx.QueryRow(ctx, `SELECT body,response_seconds,data_recipient FROM winner_notifications WHERE winner_id=$1`, id).Scan(&raw, &seconds, &recipient); err != nil {
		return false, err
	}
	var body maxapi.Body
	if err = json.Unmarshal(raw, &body); err != nil {
		return false, err
	}
	body.Attachments = []maxapi.Attachment{maxapi.Keyboard(maxapi.Callback("Получить приз", fmt.Sprintf("claim:start:%d", reserve)))}
	raw, err = json.Marshal(body)
	if err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE winners SET state='pending_notification',prize=$2,prize_quantity=$3,replaces_winner_id=$4 WHERE id=$1`, reserve, prize, quantity, id); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO winner_notifications(winner_id,body,response_seconds,data_recipient) VALUES($1,$2,$3,$4)`, reserve, raw, seconds, recipient); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
