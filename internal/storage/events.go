package storage

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/max-festival-bot/internal/maxapi"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ Pool *pgxpool.Pool }

func saveEvent(ctx context.Context, tx pgx.Tx, raw []byte) error {
	_, key, err := maxapi.Decode(raw)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO inbox(event_key,payload) VALUES($1,$2) ON CONFLICT DO NOTHING`, key, raw)
	return err
}
func (s Store) Save(ctx context.Context, raw []byte) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = saveEvent(ctx, tx, raw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s Store) SaveBatch(ctx context.Context, updates []json.RawMessage, marker *int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, raw := range updates {
		if err = saveEvent(ctx, tx, raw); err != nil {
			return err
		}
	}
	if marker != nil {
		if _, err = tx.Exec(ctx, `UPDATE poll_cursor SET marker=$1 WHERE singleton`, *marker); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s Store) Marker(ctx context.Context) (*int64, error) {
	var n *int64
	err := s.Pool.QueryRow(ctx, `SELECT marker FROM poll_cursor WHERE singleton`).Scan(&n)
	return n, err
}

type EventHandler func(context.Context, pgx.Tx, string, maxapi.Update) error

func (s Store) ProcessOne(ctx context.Context, handle EventHandler) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var key string
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT event_key,payload FROM inbox WHERE processed_at IS NULL ORDER BY received_at,event_key FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&key, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	u, _, err := maxapi.Decode(raw)
	if err != nil {
		return false, err
	}
	if err = handle(ctx, tx, key, u); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE inbox SET processed_at=now() WHERE event_key=$1`, key); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
func Reply(ctx context.Context, tx pgx.Tx, key string, chat int64, callback string, body maxapi.Body) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outgoing(event_key,chat_id,callback_id,body) VALUES($1,$2,NULLIF($3,''),$4) ON CONFLICT DO NOTHING`, key, chat, callback, raw)
	return err
}

type Sender interface {
	Send(context.Context, int64, string, maxapi.Body) error
}

func (s Store) DeliverOne(ctx context.Context, sender Sender) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id, chat int64
	var callback *string
	var raw []byte
	var attempts int
	err = tx.QueryRow(ctx, `SELECT id,chat_id,callback_id,body,attempts FROM outgoing WHERE status='pending' AND available_at<=now() ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id, &chat, &callback, &raw, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var body maxapi.Body
	if err = json.Unmarshal(raw, &body); err != nil {
		return false, err
	}
	cb := ""
	if callback != nil {
		cb = *callback
	}
	// Bound the lock duration; cancellation rolls back and leaves the job pending.
	err = sender.Send(ctx, chat, cb, body)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE outgoing SET status='sent',sent_at=now(),attempts=attempts+1 WHERE id=$1`, id)
	} else {
		var api *maxapi.APIError
		permanent := errors.As(err, &api) && api.Status >= 400 && api.Status < 500 && api.Status != 429 && api.Code != "attachment.not.ready"
		status := "pending"
		if permanent {
			status = "failed"
		}
		delay := min(300, 1<<min(attempts, 8))
		_, err = tx.Exec(ctx, `UPDATE outgoing SET status=$2,attempts=attempts+1,available_at=now()+$3*interval '1 second' WHERE id=$1`, id, status, delay)
	}
	if err != nil {
		return false, err
	}
	// A registration confirmation and its delivery state commit together.
	if _, err = tx.Exec(ctx, `UPDATE daily_entries SET
	 confirmation_status=outgoing.status, confirmed_at=outgoing.sent_at
	 FROM outgoing WHERE outgoing.id=$1 AND daily_entries.id=outgoing.daily_entry_id
	 AND outgoing.status IN ('sent','failed') AND daily_entries.confirmation_status<>'sent'`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
