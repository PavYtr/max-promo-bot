//go:build integration

package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/max-festival-bot/internal/bot"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"github.com/redis/go-redis/v9"
)

func claimRedis(t *testing.T) *redis.Client {
	t.Helper()
	opt, err := redis.ParseURL(os.Getenv("TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	r := redis.NewClient(opt)
	t.Cleanup(func() { r.Close() })
	return r
}
func winnerInfo(t *testing.T, s storage.Store) (int64, int64) {
	t.Helper()
	var id, user int64
	if err := s.Pool.QueryRow(context.Background(), `SELECT id,user_id FROM winners WHERE role='primary' ORDER BY id LIMIT 1`).Scan(&id, &user); err != nil {
		t.Fatal(err)
	}
	return id, user
}
func deliverWinner(t *testing.T, s storage.Store, id int64) {
	t.Helper()
	if err := s.NotifyWinner(context.Background(), id, winnerSender(func(context.Context, int64, maxapi.Body) error { return nil })); err != nil {
		t.Fatal(err)
	}
}
func dueWinner(t *testing.T, s storage.Store, id int64) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), `UPDATE winners SET notified_at=clock_timestamp()-interval '2 hours',deadline=clock_timestamp()-interval '1 hour' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}
func TestClaimDialogueAndRetry(t *testing.T) {
	for _, kind := range []string{"daily", "grand"} {
		t.Run(kind, func(t *testing.T) {
			s, draw := notificationDraw(t, kind)
			approve(t, s, draw)
			id, user := winnerInfo(t, s)
			deliverWinner(t, s, id)
			ctx := context.Background()
			r := claimRedis(t)
			h := bot.Handler{Claims: r}
			h.Config.GrandDataRecipient = "Билетный оператор Тест"
			chat := int64(10000) + id
			if _, err := s.Pool.Exec(ctx, `UPDATE users SET private_chat_id=$2 WHERE id=$1`, user, chat); err != nil {
				t.Fatal(err)
			}
			prefix := fmt.Sprintf("claim-%d-%d", time.Now().UnixNano(), id)
			send := func(suffix, text string, callback bool) {
				t.Helper()
				raw := commandEvent(prefix+suffix, text, user, chat)
				if callback {
					raw = []byte(fmt.Sprintf(`{"update_type":"message_callback","callback":{"callback_id":%q,"payload":%q,"user":{"user_id":%d}},"message":{"recipient":{"chat_id":%d}}}`, prefix+suffix, text, user, chat))
				}
				if err := s.Save(ctx, raw); err != nil {
					t.Fatal(err)
				}
				process(t, s, h)
			}
			send("start", fmt.Sprintf("claim:start:%d", id), true)
			// Crash after Redis advanced, before PG commits. Replaying must still ask for surname.
			raw := commandEvent(prefix+"first", "Анна-Мария", user, chat)
			u, key, err := maxapi.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Save(ctx, raw); err != nil {
				t.Fatal(err)
			}
			tx, err := s.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = h.Handle(ctx, tx, key, u); err != nil {
				t.Fatal(err)
			}
			_ = tx.Rollback(ctx)
			process(t, s, h)
			var body []byte
			if err = s.Pool.QueryRow(ctx, `SELECT body FROM outgoing WHERE event_key=$1`, key).Scan(&body); err != nil || !strings.Contains(string(body), "фамилию") {
				t.Fatal(string(body), err)
			}
			send("bad", "123", false)
			send("last", "О’Коннор", false)
			var d struct{ Token string }
			raw, err = r.Get(ctx, fmt.Sprintf("claim:user:%d", user)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(raw, &d); err != nil {
				t.Fatal(err)
			}
			send("stale", fmt.Sprintf("claim:consent:%d:stale", id), true)
			var state string
			if err = s.Pool.QueryRow(ctx, `SELECT state FROM winners WHERE id=$1`, id).Scan(&state); err != nil || state != "notified" {
				t.Fatal(state, err)
			}
			oldToken := d.Token
			if err = r.Del(ctx, fmt.Sprintf("claim:user:%d", user)).Err(); err != nil {
				t.Fatal(err)
			}
			send("lost-dialogue", fmt.Sprintf("claim:consent:%d:%s", id, oldToken), true)
			send("restart", fmt.Sprintf("claim:start:%d", id), true)
			send("first-again", "Анна-Мария", false)
			send("last-again", "О’Коннор", false)
			raw, err = r.Get(ctx, fmt.Sprintf("claim:user:%d", user)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(raw, &d); err != nil {
				t.Fatal(err)
			}
			if d.Token == oldToken {
				t.Fatal("restart reused consent token")
			}
			send("old-consent", fmt.Sprintf("claim:consent:%d:%s", id, oldToken), true)
			// Consent removes the active Redis draft. A DB rollback must still replay it safely.
			raw = []byte(fmt.Sprintf(`{"update_type":"message_callback","callback":{"callback_id":%q,"payload":%q,"user":{"user_id":%d}},"message":{"recipient":{"chat_id":%d}}}`, prefix+"consent", fmt.Sprintf("claim:consent:%d:%s", id, d.Token), user, chat))
			u, key, err = maxapi.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Save(ctx, raw); err != nil {
				t.Fatal(err)
			}
			tx, err = s.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = h.Handle(ctx, tx, key, u); err != nil {
				t.Fatal(err)
			}
			if err = tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if n, err := r.Exists(ctx, fmt.Sprintf("claim:user:%d", user)).Result(); err != nil || n != 0 {
				t.Fatal("finished dialogue retained", n, err)
			}
			send("consent", fmt.Sprintf("claim:consent:%d:%s", id, d.Token), true)
			send("duplicate", fmt.Sprintf("claim:consent:%d:%s", id, d.Token), true)
			var first, last, recipient string
			if err = s.Pool.QueryRow(ctx, `SELECT first_name,last_name,data_recipient FROM winners WHERE id=$1 AND state='claimed' AND consent_at IS NOT NULL`, id).Scan(&first, &last, &recipient); err != nil {
				t.Fatal(err)
			}
			if first != "Анна-Мария" || last != "О’Коннор" || recipient == "" {
				t.Fatal(first, last, recipient)
			}
			if done, err := s.ExpireWinner(ctx); err != nil || done {
				t.Fatal("claimed expired", done, err)
			}
			if done, err := s.CompleteDraw(ctx); err != nil || done != (kind == "daily") {
				t.Fatal("completion with unused reserves", done, err)
			}
		})
	}
}
func TestExpirationReserveChainAndConcurrency(t *testing.T) {
	for _, kind := range []string{"daily", "grand"} {
		t.Run(kind, func(t *testing.T) {
			s, draw := notificationDraw(t, kind)
			approve(t, s, draw)
			id, user := winnerInfo(t, s)
			deliverWinner(t, s, id)
			ctx := context.Background()
			// A foreign user, reserve and late callback cannot claim.
			check := func(w, u int64, want bool) {
				t.Helper()
				tx, err := s.Pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				_, ok, err := storage.OpenClaim(ctx, tx, w, u, "Оператор")
				if err != nil || ok != want {
					t.Fatal(ok, err)
				}
			}
			check(id, user+10000, false)
			var reserve, reserveUser int64
			if err := s.Pool.QueryRow(ctx, `SELECT id,user_id FROM winners WHERE state='reserve' ORDER BY reserve_order LIMIT 1`).Scan(&reserve, &reserveUser); err != nil {
				t.Fatal(err)
			}
			check(reserve, reserveUser, false)
			dueWinner(t, s, id)
			check(id, user, false)
			// Outbox failure must leave both the expired place and reserve untouched.
			if _, err := s.Pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE winner_notifications ADD CONSTRAINT reject_reserve CHECK (winner_id<>%d)`, reserve)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ExpireWinner(ctx); err == nil {
				t.Fatal("outbox failure was ignored")
			}
			var unchanged bool
			if err := s.Pool.QueryRow(ctx, `SELECT state='notified' FROM winners WHERE id=$1`, id).Scan(&unchanged); err != nil || !unchanged {
				t.Fatal("expiration survived rollback", err)
			}
			if _, err := s.Pool.Exec(ctx, `ALTER TABLE winner_notifications DROP CONSTRAINT reject_reserve`); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := s.ExpireWinner(ctx); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			var prize, oldPrize string
			var quantity, oldQuantity int
			var next int64
			if err := s.Pool.QueryRow(ctx, `SELECT w.id,w.prize,w.prize_quantity,o.prize,o.prize_quantity FROM winners w JOIN winners o ON o.id=w.replaces_winner_id WHERE o.id=$1 AND w.state='pending_notification' AND w.deadline IS NULL`, id).Scan(&next, &prize, &quantity, &oldPrize, &oldQuantity); err != nil {
				t.Fatal(err)
			}
			if next != reserve || prize != oldPrize || quantity != oldQuantity || (kind == "grand" && quantity != 2) {
				t.Fatal("wrong place", next, prize, quantity)
			}
			// Each reserve receives its own callback and deadline only after delivery.
			var raw []byte
			if err := s.Pool.QueryRow(ctx, `SELECT body FROM winner_notifications WHERE winner_id=$1`, next).Scan(&raw); err != nil || !strings.Contains(string(raw), fmt.Sprintf("claim:start:%d", next)) {
				t.Fatal(string(raw), err)
			}
			for {
				deliverWinner(t, s, next)
				dueWinner(t, s, next)
				if _, err := s.ExpireWinner(ctx); err != nil {
					t.Fatal(err)
				}
				var replacement int64
				err := s.Pool.QueryRow(ctx, `SELECT id FROM winners WHERE replaces_winner_id=$1`, next).Scan(&replacement)
				if errors.Is(err, pgx.ErrNoRows) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				next = replacement
			}
			if done, err := s.ExpireWinner(ctx); err != nil || done {
				t.Fatal("duplicate timer", done, err)
			}
		})
	}
}

func TestConsentAndExpirationRace(t *testing.T) {
	for _, kind := range []string{"daily", "grand"} {
		t.Run(kind, func(t *testing.T) {
			s, draw := notificationDraw(t, kind)
			approve(t, s, draw)
			id, user := winnerInfo(t, s)
			deliverWinner(t, s, id)
			ctx := context.Background()
			if _, err := s.Pool.Exec(ctx, `UPDATE winners SET notified_at=clock_timestamp()-interval '1 hour',deadline=clock_timestamp()+interval '20 milliseconds' WHERE id=$1`, id); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				tx, err := s.Pool.Begin(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				defer tx.Rollback(ctx)
				c, ok, err := storage.OpenClaim(ctx, tx, id, user, "")
				if err != nil {
					t.Error(err)
					return
				}
				if ok {
					if _, err = storage.ConfirmClaim(ctx, tx, c, "Имя", "Фамилия"); err != nil {
						t.Error(err)
						return
					}
				}
				if err = tx.Commit(ctx); err != nil {
					t.Error(err)
				}
			}()
			go func() {
				defer wg.Done()
				time.Sleep(25 * time.Millisecond)
				if _, err := s.ExpireWinner(ctx); err != nil {
					t.Error(err)
				}
			}()
			wg.Wait()
			var state string
			var replacements int
			if err := s.Pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM winners WHERE replaces_winner_id=$1) FROM winners WHERE id=$1`, id).Scan(&state, &replacements); err != nil {
				t.Fatal(err)
			}
			if (state == "claimed" && replacements != 0) || (state == "expired" && replacements != 1) || (state != "claimed" && state != "expired") {
				t.Fatal(state, replacements)
			}
		})
	}
}
