//go:build integration

package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/max-festival-bot/internal/bot"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/jobs"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"github.com/hibiken/asynq"
)

type winnerSender func(context.Context, int64, maxapi.Body) error

func (f winnerSender) SendUser(c context.Context, id, chat int64, b maxapi.Body) error {
	return f(c, id, b)
}

type queueFunc func(context.Context, int64) error

func (f queueFunc) EnqueueWinner(c context.Context, id int64) error { return f(c, id) }

func notificationDraw(t *testing.T, kind string) (storage.Store, int64) {
	t.Helper()
	s := testStore(t)
	h, p := seedParticipation(t, s, 1, 0)
	ctx := context.Background()
	if kind == "grand" {
		if _, err := s.Pool.Exec(ctx, `INSERT INTO user_chances(user_id,festival_day,partner_id) SELECT u.id,p.festival_day,p.id FROM users u CROSS JOIN partners p WHERE u.id<=7`); err != nil {
			t.Fatal(err)
		}
		n := 2
		return s, runGrand(t, s, h.Now().AddDate(0, 0, 1), h.Config.Location, &n).ID
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE partners SET reserve_count=1`); err != nil {
		t.Fatal(err)
	}
	for user := int64(1); user <= 3; user++ {
		if err := s.Save(ctx, participantCallback(fmt.Sprint(user), "draw:verify", user)); err != nil {
			t.Fatal(err)
		}
	}
	process(t, s, h)
	for {
		done, err := s.DeliverOne(ctx, senderFunc(func(context.Context, int64, string, maxapi.Body) error { return nil }))
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			break
		}
	}
	return s, runDaily(t, s, p[0].ID, p[0].EntryClosesAt.Time, h.Config.Location).ID
}

func approve(t *testing.T, s storage.Store, id int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = storage.ApproveDraw(ctx, tx, id, 42, time.Now(), 7200); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestApprovalAtomicConcurrentAndNotifications(t *testing.T) {
	for _, kind := range []string{"daily", "grand"} {
		t.Run(kind, func(t *testing.T) {
			s, id := notificationDraw(t, kind)
			ctx := context.Background()
			want := 1
			if kind == "grand" {
				want = 5
			}
			var primary int64
			if err := s.Pool.QueryRow(ctx, `SELECT id FROM winners WHERE draw_id=$1 AND role='primary' ORDER BY id LIMIT 1`, id).Scan(&primary); err != nil {
				t.Fatal(err)
			}
			var sent atomic.Int32
			sender := winnerSender(func(_ context.Context, user int64, b maxapi.Body) error {
				sent.Add(1)
				if user <= 0 || !strings.Contains(fmt.Sprint(b.Attachments), "claim:start:") {
					t.Error("missing winner-specific callback")
				}
				if kind == "daily" && (!strings.Contains(b.Text, "Partner 0") || !strings.Contains(b.Text, "Claim")) {
					t.Error(b.Text)
				}
				if kind == "grand" && !strings.Contains(b.Text, "два билета") {
					t.Error(b.Text)
				}
				return nil
			})
			// Even a directly injected task cannot send before approval.
			if err := s.NotifyWinner(ctx, primary, sender); err != nil {
				t.Fatal(err)
			}
			if sent.Load() != 0 || count(t, s, "winner_notifications") != 0 {
				t.Fatal("premature delivery")
			}
			// A stale or injected queue row also cannot bypass the approval check.
			if _, err := s.Pool.Exec(ctx, `INSERT INTO winner_notifications(winner_id,body,response_seconds) VALUES($1,'{"text":"forged"}',3600)`, primary); err != nil {
				t.Fatal(err)
			}
			if err := s.NotifyWinner(ctx, primary, sender); err != nil {
				t.Fatal(err)
			}
			if sent.Load() != 0 {
				t.Fatal("unapproved draw notified")
			}
			if _, err := s.Pool.Exec(ctx, `DELETE FROM winner_notifications`); err != nil {
				t.Fatal(err)
			}
			// Crash after approval/outbox writes rolls back both.
			tx, err := s.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = storage.ApproveDraw(ctx, tx, id, 42, time.Now(), 7200); err != nil {
				t.Fatal(err)
			}
			if err = tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if count(t, s, "winner_notifications") != 0 {
				t.Fatal("outbox survived rollback")
			}
			var status string
			if err = s.Pool.QueryRow(ctx, `SELECT status FROM draws WHERE id=$1`, id).Scan(&status); err != nil || status != "pending_approval" {
				t.Fatal(status, err)
			}
			// Failure while writing the outbox must also roll back approval.
			if _, err = s.Pool.Exec(ctx, `ALTER TABLE winner_notifications ADD CONSTRAINT injected_failure CHECK (winner_id<0)`); err != nil {
				t.Fatal(err)
			}
			tx, err = s.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = storage.ApproveDraw(ctx, tx, id, 42, time.Now(), 7200)
			if err == nil {
				t.Fatal("injected failure did not fail")
			}
			_ = tx.Rollback(ctx)
			if _, err = s.Pool.Exec(ctx, `ALTER TABLE winner_notifications DROP CONSTRAINT injected_failure`); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() { defer wg.Done(); approve(t, s, id) }()
			}
			wg.Wait()
			if count(t, s, "winner_notifications") != want {
				t.Fatal("duplicate or missing jobs")
			}
			var approvedBy int64
			if err = s.Pool.QueryRow(ctx, `SELECT approved_by FROM draws WHERE id=$1 AND approved_at IS NOT NULL AND status='approved'`, id).Scan(&approvedBy); err != nil || approvedBy != 42 {
				t.Fatal(err)
			}
			// Reserve has no notification or timer. Pending primaries have no timer either.
			var bad int
			if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM winners WHERE deadline IS NOT NULL OR notified_at IS NOT NULL`).Scan(&bad); err != nil || bad != 0 {
				t.Fatal("early timer", err)
			}
			// An approved notification keeps its saved text and response period.
			if _, err = s.Pool.Exec(ctx, `UPDATE partners SET name='Changed',claim_instructions='Changed',response_seconds=1`); err != nil {
				t.Fatal(err)
			}
			// An unavailable Redis leaves the durable job available for the next dispatcher.
			done, err := s.DispatchNotification(ctx, queueFunc(func(context.Context, int64) error { return errors.New("redis down") }))
			if err == nil || done {
				t.Fatal("queue failure acknowledged")
			}
			if err = s.NotifyWinner(ctx, primary, winnerSender(func(context.Context, int64, maxapi.Body) error { return errors.New("ambiguous transport failure") })); err == nil {
				t.Fatal("transient failure acknowledged")
			}
			var attempts int
			if err = s.Pool.QueryRow(ctx, `SELECT attempts FROM winner_notifications WHERE winner_id=$1`, primary).Scan(&attempts); err != nil || attempts != 1 {
				t.Fatal("temporary failure was not recorded", attempts, err)
			}
			// Two processes racing on duplicate tasks send once after successful DB commit.
			before := time.Now()
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if e := s.NotifyWinner(ctx, primary, sender); e != nil {
						t.Error(e)
					}
				}()
			}
			wg.Wait()
			if sent.Load() != 1 {
				t.Fatal("duplicate send", sent.Load())
			}
			var notified, deadline time.Time
			if err = s.Pool.QueryRow(ctx, `SELECT notified_at,deadline FROM winners WHERE id=$1 AND state='notified'`, primary).Scan(&notified, &deadline); err != nil {
				t.Fatal(err)
			}
			seconds := time.Hour
			if kind == "grand" {
				seconds = 2 * time.Hour
			}
			if notified.Before(before.Add(-time.Second)) || deadline.Sub(notified) < seconds || deadline.Sub(notified) > seconds+time.Second {
				t.Fatal("bad timer", notified, deadline)
			}
			if kind == "grand" {
				var other int64
				if err = s.Pool.QueryRow(ctx, `SELECT id FROM winners WHERE role='primary' AND id<>$1 LIMIT 1`, primary).Scan(&other); err != nil {
					t.Fatal(err)
				}
				failure := winnerSender(func(context.Context, int64, maxapi.Body) error { return &maxapi.APIError{Status: 403} })
				if err = s.NotifyWinner(ctx, other, failure); err != nil {
					t.Fatal(err)
				}
				if err = s.NotifyWinner(ctx, other, sender); err != nil {
					t.Fatal(err)
				}
				if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM winners WHERE id=$1 AND state='unreachable' AND deadline IS NULL`, other).Scan(&bad); err != nil || bad != 1 {
					t.Fatal("permanent error not recorded", err)
				}
			}
		})
	}
}

func TestApprovalCommandAccessAndValidation(t *testing.T) {
	s, id := notificationDraw(t, "grand")
	ctx := context.Background()
	h := bot.Handler{Config: config.Config{AdminIDs: map[int64]bool{1: true}, AdminChatID: -1}}
	for i, tc := range []struct {
		user, chat int64
		text       string
	}{{2, -1, fmt.Sprintf("/approve_draw %d", id)}, {1, -2, fmt.Sprintf("/approve_draw %d", id)}, {1, -1, "/approve_draw"}, {1, -1, "/approve_draw 1 extra"}, {1, -1, "/approve_draw -1"}, {1, -1, "/approve_draw 999999"}, {1, -1, fmt.Sprintf("/approve_draw %d", id)}} {
		if err := s.Save(ctx, commandEvent(fmt.Sprintf("approve-%d", i), tc.text, tc.user, tc.chat)); err != nil {
			t.Fatal(err)
		}
		process(t, s, h)
		if count(t, s, "winner_notifications") != 0 {
			t.Fatal("invalid or unconfigured approval")
		}
	}
	h.Config.GrandResponseSeconds = 3600
	if err := s.Save(ctx, commandEvent("valid-approval", fmt.Sprintf("/approve_draw %d", id), 1, -1)); err != nil {
		t.Fatal(err)
	}
	process(t, s, h)
	if count(t, s, "winner_notifications") != 5 {
		t.Fatal("valid approval failed")
	}
}

func TestNotificationAsynqOutboxRecovery(t *testing.T) {
	s, id := notificationDraw(t, "grand")
	ctx := context.Background()
	approve(t, s, id)
	uri := os.Getenv("TEST_REDIS_URL")
	if uri == "" {
		t.Fatal("TEST_REDIS_URL required")
	}
	opt, err := asynq.ParseRedisURI(uri)
	if err != nil {
		t.Fatal(err)
	}
	client := asynq.NewClient(opt)
	defer client.Close()
	queue := jobs.Queue{Client: client}
	// Queue while the worker is offline, then lose the dispatch acknowledgement.
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var winner int64
	if err = tx.QueryRow(ctx, `SELECT winner_id FROM winner_notifications ORDER BY winner_id LIMIT 1 FOR UPDATE`).Scan(&winner); err != nil {
		t.Fatal(err)
	}
	if err = queue.EnqueueWinner(ctx, winner); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		done, e := s.DispatchNotification(ctx, queue)
		if e != nil {
			t.Fatal(e)
		}
		if !done {
			break
		}
	}
	var sent atomic.Int32
	worker := asynq.NewServer(opt, asynq.Config{Concurrency: 4, Queues: map[string]int{"winners": 1}, ShutdownTimeout: time.Second})
	if err = worker.Start(jobs.Handler(storage.Store{Pool: s.Pool}, winnerSender(func(context.Context, int64, maxapi.Body) error { sent.Add(1); return nil }))); err != nil {
		t.Fatal(err)
	}
	defer worker.Shutdown()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM winner_notifications WHERE status='sent'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 5 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// A task replayed after restart must not send again.
	worker.Stop()
	worker.Shutdown()
	if err = s.NotifyWinner(ctx, winner, winnerSender(func(context.Context, int64, maxapi.Body) error { sent.Add(1); return nil })); err != nil {
		t.Fatal(err)
	}
	if sent.Load() != 5 {
		t.Fatal("queue delivery lost or duplicated", sent.Load())
	}
	var n int
	if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM winner_notifications WHERE status='sent'`).Scan(&n); err != nil || n != 5 {
		t.Fatal(n, err)
	}
	// PostgreSQL excludes delivered rows from subsequent dispatch passes.
	if done, e := s.DispatchNotification(ctx, queue); e != nil || done {
		t.Fatal("sent notification redispatched", e)
	}
}
