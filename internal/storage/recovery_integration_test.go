//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"example.com/max-festival-bot/internal/bot"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

func restartedStore(t *testing.T, s storage.Store) storage.Store {
	t.Helper()
	pool, err := pgxpool.NewWithConfig(context.Background(), s.Pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return storage.Store{Pool: pool}
}
func drawStatus(t *testing.T, s storage.Store, id, after int64) string {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	message, err := storage.DrawStatus(ctx, tx, id, after)
	if err != nil {
		t.Fatal(err)
	}
	if utf8.RuneCountInString(message) > 4000 {
		t.Fatal("status exceeds MAX limit")
	}
	return message
}
func drawWinnerIDs(t *testing.T, s storage.Store, state string) []int64 {
	t.Helper()
	rows, err := s.Pool.Query(context.Background(), `SELECT id FROM winners WHERE state=$1 ORDER BY id`, state)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestRecoveryUnavailableAndAdminStatus(t *testing.T) {
	for _, kind := range []string{"daily", "grand"} {
		t.Run(kind, func(t *testing.T) {
			s, draw := notificationDraw(t, kind)
			ctx := context.Background()
			if done, err := s.CompleteDraw(ctx); err != nil || done {
				t.Fatal("unapproved draw completed", done, err)
			}
			approve(t, s, draw)
			winner, _ := winnerInfo(t, s)
			if err := s.NotifyWinner(ctx, winner, winnerSender(func(context.Context, int64, maxapi.Body) error { return &maxapi.APIError{Status: 403} })); err != nil {
				t.Fatal(err)
			}
			restored := restartedStore(t, s)
			message := drawStatus(t, restored, draw, 0)
			for _, want := range []string{"Недоступные получатели: 1", "unreachable", "доставка:failed", "срок ответа не начат"} {
				if !strings.Contains(message, want) {
					t.Fatal(message)
				}
			}
			if done, err := restored.ExpireWinner(ctx); err != nil || done {
				t.Fatal("undelivered notification expired", done, err)
			}
			if done, err := restored.CompleteDraw(ctx); err != nil || done {
				t.Fatal("unavailable winner silently completed", done, err)
			}
			h := bot.Handler{Config: config.Config{AdminIDs: map[int64]bool{1: true}, AdminChatID: -1}}
			for i, tc := range []struct {
				user, chat int64
				args, want string
			}{
				{2, -1, fmt.Sprint(draw), "Доступ запрещен"},
				{1, -2, fmt.Sprint(draw), "Доступ запрещен"},
				{1, -1, "", "Формат:"}, {1, -1, "0", "Формат:"}, {1, -1, "-1", "Формат:"},
				{1, -1, "abc", "Формат:"}, {1, -1, "9223372036854775808", "Формат:"},
				{1, -1, fmt.Sprintf("%d -1", draw), "Формат:"}, {1, -1, fmt.Sprintf("%d 0 extra", draw), "Формат:"},
				{1, -1, "99999999", "не найден"}, {1, -1, fmt.Sprint(draw), "Недоступные получатели: 1"},
			} {
				key := fmt.Sprintf("status-%d", i)
				if err := restored.Save(ctx, commandEvent(key, "/draw_status "+tc.args, tc.user, tc.chat)); err != nil {
					t.Fatal(err)
				}
				process(t, restored, h)
				var text string
				if err := restored.Pool.QueryRow(ctx, `SELECT body->>'text' FROM outgoing WHERE event_key=$1`, "message_created:"+key).Scan(&text); err != nil || !strings.Contains(text, tc.want) {
					t.Fatal(text, err)
				}
				if tc.want == "Доступ запрещен" && strings.Contains(text, "user:") {
					t.Fatal("winner details leaked")
				}
			}
		})
	}
}

func TestRestartRecoveryOfOutboxDeadlinesAndExhaustion(t *testing.T) {
	for _, kind := range []string{"daily", "grand"} {
		t.Run(kind, func(t *testing.T) {
			s, draw := notificationDraw(t, kind)
			approve(t, s, draw)
			ctx := context.Background()
			var original string
			if err := s.Pool.QueryRow(ctx, `SELECT selection_snapshot::text FROM draws WHERE id=$1`, draw).Scan(&original); err != nil {
				t.Fatal(err)
			}
			// Redis acknowledged a job that is subsequently lost. PG still holds the work.
			var lost int64
			if done, err := s.DispatchNotification(ctx, queueFunc(func(_ context.Context, id int64) error { lost = id; return nil })); err != nil || !done {
				t.Fatal(done, err)
			}
			if _, err := s.Pool.Exec(ctx, `UPDATE winner_notifications SET next_enqueue_at=clock_timestamp()-interval '1 second' WHERE winner_id=$1`, lost); err != nil {
				t.Fatal(err)
			}
			restored := restartedStore(t, s)
			redelivered := map[int64]bool{}
			for {
				done, err := restored.DispatchNotification(ctx, queueFunc(func(_ context.Context, id int64) error { redelivered[id] = true; return nil }))
				if err != nil {
					t.Fatal(err)
				}
				if !done {
					break
				}
			}
			if !redelivered[lost] {
				t.Fatal("Redis data loss lost durable notification")
			}
			if done, err := restored.CompleteDraw(ctx); err != nil || done {
				t.Fatal("pending notification completed", done, err)
			}
			for _, id := range drawWinnerIDs(t, restored, "pending_notification") {
				deliverWinner(t, restored, id)
				dueWinner(t, restored, id)
			}
			// A new process discovers overdue winners without the old timers or queue.
			restored = restartedStore(t, restored)
			for {
				done, err := restored.ExpireWinner(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !done {
					break
				}
			}
			want := 1
			if kind == "grand" {
				want = 2
			}
			var stable bool
			if err := restored.Pool.QueryRow(ctx, `SELECT bool_and(n.data_recipient=old.data_recipient AND n.response_seconds=old.response_seconds) FROM winners w JOIN winner_notifications n ON n.winner_id=w.id JOIN winner_notifications old ON old.winner_id=w.replaces_winner_id`).Scan(&stable); err != nil || !stable {
				t.Fatal("reserve notification lost agreed terms", err)
			}
			ids := drawWinnerIDs(t, restored, "pending_notification")
			if len(ids) != want {
				t.Fatal("wrong reserve count", ids)
			}
			for len(ids) > 0 {
				for _, id := range ids {
					deliverWinner(t, restored, id)
					dueWinner(t, restored, id)
				}
				for {
					done, err := restored.ExpireWinner(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !done {
						break
					}
				}
				ids = drawWinnerIDs(t, restored, "pending_notification")
			}
			wantPlaces := 1
			if kind == "grand" {
				wantPlaces = 5
			}
			var exhausted int
			if err := restored.Pool.QueryRow(ctx, `SELECT count(*) FROM winners WHERE reserves_exhausted_at IS NOT NULL`).Scan(&exhausted); err != nil || exhausted != wantPlaces {
				t.Fatal(exhausted, err)
			}
			restored = restartedStore(t, restored)
			if done, err := restored.CompleteDraw(ctx); err != nil || !done {
				t.Fatal("settled draw not completed", done, err)
			}
			if done, err := restored.CompleteDraw(ctx); err != nil || done {
				t.Fatal("completion replay", done, err)
			}
			message := drawStatus(t, restored, draw, 0)
			if !strings.Contains(message, "completed") || !strings.Contains(message, "РЕЗЕРВ ИСЧЕРПАН") || !strings.Contains(message, fmt.Sprintf("мест без резерва: %d", wantPlaces)) {
				t.Fatal(message)
			}
			var after string
			if err := restored.Pool.QueryRow(ctx, `SELECT selection_snapshot::text FROM draws WHERE id=$1`, draw).Scan(&after); err != nil || after != original {
				t.Fatal("selection changed after recovery", err)
			}
		})
	}
}

func TestDrawStatusPagination(t *testing.T) {
	s := testStore(t)
	h, _ := seedParticipation(t, s, 1, 0)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `INSERT INTO user_chances(user_id,festival_day,partner_id) SELECT u.id,p.festival_day,p.id FROM users u CROSS JOIN partners p`); err != nil {
		t.Fatal(err)
	}
	n := 15
	draw := runGrand(t, s, h.Now().AddDate(0, 0, 1), h.Config.Location, &n).ID
	approve(t, s, draw)
	ids := drawWinnerIDs(t, s, "reserve")
	first := drawStatus(t, s, draw, 0)
	// Five primaries and the first five reserves make page one.
	cursor := ids[4]
	if !strings.Contains(first, fmt.Sprintf("/draw_status %d %d", draw, cursor)) || strings.Count(first, "\n#") != 10 {
		t.Fatal(first)
	}
	second := drawStatus(t, s, draw, cursor)
	if strings.Count(second, "\n#") != 10 || strings.Contains(second, "Следующая страница:") {
		t.Fatal(second)
	}
	if strings.Contains(second, fmt.Sprintf("\n#%d user:", cursor)) {
		t.Fatal("pagination repeated winner")
	}
}

func TestApprovalRequiresAndSnapshotsRecipient(t *testing.T) {
	s, draw := notificationDraw(t, "grand")
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	message, err := storage.ApproveDraw(ctx, tx, draw, 42, time.Now(), 3600, "")
	if err != nil || !strings.Contains(message, "GRAND_DATA_RECIPIENT") {
		t.Fatal(message, err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if count(t, s, "winner_notifications") != 0 {
		t.Fatal("unclaimable notifications created")
	}
	approve(t, s, draw)
	id, user := winnerInfo(t, s)
	deliverWinner(t, s, id)
	tx, err = s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	c, ok, err := storage.OpenClaim(ctx, tx, id, user, "Changed operator after restart")
	if err != nil || !ok || c.Recipient != "Билетный оператор Тест" {
		t.Fatal(c, ok, err)
	}
}

func TestWinnerDeliveryRecoverableErrors(t *testing.T) {
	for _, kind := range []string{"daily", "grand"} {
		t.Run(kind, func(t *testing.T) {
			s, draw := notificationDraw(t, kind)
			approve(t, s, draw)
			id, _ := winnerInfo(t, s)
			ctx := context.Background()
			for _, failure := range []*maxapi.APIError{{Status: 401}, {Status: 408}, {Status: 429}, {Status: 503}, {Status: 400, Code: "attachment.not.ready"}} {
				err := s.NotifyWinner(ctx, id, winnerSender(func(context.Context, int64, maxapi.Body) error { return failure }))
				if err == nil {
					t.Fatal("retryable error acknowledged", failure)
				}
			}
			var pending bool
			if err := s.Pool.QueryRow(ctx, `SELECT w.state='pending_notification' AND w.deadline IS NULL AND n.status='pending' AND n.attempts=5 FROM winners w JOIN winner_notifications n ON n.winner_id=w.id WHERE w.id=$1`, id).Scan(&pending); err != nil || !pending {
				t.Fatal("recoverable error lost work", err)
			}
			deliverWinner(t, restartedStore(t, s), id)
		})
	}
}
