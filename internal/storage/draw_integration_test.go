//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
)

func runDaily(t *testing.T, s storage.Store, partner int64, now time.Time, loc *time.Location) storage.DailyDrawResult {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	r, err := storage.DrawDaily(ctx, tx, partner, now, loc)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDailyDrawRecoverySnapshotAndConcurrency(t *testing.T) {
	s := testStore(t)
	h, partners := seedParticipation(t, s, 1, 0)
	p := partners[0]
	ctx := context.Background()
	// Two primary places with distinct allocations and two reserves.
	if _, err := s.Pool.Exec(ctx, `UPDATE partners SET winner_count=2,reserve_count=2,
 prizes='[{"place":2,"name":"Second","quantity":3},{"place":1,"name":"First","quantity":2}]'`); err != nil {
		t.Fatal(err)
	}
	for user := int64(1); user <= 5; user++ {
		if err := s.Save(ctx, participantCallback(fmt.Sprint(user), "draw:verify", user)); err != nil {
			t.Fatal(err)
		}
	}
	process(t, s, h)
	open := h.Now()
	if r := runDaily(t, s, p.ID, open, h.Config.Location); r.ID != 0 || !strings.Contains(r.Message, "открыт") {
		t.Fatal(r)
	}
	closed := p.EntryClosesAt.Time
	// A permanently failed confirmation is requeued, and no participant is silently dropped.
	if _, err := s.DeliverOne(ctx, senderFunc(func(context.Context, int64, string, maxapi.Body) error { return &maxapi.APIError{Status: 403} })); err != nil {
		t.Fatal(err)
	}
	if r := runDaily(t, s, p.ID, closed, h.Config.Location); r.ID != 0 || !strings.Contains(r.Message, "не доставлены") {
		t.Fatal(r)
	}
	var failed int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM outgoing WHERE status='failed'`).Scan(&failed); err != nil || failed != 0 {
		t.Fatalf("recovery: %d %v", failed, err)
	}
	for {
		done, err := s.DeliverOne(ctx, senderFunc(func(context.Context, int64, string, maxapi.Body) error { return nil }))
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			break
		}
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE partners SET reserve_count=4`); err != nil {
		t.Fatal(err)
	}
	if r := runDaily(t, s, p.ID, closed, h.Config.Location); r.ID != 0 || !strings.Contains(r.Message, "Недостаточно участников") {
		t.Fatal(r)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE partners SET reserve_count=2`); err != nil {
		t.Fatal(err)
	}
	// Command transactions serialize on the same partner and return the same saved draw.
	var wg sync.WaitGroup
	ids := make(chan int64, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := s.Pool.Begin(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			defer tx.Rollback(ctx)
			r, err := storage.DrawDaily(ctx, tx, p.ID, closed, h.Config.Location)
			if err != nil {
				t.Error(err)
				return
			}
			if err = tx.Commit(ctx); err != nil {
				t.Error(err)
				return
			}
			ids <- r.ID
		}()
	}
	wg.Wait()
	close(ids)
	var id int64
	for n := range ids {
		if n == 0 || (id != 0 && id != n) {
			t.Fatalf("different draws %d %d", id, n)
		}
		id = n
	}
	if count(t, s, "draws") != 1 || count(t, s, "draw_participants") != 5 || count(t, s, "winners") != 4 || count(t, s, "outgoing") != 5 {
		t.Fatal("wrong draw size or premature notifications")
	}
	var good bool
	if err := s.Pool.QueryRow(ctx, `SELECT status='pending_approval' AND drawn_at=$1 AND approved_at IS NULL AND jsonb_array_length(selection_snapshot)=4 FROM draws`, closed).Scan(&good); err != nil || !good {
		t.Fatal("draw metadata", err)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT count(*)=2 FROM winners WHERE role='reserve' AND state='reserve' AND deadline IS NULL AND notified_at IS NULL AND prize IS NULL AND prize_quantity IS NULL`).Scan(&good); err != nil || !good {
		t.Fatal("reserve activated early", err)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT count(*)=2 FROM winners WHERE role='primary' AND ((prize='First' AND prize_quantity=2) OR (prize='Second' AND prize_quantity=3))`).Scan(&good); err != nil || !good {
		t.Fatal("prize allocation", err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE users SET username='changed'`); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT bool_and(snapshot->>'username'<>'changed' AND snapshot ? 'registered_at' AND snapshot ? 'subscription_checked_at' AND snapshot ? 'confirmed_at') FROM draw_participants`).Scan(&good); err != nil || !good {
		t.Fatal("snapshot changed", err)
	}
	for _, query := range []string{`UPDATE draw_participants SET weight=2`, `DELETE FROM draw_participants`, `UPDATE draws SET status='completed'`, `UPDATE winners SET state='notified' WHERE role='primary'`,
		`UPDATE winners SET state='pending_notification',prize='First',prize_quantity=2 WHERE role='reserve'`, `UPDATE winners SET deadline=now() WHERE role='reserve'`} {
		if _, err := s.Pool.Exec(ctx, query); err == nil {
			t.Fatalf("invalid change accepted: %s", query)
		}
	}
	// Allowed transitions, then a terminal winner cannot expire.
	for _, query := range []string{`UPDATE draws SET status='approved',approved_at=now(),approved_by=1`,
		`UPDATE winners SET state='notified',notified_at=now(),deadline=now()+interval '1 hour' WHERE role='primary'`,
		`UPDATE winners SET state='claimed',first_name='A',last_name='B',consent_at=now(),data_recipient='Partner' WHERE role='primary'`} {
		if _, err := s.Pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE winners SET state='expired' WHERE state='claimed'`); err == nil {
		t.Fatal("claimed expired")
	}
}

func TestDailyDrawShortageAndAdminAccess(t *testing.T) {
	s := testStore(t)
	h, partners := seedParticipation(t, s, 1, 0)
	ctx := context.Background()
	p := partners[0]
	h.Config.AdminIDs = map[int64]bool{1: true}
	h.Config.AdminChatID = -1
	h.Now = func() time.Time { return p.EntryClosesAt.Time }
	for i, who := range []struct{ user, chat int64 }{{2, -1}, {1, -2}, {1, -1}} {
		if err := s.Save(ctx, commandEvent(fmt.Sprint(i), fmt.Sprintf("/draw_daily %d", p.ID), who.user, who.chat)); err != nil {
			t.Fatal(err)
		}
		process(t, s, h)
		var text string
		if err := s.Pool.QueryRow(ctx, `SELECT body->>'text' FROM outgoing WHERE event_key=$1`, "message_created:"+fmt.Sprint(i)).Scan(&text); err != nil {
			t.Fatal(err)
		}
		want := "Доступ запрещен"
		if i == 2 {
			want = "Недостаточно участников"
		}
		if !strings.Contains(text, want) {
			t.Fatal(text)
		}
	}
	if count(t, s, "draws") != 0 {
		t.Fatal("shortage created draw")
	}
	// A rollback after selection leaves no partial result.
	if _, err := s.Pool.Exec(ctx, `INSERT INTO daily_entries(user_id,partner_id,registered_at,subscription_checked_at,confirmation_status,confirmed_at)
 VALUES(1,$1,$2,$2,'sent',$2)`, p.ID, p.EntryClosesAt.Time.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = storage.DrawDaily(ctx, tx, p.ID, h.Now(), h.Config.Location); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if count(t, s, "draws") != 0 || count(t, s, "winners") != 0 || count(t, s, "draw_participants") != 0 {
		t.Fatal("partial draw survived rollback")
	}
	// No default partner outside the calendar.
	var r storage.DailyDrawResult
	tx, err = s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	r, err = storage.DrawDaily(ctx, tx, 0, h.Now().AddDate(0, 0, 1), h.Config.Location)
	if err != nil || r.ID != 0 {
		t.Fatal(r, err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Validate command arguments, then create through the actual event handler.
	for i, args := range []string{"-1", "0", "abc", "9223372036854775808", "1 2"} {
		key := fmt.Sprintf("invalid-%d", i)
		if err = s.Save(ctx, commandEvent(key, "/draw_daily "+args, 1, -1)); err != nil {
			t.Fatal(err)
		}
		process(t, s, h)
		var message string
		if err = s.Pool.QueryRow(ctx, `SELECT body->>'text' FROM outgoing WHERE event_key=$1`, "message_created:"+key).Scan(&message); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(message, "Формат:") {
			t.Fatal(message)
		}
	}
	for i, command := range []string{"/draw_daily", fmt.Sprintf("/draw_daily %d", p.ID)} {
		if err = s.Save(ctx, commandEvent(fmt.Sprintf("valid-%d", i), command, 1, -1)); err != nil {
			t.Fatal(err)
		}
		process(t, s, h)
	}
	if count(t, s, "draws") != 1 || count(t, s, "winners") != 1 {
		t.Fatal("handler did not create one draw")
	}
}
