//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/max-festival-bot/internal/storage"
)

func runGrand(t *testing.T, s storage.Store, now time.Time, loc *time.Location, reserves *int) storage.DailyDrawResult {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	r, err := storage.DrawGrand(ctx, tx, now, loc, reserves)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestGrandSnapshotConcurrencyAndRollback(t *testing.T) {
	s := testStore(t)
	h, _ := seedParticipation(t, s, 3, 0)
	ctx := context.Background()
	// Users 1..7 have weights 1,2,3,3,3,3,3. Other bot users have no chances.
	if _, err := s.Pool.Exec(ctx, `INSERT INTO user_chances(user_id,festival_day,partner_id,granted_at)
 SELECT u.id,p.festival_day,p.id,p.festival_day::timestamp AT TIME ZONE 'Europe/Moscow'
 FROM users u CROSS JOIN partners p WHERE u.id<=7 AND p.festival_day<'2027-05-01'::date+u.id::int`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2027, 5, 4, 0, 0, 0, 0, h.Config.Location)
	reserves := 2
	// The caller's rollback discards the complete selection and all snapshots.
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := storage.DrawGrand(ctx, tx, now, h.Config.Location, &reserves)
	if err != nil || r.ID == 0 {
		t.Fatal(r, err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if count(t, s, "draws") != 0 || count(t, s, "draw_participants") != 0 || count(t, s, "winners") != 0 {
		t.Fatal("partial result survived rollback")
	}
	var wg sync.WaitGroup
	ids := make(chan int64, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := s.Pool.Begin(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			defer tx.Rollback(ctx)
			r, err := storage.DrawGrand(ctx, tx, now, h.Config.Location, &reserves)
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
			t.Fatal("different draws", id, n)
		}
		id = n
	}
	if count(t, s, "draws") != 1 || count(t, s, "draw_participants") != 7 || count(t, s, "winners") != 7 || count(t, s, "outgoing") != 0 {
		t.Fatal("wrong result size or premature notifications")
	}
	var good bool
	for _, q := range []string{
		`SELECT status='pending_approval' AND drawn_at=$1 AND approved_at IS NULL AND jsonb_array_length(selection_snapshot)=7 FROM draws`,
		`SELECT count(*)=5 AND sum(prize_quantity)=10 AND bool_and(prize_quantity=2 AND state='pending_notification' AND deadline IS NULL AND notified_at IS NULL) FROM winners WHERE role='primary' AND $1::timestamptz IS NOT NULL`,
		`SELECT count(*)=2 AND min(reserve_order)=1 AND max(reserve_order)=2 AND bool_and(state='reserve' AND prize IS NULL AND prize_quantity IS NULL AND deadline IS NULL AND notified_at IS NULL) FROM winners WHERE role='reserve' AND $1::timestamptz IS NOT NULL`,
		`SELECT sum(weight)=18 AND bool_and(weight=jsonb_array_length(snapshot->'chances') AND weight=least(user_id,3) AND snapshot ? 'username') FROM draw_participants WHERE $1::timestamptz IS NOT NULL`,
	} {
		if err = s.Pool.QueryRow(ctx, q, now).Scan(&good); err != nil || !good {
			t.Fatal(q, good, err)
		}
	}
	var before string
	if err = s.Pool.QueryRow(ctx, `SELECT selection_snapshot::text FROM draws`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE users SET username='changed'; UPDATE partners SET name='changed'; DELETE FROM user_chances`); err != nil {
		t.Fatal(err)
	}
	// Repeat ignores changed settings, time and source data and returns the saved draw.
	if r = runGrand(t, s, now.Add(-24*time.Hour), h.Config.Location, nil); r.ID != id {
		t.Fatal(r)
	}
	var after string
	if err = s.Pool.QueryRow(ctx, `SELECT selection_snapshot::text FROM draws`).Scan(&after); err != nil || before != after {
		t.Fatal("selection changed", err)
	}
	if err = s.Pool.QueryRow(ctx, `SELECT bool_and(snapshot->>'username'<>'changed' AND snapshot->'chances'->0->>'partner_name'<>'changed') FROM draw_participants`).Scan(&good); err != nil || !good {
		t.Fatal("snapshot changed", err)
	}
	for _, q := range []string{`UPDATE draw_participants SET weight=1`, `DELETE FROM draw_participants`, `UPDATE draws SET selection_snapshot='[]'`, `UPDATE draws SET drawn_at=now()`, `UPDATE winners SET user_id=20`, `INSERT INTO draw_participants(draw_id,user_id,weight,snapshot) SELECT id,20,1,'{}' FROM draws`} {
		if _, err = s.Pool.Exec(ctx, q); err == nil {
			t.Fatal("snapshot mutation accepted", q)
		}
	}
}

func TestGrandCalendarShortageAndAdmin(t *testing.T) {
	s := testStore(t)
	h, _ := seedParticipation(t, s, 3, 0)
	ctx := context.Background()
	reserves := 2
	loc := h.Config.Location
	end := time.Date(2027, 5, 4, 0, 0, 0, 0, loc)
	for _, now := range []time.Time{end.AddDate(0, 0, -4), end.AddDate(0, 0, -2), end.Add(-time.Nanosecond)} {
		if r := runGrand(t, s, now, loc, &reserves); r.ID != 0 || !strings.Contains(r.Message, "после завершения") {
			t.Fatal(r)
		}
	}
	if r := runGrand(t, s, end, loc, nil); r.ID != 0 || !strings.Contains(r.Message, "GRAND_RESERVE_COUNT") {
		t.Fatal(r)
	}
	if r := runGrand(t, s, end, loc, &reserves); r.ID != 0 || !strings.Contains(r.Message, "Недостаточно") {
		t.Fatal(r)
	}
	if _, err := s.Pool.Exec(ctx, `INSERT INTO user_chances(user_id,festival_day,partner_id) SELECT u.id,p.festival_day,p.id FROM users u CROSS JOIN partners p WHERE u.id<=7`); err != nil {
		t.Fatal(err)
	}
	h.Config.GrandReserveCount = &reserves
	h.Config.AdminIDs = map[int64]bool{1: true}
	h.Config.AdminChatID = -1
	h.Now = func() time.Time { return end }
	for i, tc := range []struct {
		user, chat    int64
		command, want string
	}{
		{2, -1, "/draw_grand", "Доступ запрещен"},
		{1, -2, "/draw_grand", "Доступ запрещен"},
		{1, -1, "/draw_grand 2", "Формат:"},
		{1, -1, "/draw_grand", "5 основных по 2 билета"},
		{1, -1, "/draw_grand", "уже сохранен"},
	} {
		key := fmt.Sprintf("grand-%d", i)
		if err := s.Save(ctx, commandEvent(key, tc.command, tc.user, tc.chat)); err != nil {
			t.Fatal(err)
		}
		process(t, s, h)
		var message string
		if err := s.Pool.QueryRow(ctx, `SELECT body->>'text' FROM outgoing WHERE event_key=$1`, "message_created:"+key).Scan(&message); err != nil || !strings.Contains(message, tc.want) {
			t.Fatal(message, err)
		}
	}
	if count(t, s, "draws") != 1 {
		t.Fatal("duplicate draw")
	}
}

func TestGrandWithoutCalendar(t *testing.T) {
	s := testStore(t)
	n := 0
	if r := runGrand(t, s, time.Now(), time.UTC, &n); r.ID != 0 || !strings.Contains(r.Message, "после завершения") {
		t.Fatal(r)
	}
}

func TestGrandWriteFailureAndZeroReserves(t *testing.T) {
	s := testStore(t)
	h, _ := seedParticipation(t, s, 1, 0)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `INSERT INTO user_chances(user_id,festival_day,partner_id)
 SELECT u.id,p.festival_day,p.id FROM users u CROSS JOIN partners p WHERE u.id<=6`); err != nil {
		t.Fatal(err)
	}
	// Fail after the draw, all participants and five primaries have been written.
	if _, err := s.Pool.Exec(ctx, `CREATE FUNCTION fail_reserve() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.role='reserve' THEN RAISE EXCEPTION 'simulated write failure'; END IF; RETURN NEW; END; $$;
 CREATE TRIGGER fail_reserve BEFORE INSERT ON winners FOR EACH ROW EXECUTE FUNCTION fail_reserve()`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2027, 5, 2, 0, 0, 0, 0, h.Config.Location)
	n := 1
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, drawErr := storage.DrawGrand(ctx, tx, now, h.Config.Location, &n)
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if drawErr == nil || !strings.Contains(drawErr.Error(), "simulated write failure") {
		t.Fatal(drawErr)
	}
	for _, table := range []string{"draws", "draw_participants", "winners"} {
		if count(t, s, table) != 0 {
			t.Fatal("partial result survived write failure", table)
		}
	}
	n = 0
	if r := runGrand(t, s, now, h.Config.Location, &n); r.ID == 0 {
		t.Fatal(r)
	}
	if count(t, s, "draw_participants") != 6 || count(t, s, "winners") != 5 {
		t.Fatal("zero reserves mishandled")
	}
}
