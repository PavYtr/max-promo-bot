//go:build integration

package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/max-festival-bot/internal/bot"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"example.com/max-festival-bot/internal/storage/db"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T) storage.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err = pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	sqlDB := stdlib.OpenDB(*cfg.ConnConfig)
	if err = goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	goose.SetLogger(goose.NopLogger())
	for _, direction := range []string{"up", "down", "up", "up"} {
		if err = goose.RunContext(ctx, direction, sqlDB, "../../migrations"); err != nil {
			t.Fatal(err)
		}
	}
	_ = sqlDB.Close()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return storage.Store{Pool: pool}
}
func startEvent(timestamp int64, name string) []byte {
	return []byte(fmt.Sprintf(`{"update_type":"bot_started","timestamp":%d,"chat_id":99,"user":{"user_id":7,"first_name":%q,"last_name":"Family","username":"handle"}}`, timestamp, name))
}
func commandEvent(id, text string, user, chat int64) []byte {
	b, _ := json.Marshal(map[string]any{"update_type": "message_created", "message": map[string]any{"sender": map[string]any{"user_id": user}, "recipient": map[string]any{"chat_id": chat}, "body": map[string]string{"mid": id, "text": text}}})
	return b
}
func callbackEvent(id, payload string) []byte {
	return []byte(fmt.Sprintf(`{"update_type":"message_callback","callback":{"callback_id":%q,"payload":%q,"user":{"user_id":7}},"message":{"recipient":{"chat_id":99}}}`, id, payload))
}
func count(t *testing.T, s storage.Store, table string) int {
	t.Helper()
	var n int
	if err := s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func process(t *testing.T, s storage.Store, h bot.Handler) {
	t.Helper()
	for {
		done, err := s.ProcessOne(context.Background(), h.Handle)
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			return
		}
	}
}
func TestCorePersistenceAndNavigation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	loc, _ := time.LoadLocation("Europe/Moscow")
	h := bot.Handler{Config: config.Config{Location: loc, BannerToken: "banner", HowTo: "Как участвовать", Rules: "Правила", AdminIDs: map[int64]bool{1: true}, AdminChatID: -1}}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Save(ctx, startEvent(1, "First")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if count(t, s, "inbox") != 1 {
		t.Fatal("duplicate deliveries persisted")
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				done, err := s.ProcessOne(ctx, h.Handle)
				if err != nil {
					t.Error(err)
					return
				}
				if !done {
					return
				}
			}
		}()
	}
	wg.Wait()
	if count(t, s, "users") != 1 || count(t, s, "outgoing") != 1 {
		t.Fatal("duplicate side effects")
	}
	before, err := db.New(s.Pool).GetUser(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !before.FirstStartedAt.Time.Equal(time.UnixMilli(1)) {
		t.Fatal("first start must use the event time, including delayed delivery")
	}
	if err = s.Save(ctx, startEvent(2, "Changed")); err != nil {
		t.Fatal(err)
	}
	process(t, s, h)
	after, err := db.New(s.Pool).GetUser(ctx, 7)
	if err != nil || before != after {
		t.Fatal("repeated start changed initial user data")
	}
	if err = s.Save(ctx, commandEvent("start", "/start", 7, 99)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"info:how_to", "info:rules", "menu", "promo:get", "draw:enter", "profile:chances"} {
		if err = s.Save(ctx, callbackEvent(p, p)); err != nil {
			t.Fatal(err)
		}
	}
	process(t, s, h)
	for _, p := range []string{"info:how_to", "info:rules"} {
		var raw []byte
		if err = s.Pool.QueryRow(ctx, `SELECT body FROM outgoing WHERE event_key=$1`, "message_callback:"+p).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), "В меню") {
			t.Fatal("information screen missing menu return")
		}
	}
	// A crash before commit rolls back user and reply; the durable inbox survives.
	if err = s.Save(ctx, commandEvent("crash", "/start", 8, 88)); err != nil {
		t.Fatal(err)
	}
	_, err = s.ProcessOne(ctx, func(ctx context.Context, tx pgx.Tx, key string, u maxapi.Update) error {
		if err := h.Handle(ctx, tx, key, u); err != nil {
			return err
		}
		return errors.New("simulated crash")
	})
	if err == nil || count(t, s, "users") != 1 {
		t.Fatal("partial transaction committed")
	}
	process(t, s, h)
	if count(t, s, "users") != 2 {
		t.Fatal("inbox did not recover")
	}
	// API outage leaves replies pending. New store instance recovers them.
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	client, _ := maxapi.New(api.URL, "fake", "", nil)
	if done, e := s.DeliverOne(ctx, client); e != nil || !done {
		t.Fatal(e)
	}
	api.Close()
	var sent int
	api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sent++; _, _ = w.Write([]byte(`{"success":true}`)) }))
	defer api.Close()
	client, _ = maxapi.New(api.URL, "fake", "", nil)
	if _, err = s.Pool.Exec(ctx, `UPDATE outgoing SET available_at=now()`); err != nil {
		t.Fatal(err)
	}
	recovered := storage.Store{Pool: s.Pool}
	for {
		done, e := recovered.DeliverOne(ctx, client)
		if e != nil {
			t.Fatal(e)
		}
		if !done {
			break
		}
	}
	if sent != count(t, s, "outgoing") {
		t.Fatal("outgoing recovery lost replies")
	}
}
func TestProtectedAtomicImport(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	loc, _ := time.LoadLocation("Europe/Moscow")
	h := bot.Handler{Config: config.Config{Location: loc, AdminIDs: map[int64]bool{1: true}, AdminChatID: -1}}
	raw, err := os.ReadFile("../../examples/partners.json")
	if err != nil {
		t.Fatal(err)
	}
	// Compact JSON fits MAX's 4000 character incoming message limit.
	var data storage.Import
	_ = json.Unmarshal(raw, &data)
	compact, _ := json.Marshal(data)
	for i, who := range []struct{ user, chat int64 }{{2, -1}, {1, -2}, {1, -1}} {
		if err = s.Save(ctx, commandEvent(fmt.Sprint(i), "/import "+string(compact), who.user, who.chat)); err != nil {
			t.Fatal(err)
		}
		process(t, s, h)
		want := 0
		if i == 2 {
			want = 1
		}
		if count(t, s, "partners") != want {
			t.Fatal("unauthorized import or valid import failed")
		}
	}
	if count(t, s, "promocodes") != 2 {
		t.Fatal("codes missing")
	}
	// A new partner followed by a duplicate rolls back the whole batch.
	next := data.Partners[0]
	next.Day = "2027-05-02"
	next.EntryClosesAt = next.EntryClosesAt.AddDate(0, 0, 1)
	data.Partners = append([]storage.Partner{next}, data.Partners...)
	raw, _ = json.Marshal(data)
	if err = s.Save(ctx, commandEvent("rollback", "/import "+string(raw), 1, -1)); err != nil {
		t.Fatal(err)
	}
	process(t, s, h)
	if count(t, s, "partners") != 1 || count(t, s, "promocodes") != 2 {
		t.Fatal("partial import committed")
	}
	// Duplicate delivery of the successful import has no extra effect.
	if err = s.Save(ctx, commandEvent("2", "/import "+string(compact), 1, -1)); err != nil {
		t.Fatal(err)
	}
	process(t, s, h)
	if count(t, s, "partners") != 1 {
		t.Fatal("duplicate import")
	}
}
func TestSchemaConstraints(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	exec := func(query string) {
		t.Helper()
		if _, err := s.Pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	reject := func(query string) {
		t.Helper()
		if _, err := s.Pool.Exec(ctx, query); err == nil {
			t.Fatalf("constraint missing: %s", query)
		}
	}
	exec(`INSERT INTO users(id,first_name) VALUES(1,'A'),(2,'B')`)
	raw, err := os.ReadFile("../../examples/partners.json")
	if err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Europe/Moscow")
	data, err := storage.ParseImport(raw, loc)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = storage.ImportPartners(ctx, tx, data); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE promocodes SET user_id=1,issued_at=now() WHERE code='DEMO-001'`)
	reject(`UPDATE promocodes SET user_id=1,issued_at=now() WHERE code='DEMO-002'`)
	exec(`INSERT INTO daily_entries(user_id,partner_id,subscription_checked_at) SELECT 1,id,now() FROM partners`)
	reject(`INSERT INTO daily_entries(user_id,partner_id,subscription_checked_at) SELECT 1,id,now() FROM partners`)
	exec(`INSERT INTO user_chances(user_id,festival_day,partner_id) SELECT 1,festival_day,id FROM partners`)
	reject(`INSERT INTO user_chances(user_id,festival_day,partner_id) SELECT 1,festival_day,id FROM partners`)
	exec(`INSERT INTO draws(kind,status,drawn_at) VALUES('grand','pending_approval',now())`)
	reject(`INSERT INTO draws(kind,status,drawn_at) VALUES('grand','pending_approval',now())`)
	exec(`INSERT INTO draw_participants(draw_id,user_id,weight,snapshot) SELECT id,1,1,'{"days":["2027-05-01"]}' FROM draws`)
	reject(`UPDATE draw_participants SET weight=2`)
	reject(`DELETE FROM draw_participants`)
	exec(`INSERT INTO winners(draw_id,user_id,role,prize,prize_quantity,state) SELECT id,1,'primary','Ticket',2,'pending_notification' FROM draws`)
	reject(`INSERT INTO winners(draw_id,user_id,role,prize,prize_quantity,state) SELECT id,1,'reserve','Ticket',2,'reserve' FROM draws`)
	reject(`UPDATE winners SET state='claimed'`)
}

func TestPollingBatchAndCursorAreAtomic(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	initial, err := s.Marker(ctx)
	if err != nil || initial != nil {
		t.Fatal("initial cursor must be NULL")
	}
	marker := int64(10)
	if err = s.SaveBatch(ctx, []json.RawMessage{startEvent(1, "First")}, &marker); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash/restart: a fresh Store resumes with the committed marker.
	recovered := storage.Store{Pool: s.Pool}
	saved, err := recovered.Marker(ctx)
	if err != nil || saved == nil || *saved != marker {
		t.Fatal("cursor not durable")
	}
	marker = 20
	err = s.SaveBatch(ctx, []json.RawMessage{startEvent(2, "Second"), json.RawMessage(`{}`)}, &marker)
	if err == nil {
		t.Fatal("malformed batch accepted")
	}
	saved, err = s.Marker(ctx)
	if err != nil || *saved != 10 || count(t, s, "inbox") != 1 {
		t.Fatal("failed batch partially committed")
	}
}
