//go:build integration

package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/max-festival-bot/internal/bot"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"example.com/max-festival-bot/internal/storage/db"
	"github.com/jackc/pgx/v5"
)

type membershipFunc func(context.Context, int64, int64) (bool, error)

func (f membershipFunc) CheckMembership(ctx context.Context, chat, user int64) (bool, error) {
	return f(ctx, chat, user)
}

type senderFunc func(context.Context, int64, string, maxapi.Body) error

func (f senderFunc) Send(ctx context.Context, chat int64, cb string, body maxapi.Body) error {
	return f(ctx, chat, cb, body)
}

func seedParticipation(t *testing.T, s storage.Store, days, codes int) (bot.Handler, []db.Partner) {
	t.Helper()
	ctx := context.Background()
	loc, _ := time.LoadLocation("Europe/Moscow")
	instant := time.Date(2027, 5, 1, 12, 0, 0, 0, loc)
	h := bot.Handler{Config: config.Config{Location: loc}, Now: func() time.Time { return instant }, Membership: membershipFunc(func(context.Context, int64, int64) (bool, error) { return true, nil })}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO users(id,first_name) SELECT n,'User' FROM generate_series(1,20) n`); err != nil {
		t.Fatal(err)
	}
	var partners []storage.Partner
	for d := 0; d < days; d++ {
		date := instant.AddDate(0, 0, d)
		p := storage.Partner{Day: date.Format(time.DateOnly), Name: fmt.Sprintf("Partner %d", d), ChatID: -100 - int64(d), Link: "https://max.ru/test",
			PromoExpiresAt: date.AddDate(0, 0, 5), PromoInstructions: "На сайте https://example.com", WinnerCount: 1,
			Prizes: []storage.Prize{{Place: 1, Name: "Prize", Quantity: 1}}, ResponseSeconds: 3600,
			EntryClosesAt: time.Date(date.Year(), date.Month(), date.Day(), 20, 0, 0, 0, loc), ClaimInstructions: "Claim", DataRecipient: "Partner"}
		for n := 0; n < codes; n++ {
			p.Codes = append(p.Codes, fmt.Sprintf("CODE-%d-%d", d, n))
		}
		partners = append(partners, p)
	}
	if err = storage.ImportPartners(ctx, tx, storage.Import{Partners: partners}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var result []db.Partner
	for _, p := range partners {
		date, _ := time.Parse(time.DateOnly, p.Day)
		var partner db.Partner
		day, err := (storage.Festival{Location: loc, Now: func() time.Time { return date.Add(12 * time.Hour) }}).Current(ctx, db.New(s.Pool))
		if err != nil || day.Partner == nil {
			t.Fatalf("seed calendar: %v", err)
		}
		partner = *day.Partner
		result = append(result, partner)
	}
	return h, result
}

func participantCallback(id, action string, user int64) []byte {
	return []byte(fmt.Sprintf(`{"update_type":"message_callback","callback":{"callback_id":%q,"payload":%q,"user":{"user_id":%d}},"message":{"recipient":{"chat_id":99}}}`, id, action, user))
}
func actionReply(t *testing.T, s storage.Store, h bot.Handler, id, action string) maxapi.Body {
	t.Helper()
	if err := s.Save(context.Background(), participantCallback(id, action, 7)); err != nil {
		t.Fatal(err)
	}
	process(t, s, h)
	var raw []byte
	if err := s.Pool.QueryRow(context.Background(), `SELECT body FROM outgoing WHERE event_key=$1`, "message_callback:"+id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var body maxapi.Body
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	return body
}
func requireText(t *testing.T, body maxapi.Body, parts ...string) {
	t.Helper()
	for _, part := range parts {
		if !strings.Contains(body.Text, part) {
			t.Fatalf("missing %q in %q", part, body.Text)
		}
	}
}

func concurrentActions(t *testing.T, s storage.Store, h bot.Handler, users []int64, action string) {
	t.Helper()
	ctx := context.Background()
	for i, user := range users {
		if err := s.Save(ctx, participantCallback(fmt.Sprint(i), action, user)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for range 10 {
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
}

func TestConcurrentPromoAllocation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codes int
		users []int64
		want  int
	}{
		{"different users", 10, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 10},
		{"one user", 10, []int64{7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 1},
		{"last code", 1, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testStore(t)
			h, _ := seedParticipation(t, s, 1, tc.codes)
			concurrentActions(t, s, h, tc.users, "promo:get")
			var issued int
			if err := s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM promocodes WHERE user_id IS NOT NULL`).Scan(&issued); err != nil {
				t.Fatal(err)
			}
			if issued != tc.want || count(t, s, "user_chances") != tc.want || count(t, s, "promo_requests") != 10 {
				t.Fatalf("issued=%d chances=%d requests=%d", issued, count(t, s, "user_chances"), count(t, s, "promo_requests"))
			}
			// Redelivery is neither a new request nor a new issue.
			if err := s.Save(context.Background(), participantCallback("0", "promo:get", tc.users[0])); err != nil {
				t.Fatal(err)
			}
			process(t, s, h)
			if count(t, s, "promo_requests") != 10 {
				t.Fatal("redelivery counted")
			}
		})
	}
}

func TestPromoRollbackAndLockedStock(t *testing.T) {
	s := testStore(t)
	h, p := seedParticipation(t, s, 1, 1)
	ctx := context.Background()
	// A rejected chance must roll back the assignment, request metric and reply.
	if _, err := s.Pool.Exec(ctx, `ALTER TABLE user_chances ADD CONSTRAINT reject_chance CHECK (user_id<>7)`); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, participantCallback("rollback", "promo:get", 7)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessOne(ctx, h.Handle); err == nil {
		t.Fatal("expected chance failure")
	}
	var issued int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM promocodes WHERE user_id IS NOT NULL`).Scan(&issued); err != nil {
		t.Fatal(err)
	}
	if issued != 0 || count(t, s, "promo_requests") != 0 || count(t, s, "outgoing") != 0 {
		t.Fatal("partial promo transaction")
	}
	if _, err := s.Pool.Exec(ctx, `ALTER TABLE user_chances DROP CONSTRAINT reject_chance`); err != nil {
		t.Fatal(err)
	}
	// The only code is briefly locked by another transaction, then made available.
	lock, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err = lock.Exec(ctx, `SELECT id FROM promocodes FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { time.Sleep(30 * time.Millisecond); done <- lock.Rollback(ctx) }()
	process(t, s, h)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if count(t, s, "user_chances") != 1 {
		t.Fatal("temporarily locked code reported exhausted")
	}
	first := actionReply(t, s, h, "repeat", "promo:get")
	requireText(t, first, "CODE-0-0", "На сайте", "06.05.2027")
	// Unsupported festival dates cannot receive a chance even through the helper.
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	n, err := storage.GrantDailyChance(ctx, tx, 8, p[0].ID, h.Now().AddDate(0, 0, 1), h.Config.Location)
	if err != nil || n != 0 {
		t.Fatalf("chance outside partner day: %d %v", n, err)
	}
}

func TestSubscriptionRegistrationAndDelivery(t *testing.T) {
	s := testStore(t)
	h, p := seedParticipation(t, s, 3, 0)
	ctx := context.Background()
	verify := fmt.Sprintf("draw:verify:%d", p[0].ID)
	requireText(t, actionReply(t, s, h, "empty", "promo:get"), "уже закончились", "можешь подписаться")
	start := actionReply(t, s, h, "enter", "draw:enter")
	raw, _ := json.Marshal(start)
	if !strings.Contains(string(raw), `"url":"https://max.ru/test"`) || !strings.Contains(string(raw), verify) {
		t.Fatalf("invalid subscription buttons: %s", raw)
	}
	h.Membership = membershipFunc(func(context.Context, int64, int64) (bool, error) { return false, nil })
	for i := 0; i < 12; i++ {
		requireText(t, actionReply(t, s, h, fmt.Sprintf("absent-%d", i), verify), "Пока не вижу подписку")
	}
	h.Membership = membershipFunc(func(context.Context, int64, int64) (bool, error) { return false, errors.New("API unavailable") })
	requireText(t, actionReply(t, s, h, "api-error", verify), "технической ошибки")
	if count(t, s, "daily_entries") != 0 || count(t, s, "user_chances") != 0 {
		t.Fatal("unverified registration")
	}
	h.Membership = membershipFunc(func(_ context.Context, chat, user int64) (bool, error) {
		if chat != p[0].ChatID || user != 7 {
			t.Error("wrong membership target")
		}
		return true, nil
	})
	requireText(t, actionReply(t, s, h, "success", verify), "Ты в лайнапе", "Накоплено шансов: 1 из 10")
	var registered, checked time.Time
	var status string
	if err := s.Pool.QueryRow(ctx, `SELECT registered_at,subscription_checked_at,confirmation_status FROM daily_entries`).Scan(&registered, &checked, &status); err != nil {
		t.Fatal(err)
	}
	if !registered.Equal(h.Now()) || !checked.Equal(registered) || status != "pending" {
		t.Fatal("registration times or pending state incorrect")
	}
	// Only deliver the registration confirmation. A transient error leaves it pending.
	if _, err := s.Pool.Exec(ctx, `UPDATE outgoing SET available_at=now()+interval '1 day' WHERE daily_entry_id IS NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeliverOne(ctx, senderFunc(func(context.Context, int64, string, maxapi.Body) error { return errors.New("temporary") })); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT confirmation_status FROM daily_entries`).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("transient state: %s %v", status, err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE outgoing SET available_at=now() WHERE daily_entry_id IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeliverOne(ctx, senderFunc(func(context.Context, int64, string, maxapi.Body) error { return &maxapi.APIError{Status: 403} })); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT confirmation_status FROM daily_entries`).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("permanent state: %s %v", status, err)
	}
	h.Now = func() time.Time { return registered.Add(time.Hour) }
	h.Membership = membershipFunc(func(context.Context, int64, int64) (bool, error) {
		t.Error("existing entry must not require a new API check")
		return false, nil
	})
	requireText(t, actionReply(t, s, h, "repeat", verify), "Ты уже участвуешь", "Накоплено шансов: 1 из 10")
	// Restarted delivery recovers the new confirmation and records success.
	recovered := storage.Store{Pool: s.Pool}
	if _, err := recovered.DeliverOne(ctx, senderFunc(func(context.Context, int64, string, maxapi.Body) error { return nil })); err != nil {
		t.Fatal(err)
	}
	var after time.Time
	var confirmed *time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT registered_at,confirmation_status,confirmed_at FROM daily_entries`).Scan(&after, &status, &confirmed); err != nil {
		t.Fatal(err)
	}
	if status != "sent" || confirmed == nil || !after.Equal(registered) || count(t, s, "daily_entries") != 1 || count(t, s, "user_chances") != 1 {
		t.Fatal("repeat registration or delivery changed state incorrectly")
	}
	// Calendar gaps, not chance balance, determine the remaining days.
	if _, err := s.Pool.Exec(ctx, `UPDATE partners SET festival_day=festival_day+10 WHERE id=$1`, p[2].ID); err != nil {
		t.Fatal(err)
	}
	requireText(t, actionReply(t, s, h, "profile", "profile:chances"), "1 из 10", "2027-05-01 — Partner 0", "Осталось фестивальных дней: 2", "пять победителей", "по два билета")
}

func TestRegistrationBoundariesAndCombinedChance(t *testing.T) {
	for _, boundary := range []string{"closing", "midnight"} {
		t.Run(boundary, func(t *testing.T) {
			s := testStore(t)
			h, p := seedParticipation(t, s, 2, 1)
			instant := h.Now()
			h.Now = func() time.Time { return instant }
			h.Membership = membershipFunc(func(context.Context, int64, int64) (bool, error) {
				instant = p[0].EntryClosesAt.Time
				if boundary == "midnight" {
					instant = time.Date(2027, 5, 2, 0, 0, 0, 0, h.Config.Location)
				}
				return true, nil
			})
			requireText(t, actionReply(t, s, h, "crossing", fmt.Sprintf("draw:verify:%d", p[0].ID)), "закрыт или фестивальный день сменился")
			if count(t, s, "daily_entries") != 0 || count(t, s, "user_chances") != 0 {
				t.Fatal("API crossed deadline but entry was created")
			}
			if boundary == "midnight" {
				requireText(t, actionReply(t, s, h, "stale", fmt.Sprintf("draw:verify:%d", p[0].ID)), "кнопка устарела")
				return
			}
			// Closing affects entries only: the day's code still earns a chance.
			requireText(t, actionReply(t, s, h, "closed-promo", "promo:get"), "CODE-0-0")
			requireText(t, actionReply(t, s, h, "closed-entry", "draw:enter"), "Прием заявок")
			if count(t, s, "user_chances") != 1 {
				t.Fatal("closed registration blocked promo chance")
			}
		})
	}
	s := testStore(t)
	h, _ := seedParticipation(t, s, 1, 10)
	// Concurrent promo and membership actions for one user share the daily chance.
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		action := "promo:get"
		if i%2 == 0 {
			action = "draw:verify"
		}
		if err := s.Save(ctx, participantCallback(fmt.Sprint(i), action, 7)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); process(t, s, h) }()
	}
	wg.Wait()
	if count(t, s, "daily_entries") != 1 || count(t, s, "user_chances") != 1 {
		t.Fatal("combined actions duplicated chance")
	}
}

func TestChanceCapAcrossConcurrentDays(t *testing.T) {
	s := testStore(t)
	h, partners := seedParticipation(t, s, 12, 1)
	ctx := context.Background()
	var wg sync.WaitGroup
	for _, p := range partners {
		wg.Add(1)
		go func(p db.Partner) {
			defer wg.Done()
			tx, err := s.Pool.Begin(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			defer tx.Rollback(ctx)
			instant := p.FestivalDay.Time.Add(12 * time.Hour)
			if _, err = storage.GrantDailyChance(ctx, tx, 7, p.ID, instant, h.Config.Location); err != nil {
				t.Error(err)
				return
			}
			if err = tx.Commit(ctx); err != nil {
				t.Error(err)
			}
		}(p)
	}
	wg.Wait()
	if count(t, s, "user_chances") != 10 {
		t.Fatal("concurrent total cap not enforced")
	}
	// Every extra festival day may still issue a code, but cannot exceed the cap.
	for i, p := range partners {
		h.Now = func() time.Time { return p.FestivalDay.Time.Add(12 * time.Hour) }
		requireText(t, actionReply(t, s, h, fmt.Sprintf("day-%d", i), "promo:get"), fmt.Sprintf("CODE-%d-0", i))
	}
	if count(t, s, "user_chances") != 10 {
		t.Fatal("extra days exceeded cap")
	}
	requireText(t, actionReply(t, s, h, "all-days", "profile:chances"), "10 из 10", "2027-05-12 — Partner 11", "Осталось фестивальных дней: 0")
}

func TestRegistrationRollback(t *testing.T) {
	s := testStore(t)
	h, _ := seedParticipation(t, s, 1, 0)
	ctx := context.Background()
	if err := s.Save(ctx, participantCallback("crash", "draw:verify", 7)); err != nil {
		t.Fatal(err)
	}
	_, err := s.ProcessOne(ctx, func(ctx context.Context, tx pgx.Tx, key string, u maxapi.Update) error {
		if err := h.Handle(ctx, tx, key, u); err != nil {
			return err
		}
		return errors.New("crash before commit")
	})
	if err == nil || count(t, s, "daily_entries") != 0 || count(t, s, "user_chances") != 0 || count(t, s, "outgoing") != 0 {
		t.Fatal("registration was partially committed")
	}
	process(t, s, h)
	if count(t, s, "daily_entries") != 1 || count(t, s, "outgoing") != 1 {
		t.Fatal("registration did not recover")
	}
}
