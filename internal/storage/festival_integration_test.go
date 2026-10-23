//go:build integration

package storage_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"example.com/max-festival-bot/internal/bot"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"example.com/max-festival-bot/internal/storage/db"
)

func TestFestivalCalendarAndNavigation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		t.Fatal(err)
	}
	// The database timezone deliberately differs from the festival timezone.
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL TIME ZONE 'America/Los_Angeles'`); err != nil {
		t.Fatal(err)
	}
	p := storage.Partner{Day: "2027-05-01", Name: "Partner", ChatID: -100, Link: "https://example.com",
		PromoExpiresAt: time.Date(2027, 5, 5, 0, 0, 0, 0, loc), PromoInstructions: "Use code",
		WinnerCount: 1, Prizes: []storage.Prize{{Place: 1, Name: "Prize", Quantity: 1}},
		ResponseSeconds: 3600, EntryClosesAt: time.Date(2027, 5, 1, 20, 0, 0, 0, loc),
		ClaimInstructions: "Claim", DataRecipient: "Partner"}
	if err = storage.ImportPartners(ctx, tx, storage.Import{Partners: []storage.Partner{p}}); err != nil {
		t.Fatal(err)
	}
	instant := time.Date(2027, 4, 30, 21, 0, 0, 0, time.UTC)
	f := storage.Festival{Location: loc, Now: func() time.Time { return instant }}
	day, err := f.Current(ctx, db.New(tx))
	if err != nil || day.Partner == nil || day.Partner.Name != p.Name || !day.RegistrationOpen || day.Day != p.Day {
		t.Fatalf("wrong festival date in a different database timezone: %+v, %v", day, err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Save(ctx, startEvent(1, "First")); err != nil {
		t.Fatal(err)
	}
	process(t, s, bot.Handler{Config: config.Config{Location: loc}})
	for i, tc := range []struct {
		instant, action, text string
	}{
		{"2027-04-30T20:59:59Z", "promo:get", "Сегодня нет активного партнера"},
		{"2027-04-30T21:00:00Z", "draw:enter", "Чтобы попасть в розыгрыш"},
		{"2027-05-01T17:00:00Z", "draw:enter", "Прием заявок на сегодняшний розыгрыш закрыт"},
		{"2027-05-01T17:00:00Z", "draw:verify", "Прием заявок на сегодняшний розыгрыш закрыт"},
		{"2027-05-01T17:00:00Z", "promo:get", "Промокоды от Partner уже закончились"},
		{"2027-05-01T21:00:00Z", "draw:verify", "Сегодня нет активного партнера"},
	} {
		instant, err = time.Parse(time.RFC3339, tc.instant)
		if err != nil {
			t.Fatal(err)
		}
		h := bot.Handler{Config: config.Config{Location: loc}, Now: f.Now}
		key := fmt.Sprintf("calendar-%d", i)
		if err = s.Save(ctx, callbackEvent(key, tc.action)); err != nil {
			t.Fatal(err)
		}
		process(t, s, h)
		var raw []byte
		if err = s.Pool.QueryRow(ctx, `SELECT body FROM outgoing WHERE event_key=$1`, "message_callback:"+key).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var body maxapi.Body
		if err = json.Unmarshal(raw, &body); err != nil || !strings.Contains(body.Text, tc.text) {
			t.Fatalf("action %s at %s: %s, %v", tc.action, tc.instant, raw, err)
		}
	}
	if count(t, s, "daily_entries") != 0 || count(t, s, "user_chances") != 0 {
		t.Fatal("availability checks must not create participation or chances")
	}
}
