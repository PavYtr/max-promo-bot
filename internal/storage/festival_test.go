package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/max-festival-bot/internal/storage/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type calendarQuery struct {
	partners map[string]db.Partner
	date     pgtype.Date
	err      error
}

func (q *calendarQuery) GetPartnerByDay(_ context.Context, date pgtype.Date) (db.Partner, error) {
	q.date = date
	if q.err != nil {
		return db.Partner{}, q.err
	}
	if p, ok := q.partners[date.Time.Format(time.DateOnly)]; ok {
		return p, nil
	}
	return db.Partner{}, pgx.ErrNoRows
}

func TestFestivalCurrent(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		t.Fatal(err)
	}
	parse := func(s string) time.Time {
		t.Helper()
		v, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	q := &calendarQuery{partners: map[string]db.Partner{
		"2027-05-01": {ID: 1, EntryClosesAt: pgtype.Timestamptz{Time: parse("2027-05-01T20:00:00+03:00"), Valid: true}},
		"2027-05-02": {ID: 2, EntryClosesAt: pgtype.Timestamptz{Time: parse("2027-05-03T00:00:00+03:00"), Valid: true}},
		"2027-05-04": {ID: 4, EntryClosesAt: pgtype.Timestamptz{Time: parse("2027-05-04T22:00:00+03:00"), Valid: true}},
	}}
	for _, tc := range []struct {
		name, instant, day string
		partner            int64
		open               bool
	}{
		{"before festival", "2027-04-30T20:59:59.999999999Z", "2027-04-30", 0, false},
		{"first midnight", "2027-04-30T21:00:00Z", "2027-05-01", 1, true},
		{"before closing", "2027-05-01T16:59:59.999999999Z", "2027-05-01", 1, true},
		{"exact closing", "2027-05-01T17:00:00Z", "2027-05-01", 1, false},
		{"after closing partner stays active", "2027-05-01T20:59:59.999999999Z", "2027-05-01", 1, false},
		{"next partner at midnight", "2027-05-01T21:00:00Z", "2027-05-02", 2, true},
		{"closing at end of day", "2027-05-02T20:59:59.999999999Z", "2027-05-02", 2, true},
		{"calendar gap", "2027-05-02T21:00:00Z", "2027-05-03", 0, false},
		{"last day", "2027-05-03T21:00:00Z", "2027-05-04", 4, true},
		{"after festival", "2027-05-04T21:00:00Z", "2027-05-05", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			f := Festival{Location: loc, Now: func() time.Time { calls++; return parse(tc.instant) }}
			got, err := f.Current(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			var partner int64
			if got.Partner != nil {
				partner = got.Partner.ID
			}
			if got.Day != tc.day || partner != tc.partner || got.RegistrationOpen != tc.open {
				t.Fatalf("got %+v, partner %d; want day %s, partner %d, open %v", got, partner, tc.day, tc.partner, tc.open)
			}
			if calls != 1 || !q.date.Valid || q.date.Time.Location() != time.UTC || q.date.Time.Format(time.DateOnly) != tc.day {
				t.Fatalf("clock must be read once and query must use explicit local date: calls=%d date=%+v", calls, q.date)
			}
		})
	}
}

func TestFestivalErrors(t *testing.T) {
	q := &calendarQuery{err: errors.New("database unavailable")}
	if _, err := (Festival{}).Current(context.Background(), q); err == nil {
		t.Fatal("missing timezone must not use the server timezone")
	}
	if _, err := (Festival{Location: time.UTC}).Current(context.Background(), q); !errors.Is(err, q.err) {
		t.Fatalf("database error must not mean an inactive day: %v", err)
	}
}
