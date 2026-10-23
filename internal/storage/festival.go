package storage

import (
	"context"
	"errors"
	"time"

	"example.com/max-festival-bot/internal/storage/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Festival struct {
	Location *time.Location
	Now      func() time.Time
}

type FestivalDay struct {
	Day              string
	Partner          *db.Partner
	RegistrationOpen bool
}

// Current uses imported partner dates as the festival calendar, including gaps.
// Call it in the scenario's transaction before creating an entry or granting a chance.
func (f Festival) Current(ctx context.Context, q interface {
	GetPartnerByDay(context.Context, pgtype.Date) (db.Partner, error)
}) (FestivalDay, error) {
	if f.Location == nil {
		return FestivalDay{}, errors.New("festival timezone is required")
	}
	now := time.Now
	if f.Now != nil {
		now = f.Now
	}
	instant := now()
	local := instant.In(f.Location)
	year, month, day := local.Date()
	result := FestivalDay{Day: local.Format(time.DateOnly)}
	// PostgreSQL date has no timezone; encode the local calendar date explicitly.
	date := pgtype.Date{Time: time.Date(year, month, day, 0, 0, 0, 0, time.UTC), Valid: true}
	partner, err := q.GetPartnerByDay(ctx, date)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return FestivalDay{}, err
	}
	result.Partner = &partner
	result.RegistrationOpen = instant.Before(partner.EntryClosesAt.Time)
	return result, nil
}
