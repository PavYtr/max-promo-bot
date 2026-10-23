package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage/db"
	"github.com/jackc/pgx/v5"
)

// LockParticipant serializes all chance-producing actions for one user, across days.
func LockParticipant(ctx context.Context, tx pgx.Tx, user int64) error {
	var id int64
	return tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, user).Scan(&id)
}

// GrantDailyChance locks the user in the caller's transaction to enforce the total cap.
func GrantDailyChance(ctx context.Context, tx pgx.Tx, user, partner int64, now time.Time, loc *time.Location) (int, error) {
	if loc == nil {
		return 0, errors.New("festival timezone is required")
	}
	if err := LockParticipant(ctx, tx, user); err != nil {
		return 0, err
	}
	_, err := tx.Exec(ctx, `INSERT INTO user_chances(user_id,festival_day,partner_id,granted_at)
	 SELECT $1,festival_day,id,$3 FROM partners WHERE id=$2 AND festival_day=$4::date
	 AND (SELECT count(*) FROM user_chances WHERE user_id=$1)<10 ON CONFLICT DO NOTHING`, user, partner, now, now.In(loc).Format(time.DateOnly))
	if err != nil {
		return 0, err
	}
	return ChanceCount(ctx, tx, user)
}

func ChanceCount(ctx context.Context, tx pgx.Tx, user int64) (int, error) {
	var n int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM user_chances WHERE user_id=$1`, user).Scan(&n)
	return n, err
}

// IssuePromo serializes the recipient; code and chance commit together.
func IssuePromo(ctx context.Context, tx pgx.Tx, user int64, p db.Partner, now time.Time, loc *time.Location) (string, error) {
	if err := LockParticipant(ctx, tx, user); err != nil {
		return "", err
	}
	var code string
	err := tx.QueryRow(ctx, `SELECT code FROM promocodes WHERE partner_id=$1 AND user_id=$2`, p.ID, user).Scan(&code)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		for attempt := 0; attempt < 3; attempt++ {
			err = tx.QueryRow(ctx, `UPDATE promocodes SET user_id=$2,issued_at=$3 WHERE id=(
			 SELECT id FROM promocodes WHERE partner_id=$1 AND user_id IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING code`, p.ID, user, now).Scan(&code)
			if !errors.Is(err, pgx.ErrNoRows) {
				break
			}
			if attempt < 2 {
				if err = maxapi.Pause(ctx, 50*time.Millisecond); err != nil {
					return "", err
				}
			}
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
	}
	_, err = GrantDailyChance(ctx, tx, user, p.ID, now, loc)
	return code, err
}

func Entry(ctx context.Context, tx pgx.Tx, user, partner int64) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `SELECT id FROM daily_entries WHERE user_id=$1 AND partner_id=$2`, user, partner).Scan(&id)
	return id, err
}

func RegisterDaily(ctx context.Context, tx pgx.Tx, user int64, p db.Partner, now time.Time, loc *time.Location) (int64, int, error) {
	if err := LockParticipant(ctx, tx, user); err != nil {
		return 0, 0, err
	}
	var id int64
	err := tx.QueryRow(ctx, `INSERT INTO daily_entries(user_id,partner_id,registered_at,subscription_checked_at)
	 VALUES($1,$2,$3,$3) ON CONFLICT(partner_id,user_id) DO UPDATE SET user_id=EXCLUDED.user_id RETURNING id`, user, p.ID, now).Scan(&id)
	if err != nil {
		return 0, 0, err
	}
	n, err := GrantDailyChance(ctx, tx, user, p.ID, now, loc)
	return id, n, err
}

func Confirmation(ctx context.Context, tx pgx.Tx, key string, chat int64, callback string, body maxapi.Body, entry int64) error {
	if err := Reply(ctx, tx, key, chat, callback, body); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE outgoing SET daily_entry_id=$2 WHERE event_key=$1`, key, entry)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE daily_entries SET confirmation_status='pending' WHERE id=$1 AND confirmation_status='failed'`, entry)
	return err
}

func ChanceProfile(ctx context.Context, tx pgx.Tx, user int64, day string) (string, error) {
	rows, err := tx.Query(ctx, `SELECT p.festival_day::text,p.name FROM partners p WHERE
	 EXISTS(SELECT 1 FROM user_chances WHERE partner_id=p.id AND user_id=$1) OR
	 EXISTS(SELECT 1 FROM promocodes WHERE partner_id=p.id AND user_id=$1) OR
	 EXISTS(SELECT 1 FROM daily_entries WHERE partner_id=p.id AND user_id=$1)
	 ORDER BY p.festival_day`, user)
	if err != nil {
		return "", err
	}
	var days []string
	for rows.Next() {
		var date, name string
		if err = rows.Scan(&date, &name); err != nil {
			rows.Close()
			return "", err
		}
		days = append(days, date+" — "+name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	var remaining int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM partners WHERE festival_day>$1::date`, day).Scan(&remaining); err != nil {
		return "", err
	}
	activity := strings.Join(days, "\n")
	if activity == "" {
		activity = "Пока нет активных дней."
	}
	n, err := ChanceCount(ctx, tx, user)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Твой фестивальный пропуск\nНакоплено шансов: %d из 10\nПройденные дни и партнеры:\n%s\nОсталось фестивальных дней: %d\nГлавный приз: пять победителей получат по два билета на Канье Уэста, всего 10 билетов.\nЧем больше активных дней, тем выше вероятность выиграть главный приз.", n, activity, remaining), nil
}
