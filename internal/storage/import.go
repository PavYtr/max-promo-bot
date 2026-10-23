package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"example.com/max-festival-bot/internal/config"
	"github.com/jackc/pgx/v5"
	"io"
	"strings"
	"time"
)

type Prize struct {
	Place    int    `json:"place"`
	Name     string `json:"name"`
	Quantity int    `json:"quantity"`
}
type Partner struct {
	Day               string    `json:"festival_day"`
	Name              string    `json:"name"`
	ChatID            int64     `json:"chat_id"`
	Link              string    `json:"link"`
	PromoExpiresAt    time.Time `json:"promo_expires_at"`
	PromoInstructions string    `json:"promo_instructions"`
	WinnerCount       int       `json:"winner_count"`
	Prizes            []Prize   `json:"prizes"`
	ReserveCount      int       `json:"reserve_count"`
	ResponseSeconds   int       `json:"response_seconds"`
	EntryClosesAt     time.Time `json:"entry_closes_at"`
	ClaimInstructions string    `json:"claim_instructions"`
	DataRecipient     string    `json:"data_recipient"`
	Codes             []string  `json:"codes"`
}
type Import struct {
	Partners []Partner `json:"partners"`
}

func ParseImport(raw []byte, loc *time.Location) (Import, error) {
	var data Import
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&data); err != nil {
		return data, errors.New("invalid import JSON")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return data, errors.New("expected one JSON document")
	}
	if len(data.Partners) == 0 {
		return data, errors.New("partners must not be empty")
	}
	days := map[string]bool{}
	for _, p := range data.Partners {
		day, e := time.ParseInLocation("2006-01-02", p.Day, loc)
		if e != nil || days[p.Day] {
			return data, errors.New("invalid or duplicate festival_day")
		}
		days[p.Day] = true
		if strings.TrimSpace(p.Name) == "" || p.ChatID == 0 || !config.HTTPS(p.Link) || strings.TrimSpace(p.PromoInstructions) == "" || strings.TrimSpace(p.ClaimInstructions) == "" || strings.TrimSpace(p.DataRecipient) == "" {
			return data, errors.New("missing partner fields or invalid link")
		}
		if p.WinnerCount < 1 || p.WinnerCount > 10000 || p.ReserveCount < 0 || p.ReserveCount > 10000 || p.ResponseSeconds < 1 || p.ResponseSeconds > 2147483647 {
			return data, errors.New("invalid winner, reserve or response settings")
		}
		next := day.AddDate(0, 0, 1)
		if !p.EntryClosesAt.After(day) || p.EntryClosesAt.After(next) || p.PromoExpiresAt.Before(next) {
			return data, errors.New("closing time must be within festival day; promo must cover day")
		}
		if len(p.Prizes) != p.WinnerCount {
			return data, errors.New("one prize allocation per winning place is required")
		}
		places := map[int]bool{}
		for _, prize := range p.Prizes {
			if prize.Place < 1 || prize.Place > p.WinnerCount || places[prize.Place] || strings.TrimSpace(prize.Name) == "" || prize.Quantity < 1 || prize.Quantity > 2147483647 {
				return data, errors.New("invalid or duplicate prize place")
			}
			places[prize.Place] = true
		}
		codes := map[string]bool{}
		for _, code := range p.Codes {
			if strings.TrimSpace(code) == "" || code != strings.TrimSpace(code) || codes[code] {
				return data, errors.New("empty or duplicate promo code")
			}
			codes[code] = true
		}
	}
	return data, nil
}

// Strict insert-only import: a conflicting date or code rolls back the entire file.
func ImportPartners(ctx context.Context, tx pgx.Tx, data Import) error {
	for _, p := range data.Partners {
		prizes, err := json.Marshal(p.Prizes)
		if err != nil {
			return err
		}
		var id int64
		err = tx.QueryRow(ctx, `INSERT INTO partners(festival_day,name,chat_id,link,promo_expires_at,promo_instructions,winner_count,prizes,reserve_count,response_seconds,entry_closes_at,claim_instructions,data_recipient)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id`, p.Day, p.Name, p.ChatID, p.Link, p.PromoExpiresAt, p.PromoInstructions, p.WinnerCount, prizes, p.ReserveCount, p.ResponseSeconds, p.EntryClosesAt, p.ClaimInstructions, p.DataRecipient).Scan(&id)
		if err != nil {
			return errors.New("partner import rejected (check duplicate dates and fields)")
		}
		for _, code := range p.Codes {
			if _, err = tx.Exec(ctx, `INSERT INTO promocodes(partner_id,code) VALUES($1,$2)`, id, code); err != nil {
				return errors.New("promo import rejected")
			}
		}
	}
	return nil
}
