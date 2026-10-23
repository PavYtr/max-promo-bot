package storage

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestImportValidation(t *testing.T) {
	raw, err := os.ReadFile("../../examples/partners.json")
	if err != nil {
		t.Fatal(err)
	}
	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		t.Fatal(err)
	}
	good, err := ParseImport(raw, loc)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Import)
	}{
		{"duplicate day", func(d *Import) { d.Partners = append(d.Partners, d.Partners[0]) }},
		{"invalid day", func(d *Import) { d.Partners[0].Day = "2027-02-30" }},
		{"empty name", func(d *Import) { d.Partners[0].Name = " " }},
		{"duplicate code", func(d *Import) { d.Partners[0].Codes = []string{"x", "x"} }},
		{"closing time", func(d *Import) { d.Partners[0].EntryClosesAt = d.Partners[0].EntryClosesAt.AddDate(0, 0, 1) }},
		{"expiration", func(d *Import) { d.Partners[0].PromoExpiresAt = time.Time{} }},
		{"prize count", func(d *Import) { d.Partners[0].WinnerCount = 2 }},
		{"prize quantity", func(d *Import) { d.Partners[0].Prizes[0].Quantity = 0 }},
		{"response time", func(d *Import) { d.Partners[0].ResponseSeconds = 0 }},
		{"chat", func(d *Import) { d.Partners[0].ChatID = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var d Import
			b, _ := json.Marshal(good)
			_ = json.Unmarshal(b, &d)
			tc.mutate(&d)
			b, _ = json.Marshal(d)
			if _, err = ParseImport(b, loc); err == nil {
				t.Fatal("invalid import accepted")
			}
		})
	}
	for _, bad := range []string{`{"partners":[]}`, `{"unknown":1}`, string(raw) + `{}`} {
		if _, err = ParseImport([]byte(bad), loc); err == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
}
