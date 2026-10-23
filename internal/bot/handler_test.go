package bot

import (
	"encoding/json"
	"example.com/max-festival-bot/internal/config"
	"testing"
)

func TestMenu(t *testing.T) {
	h := Handler{Config: config.Config{BannerToken: "banner"}}
	raw, err := json.Marshal(h.Menu())
	if err != nil {
		t.Fatal(err)
	}
	var b struct {
		Text        string `json:"text"`
		Attachments []struct {
			Type    string `json:"type"`
			Payload struct {
				Token   string                                   `json:"token"`
				Buttons [][]struct{ Text, Type, Payload string } `json:"buttons"`
			} `json:"payload"`
		} `json:"attachments"`
	}
	if err = json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	if b.Text != "Добро пожаловать в Бот розыгрышей" || len(b.Attachments) != 2 || b.Attachments[0].Type != "image" || b.Attachments[0].Payload.Token != "banner" {
		t.Fatal("welcome banner contract")
	}
	rows := b.Attachments[1].Payload.Buttons
	want := []string{"promo:get", "draw:enter", "info:how_to", "profile:chances", "info:rules"}
	if len(rows) != 5 {
		t.Fatal("five buttons required")
	}
	for i, row := range rows {
		if len(row) != 1 || row[0].Type != "callback" || row[0].Payload != want[i] {
			t.Fatal("MAX uses payload, not callback_data")
		}
	}
}
