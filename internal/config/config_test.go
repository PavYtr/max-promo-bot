package config

import "testing"

func TestConfiguration(t *testing.T) {
	t.Setenv("BOT_MODE", "webhook")
	t.Setenv("WEBHOOK_SECRET", "a-long-test-secret")
	t.Setenv("ADMIN_IDS", "10,20")
	t.Setenv("ADMIN_CHAT_ID", "-30")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.IsAdmin(10, -30) || c.IsAdmin(11, -30) || c.IsAdmin(10, -31) {
		t.Fatal("admin must match both allowlist and chat")
	}
	for _, tc := range []struct{ key, value string }{{"WEBHOOK_SECRET", "short"}, {"ADMIN_IDS", "broken"}, {"ADMIN_CHAT_ID", "0"}, {"BOT_MODE", "other"}, {"FESTIVAL_TIMEZONE", "No/Such"}, {"BANNER_URL", "http://example.com/x"}, {"MAX_API_URL", "http://example.com"}, {"MAX_API_URL", "https://token@example.com"}} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestGrandReserveCount(t *testing.T) {
	t.Setenv("BOT_MODE", "polling")
	for _, raw := range []string{"", "0", "3", "10000"} {
		t.Setenv("GRAND_RESERVE_COUNT", raw)
		c, err := Load()
		if err != nil || (raw == "") != (c.GrandReserveCount == nil) {
			t.Fatal(raw, c.GrandReserveCount, err)
		}
		if raw == "3" && *c.GrandReserveCount != 3 {
			t.Fatal("wrong reserve count")
		}
	}
	for _, raw := range []string{"-1", "10001", "abc", "9223372036854775808"} {
		t.Setenv("GRAND_RESERVE_COUNT", raw)
		if _, err := Load(); err == nil {
			t.Fatal("invalid reserve count accepted", raw)
		}
	}
}

func TestGrandResponseSeconds(t *testing.T) {
	t.Setenv("BOT_MODE", "polling")
	for _, raw := range []string{"", "1", "7200", "2147483647"} {
		t.Setenv("GRAND_RESPONSE_SECONDS", raw)
		if _, err := Load(); err != nil {
			t.Fatal(raw, err)
		}
	}
	for _, raw := range []string{"0", "-1", "abc", "2147483648"} {
		t.Setenv("GRAND_RESPONSE_SECONDS", raw)
		if _, err := Load(); err == nil {
			t.Fatal("invalid response period", raw)
		}
	}
}
