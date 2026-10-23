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
