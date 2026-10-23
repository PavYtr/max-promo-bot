package transport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

type inboxStub struct {
	saves  int
	fail   bool
	marker *int64
}

func (i *inboxStub) Save(context.Context, []byte) error {
	if i.fail {
		return errors.New("database unavailable")
	}
	i.saves++
	return nil
}
func (i *inboxStub) SaveBatch(_ context.Context, _ []json.RawMessage, m *int64) error {
	if i.fail {
		return errors.New("commit failed")
	}
	i.saves++
	i.marker = m
	return nil
}
func (i *inboxStub) Marker(context.Context) (*int64, error) { return i.marker, nil }

type pollStub struct{ seen *int64 }

func (p *pollStub) Updates(_ context.Context, m *int64) ([]json.RawMessage, *int64, error) {
	p.seen = m
	n := int64(42)
	return nil, &n, nil
}
func TestWebhookAuthenticationAndCommit(t *testing.T) {
	inbox := &inboxStub{}
	h := Webhook("a-long-secret", inbox)
	raw := `{"update_type":"bot_started","timestamp":1,"chat_id":8,"user":{"user_id":7}}`
	for _, tc := range []struct {
		secret, body string
		fail         bool
		status       int
	}{{"bad", raw, false, 401}, {"a-long-secret", `{}`, false, 400}, {"a-long-secret", raw, true, 503}, {"a-long-secret", raw, false, 200}} {
		inbox.fail = tc.fail
		r := httptest.NewRequest("POST", "/webhook", strings.NewReader(tc.body))
		r.Header.Set("X-Max-Bot-Api-Secret", tc.secret)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("status %d want %d", w.Code, tc.status)
		}
	}
	if inbox.saves != 1 {
		t.Fatal("invalid or failed events must not be acknowledged")
	}
}
func TestPollingDoesNotAdvanceOnFailedCommit(t *testing.T) {
	old := int64(10)
	inbox := &inboxStub{fail: true, marker: &old}
	poll := &pollStub{}
	if err := PollOnce(context.Background(), poll, inbox); err == nil || *inbox.marker != 10 {
		t.Fatal("failed commit advanced cursor")
	}
	inbox.fail = false
	if err := PollOnce(context.Background(), poll, inbox); err != nil || *poll.seen != 10 || *inbox.marker != 42 {
		t.Fatal("cursor recovery failed")
	}
}
