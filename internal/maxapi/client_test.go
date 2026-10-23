package maxapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type gateStub struct{ calls, cooldowns int }

func (g *gateStub) Wait(context.Context, int64) error             { g.calls++; return nil }
func (g *gateStub) Cooldown(context.Context, time.Duration) error { g.cooldowns++; return nil }
func TestClientContractsAndRetries(t *testing.T) {
	calls := 0
	gate := &gateStub{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "test-token" || r.URL.Query().Has("access_token") {
			t.Error("authorization contract")
		}
		if calls == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"code":"rate.limit","message":"private"}`))
			return
		}
		if r.URL.Path != "/messages" || r.URL.Query().Get("chat_id") != "99" {
			t.Error("message destination")
		}
		var body Body
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Text != "hello" {
			t.Error("invalid message body")
		}
		_, _ = w.Write([]byte(`{"message":{}}`))
	}))
	defer server.Close()
	c, err := New(server.URL, "test-token", "", gate)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Send(context.Background(), 99, "", Body{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || gate.calls != 2 || gate.cooldowns != 1 {
		t.Fatal("every retry must acquire limiter and 429 must set shared cooldown")
	}
}
func TestErrorsBoundedAndSanitized(t *testing.T) {
	for _, status := range []int{401, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"code":"failure","message":"secret-token user-data"}`))
			}))
			defer s.Close()
			c, _ := New(s.URL, "secret-token", "", nil)
			err := c.Call(context.Background(), "GET", "/me", nil, nil, 0, nil)
			var api *APIError
			if !errors.As(err, &api) || api.Status != status || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unexpected error: %v", err)
			}
			want := 1
			if status == 500 {
				want = 3
			}
			if calls != want {
				t.Fatalf("%d calls; want %d", calls, want)
			}
		})
	}
}
func TestCallbackAndMembership(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/answers":
			if r.URL.Query().Get("callback_id") != "cb" {
				t.Error("callback_id missing")
			}
			var body struct {
				Message Body `json:"message"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.Message.Text != "info" {
				t.Error("callback body")
			}
			_, _ = w.Write([]byte(`{"success":true}`))
		case "/chats/-3/members":
			if r.URL.Query().Get("user_ids") != "7" {
				t.Error("user filter missing")
			}
			_, _ = w.Write([]byte(`{"members":[{"user_id":7}]}`))
		default:
			t.Error("unexpected path")
			w.WriteHeader(404)
		}
	}))
	defer s.Close()
	c, _ := New(s.URL, "token", "", nil)
	if err := c.Send(context.Background(), 5, "cb", Body{Text: "info"}); err != nil {
		t.Fatal(err)
	}
	yes, err := c.CheckMembership(context.Background(), -3, 7)
	if err != nil || !yes {
		t.Fatal("membership contract")
	}
}
func TestCancellationTLSAndRetryAfter(t *testing.T) {
	c, _ := New("http://127.0.0.1:1", "secret-token", "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Call(ctx, "GET", "/me", url.Values{}, nil, 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := New("https://example.com", "token", "missing-ca", nil); err == nil {
		t.Fatal("missing CA should fail")
	}
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS accepted") }))
	defer s.Close()
	c, _ = New(s.URL, "token", "", nil)
	if err := c.Call(context.Background(), "POST", "/me", nil, nil, 0, nil); err == nil || strings.Contains(err.Error(), "token") {
		t.Fatal("TLS validation disabled or secret leaked")
	}
	now := time.Now().UTC().Truncate(time.Second)
	if retryAfter("3", now) != 3*time.Second || retryAfter(now.Add(4*time.Second).Format(http.TimeFormat), now) != 4*time.Second || retryAfter("invalid", now) != 0 {
		t.Fatal("Retry-After parsing")
	}
}

func TestMembershipAbsenceAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{"absent", 200, `{"members":[]}`, false},
		{"different user", 200, `{"members":[{"user_id":8}]}`, false},
		{"no permissions", 403, `{"code":"access.denied"}`, true},
		{"invalid response", 200, `not json`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/chats/-3/members" || r.URL.Query().Get("user_ids") != "7" {
					t.Error("wrong subscription request")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer s.Close()
			client, err := New(s.URL, "token", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			member, err := client.CheckMembership(context.Background(), -3, 7)
			if member || (err != nil) != tc.wantErr {
				t.Fatalf("member=%v error=%v", member, err)
			}
		})
	}
}
func TestDecodeIdentity(t *testing.T) {
	a := []byte(`{"update_type":"message_created","timestamp":1,"message":{"sender":{"user_id":7},"recipient":{"chat_id":9},"body":{"mid":"m1","text":"/start"}}}`)
	b := []byte(`{"timestamp":2,"message":{"sender":{"user_id":7},"recipient":{"chat_id":9},"body":{"mid":"m1","text":"/start"}},"update_type":"message_created"}`)
	_, k, e := Decode(a)
	_, k2, e2 := Decode(b)
	if e != nil || e2 != nil || k != k2 {
		t.Fatal("message id should deduplicate deliveries")
	}
	for _, raw := range []string{`{}`, `{"update_type":"bot_started"}`, `{"update_type":"message_callback","callback":{"callback_id":"x"}}`} {
		if _, _, err := Decode([]byte(raw)); err == nil {
			t.Fatal("malformed update accepted")
		}
	}
}

func TestExpiredCallbackFallbackAndFailedResult(t *testing.T) {
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == "/answers" {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"code":"callback.expired"}`))
			return
		}
		_, _ = w.Write([]byte(`{"success":false}`))
	}))
	defer server.Close()
	c, _ := New(server.URL, "token", "", nil)
	err := c.Send(context.Background(), 9, "expired", Body{Text: "Saved screen"})
	var api *APIError
	if !errors.As(err, &api) || api.Status != 400 {
		t.Fatal("unsuccessful API result must not mark delivery successful")
	}
	if len(paths) != 2 || paths[0] != "/answers" || paths[1] != "/messages" {
		t.Fatal("expired callback must fall back to normal message")
	}
}
