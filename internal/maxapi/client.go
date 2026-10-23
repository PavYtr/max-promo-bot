package maxapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Gate interface {
	Wait(context.Context, int64) error
	Cooldown(context.Context, time.Duration) error
}
type Client struct {
	base, token string
	http        *http.Client
	gate        Gate
}
type APIError struct {
	Status     int
	Code       string
	RetryAfter time.Duration
}

func (e *APIError) Error() string { return fmt.Sprintf("MAX API HTTP %d", e.Status) }
func New(base, token, caFile string, gate Gate) (*Client, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.New("load system certificates")
	}
	if caFile != "" {
		pem, e := os.ReadFile(caFile)
		if e != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("load MAX CA certificate")
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	return &Client{base: strings.TrimRight(base, "/"), token: token, gate: gate, http: &http.Client{Transport: tr, Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Call(ctx context.Context, method, path string, q url.Values, body any, chat int64, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if body == nil {
		b = nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		if c.gate != nil {
			if err = c.gate.Wait(ctx, chat); err != nil {
				return err
			}
		}
		req, e := http.NewRequestWithContext(ctx, method, c.base+path+"?"+q.Encode(), bytes.NewReader(b))
		if e != nil {
			return errors.New("invalid MAX request")
		}
		req.Header.Set("Authorization", c.token)
		req.Header.Set("Content-Type", "application/json")
		res, e := c.http.Do(req)
		if e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// POST may already have taken effect. Durable outgoing handles recovery.
			if method != http.MethodGet || attempt == 2 {
				return errors.New("MAX transport error")
			}
			if err = Pause(ctx, time.Duration(attempt+1)*time.Second); err != nil {
				return err
			}
			continue
		}
		raw, e := io.ReadAll(io.LimitReader(res.Body, 2<<20))
		res.Body.Close()
		if e != nil {
			return errors.New("read MAX response")
		}
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			var result struct {
				Success *bool `json:"success"`
			}
			if json.Unmarshal(raw, &result) != nil {
				return errors.New("invalid MAX response")
			}
			if result.Success != nil && !*result.Success {
				return &APIError{Status: http.StatusBadRequest}
			}
			if out != nil {
				if err = json.Unmarshal(raw, out); err != nil {
					return errors.New("invalid MAX response")
				}
			}
			return nil
		}
		var failure struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(raw, &failure)
		delay := retryAfter(res.Header.Get("Retry-After"), time.Now())
		if delay == 0 {
			delay = time.Duration(attempt+1) * time.Second
		}
		apiErr := &APIError{Status: res.StatusCode, Code: failure.Code, RetryAfter: delay}
		temporary := res.StatusCode == 429 || res.StatusCode >= 500 || failure.Code == "attachment.not.ready"
		if res.StatusCode == 429 && c.gate != nil {
			if err = c.gate.Cooldown(ctx, delay); err != nil {
				return errors.New("rate limiter unavailable")
			}
		}
		if !temporary || attempt == 2 {
			return apiErr
		}
		if err = Pause(ctx, delay); err != nil {
			return err
		}
	}
	return errors.New("MAX retries exhausted")
}
func retryAfter(value string, now time.Time) time.Duration {
	if n, err := strconv.ParseInt(value, 10, 32); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
func (c *Client) Send(ctx context.Context, chat int64, callback string, body Body) error {
	if callback != "" {
		err := c.Call(ctx, "POST", "/answers", url.Values{"callback_id": {callback}}, map[string]any{"message": body}, chat, nil)
		var api *APIError
		// After a long outage the callback may expire; deliver the saved screen as a message.
		if !errors.As(err, &api) || (api.Status != 400 && api.Status != 404) {
			return err
		}
	}
	return c.Call(ctx, "POST", "/messages", url.Values{"chat_id": {strconv.FormatInt(chat, 10)}}, body, chat, nil)
}
func (c *Client) Updates(ctx context.Context, marker *int64) ([]json.RawMessage, *int64, error) {
	q := url.Values{"timeout": {"30"}, "limit": {"100"}, "types": {"bot_started,message_created,message_callback"}}
	if marker != nil {
		q.Set("marker", strconv.FormatInt(*marker, 10))
	}
	var result struct {
		Updates []json.RawMessage `json:"updates"`
		Marker  *int64            `json:"marker"`
	}
	err := c.Call(ctx, "GET", "/updates", q, nil, 0, &result)
	return result.Updates, result.Marker, err
}
func (c *Client) Subscribe(ctx context.Context, endpoint, secret string) error {
	var result struct {
		Success bool `json:"success"`
	}
	err := c.Call(ctx, "POST", "/subscriptions", nil, map[string]any{"url": endpoint, "secret": secret, "update_types": []string{"bot_started", "message_created", "message_callback"}}, 0, &result)
	if err == nil && !result.Success {
		return errors.New("MAX subscription rejected")
	}
	return err
}
func (c *Client) CheckMembership(ctx context.Context, chat, user int64) (bool, error) {
	var result struct {
		Members []User `json:"members"`
	}
	err := c.Call(ctx, "GET", fmt.Sprintf("/chats/%d/members", chat), url.Values{"user_ids": {strconv.FormatInt(user, 10)}}, nil, 0, &result)
	if err != nil {
		return false, err
	}
	for _, m := range result.Members {
		if m.ID == user {
			return true, nil
		}
	}
	return false, nil
}
