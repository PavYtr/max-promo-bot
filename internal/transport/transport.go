package transport

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"example.com/max-festival-bot/internal/maxapi"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Inbox interface {
	Save(context.Context, []byte) error
	SaveBatch(context.Context, []json.RawMessage, *int64) error
	Marker(context.Context) (*int64, error)
}

func Webhook(secret string, inbox Inbox) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if secret == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Max-Bot-Api-Secret")), []byte(secret)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		if _, _, err = maxapi.Decode(raw); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err = inbox.Save(ctx, raw); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

type Poller interface {
	Updates(context.Context, *int64) ([]json.RawMessage, *int64, error)
}

func Poll(ctx context.Context, api Poller, inbox Inbox, log *slog.Logger) {
	for ctx.Err() == nil {
		if err := PollOnce(ctx, api, inbox); err != nil && ctx.Err() == nil {
			log.Warn("polling iteration failed")
			_ = maxapi.Pause(ctx, time.Second)
		}
	}
}
func PollOnce(ctx context.Context, api Poller, inbox Inbox) error {
	marker, err := inbox.Marker(ctx)
	if err != nil {
		return err
	}
	updates, next, err := api.Updates(ctx, marker)
	if err != nil {
		return err
	}
	return inbox.SaveBatch(ctx, updates, next)
}
