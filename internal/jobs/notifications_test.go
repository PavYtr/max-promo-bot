package jobs

import (
	"context"
	"errors"
	"testing"

	"example.com/max-festival-bot/internal/storage"
	"github.com/hibiken/asynq"
)

func TestRejectInvalidNotificationTasks(t *testing.T) {
	handler := Handler(storage.Store{}, nil)
	for _, tc := range []struct{ kind, payload string }{{NotificationType, ""}, {NotificationType, "abc"}, {NotificationType, "-1"}, {NotificationType, "0"}, {"other", "1"}} {
		err := handler.ProcessTask(context.Background(), asynq.NewTask(tc.kind, []byte(tc.payload)))
		if !errors.Is(err, asynq.SkipRetry) {
			t.Fatalf("invalid task retried: %v", err)
		}
	}
}
