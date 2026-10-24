package jobs

import (
	"context"
	"errors"
	"strconv"
	"time"

	"example.com/max-festival-bot/internal/storage"
	"github.com/hibiken/asynq"
)

const NotificationType = "winner:notify"

type Queue struct{ Client *asynq.Client }

func (q Queue) EnqueueWinner(ctx context.Context, id int64) error {
	_, err := q.Client.EnqueueContext(ctx, asynq.NewTask(NotificationType, []byte(strconv.FormatInt(id, 10))), asynq.Queue("winners"), asynq.Timeout(55*time.Second), asynq.MaxRetry(10))
	return err
}

func Handler(store storage.Store, sender storage.WinnerSender) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		id, err := strconv.ParseInt(string(task.Payload()), 10, 64)
		if task.Type() != NotificationType || err != nil || id <= 0 {
			return errors.Join(errors.New("invalid winner task"), asynq.SkipRetry)
		}
		return store.NotifyWinner(ctx, id, sender)
	})
}
