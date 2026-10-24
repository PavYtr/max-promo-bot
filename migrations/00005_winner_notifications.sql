-- +goose Up
ALTER TABLE users ADD COLUMN private_chat_id bigint;
UPDATE users u SET private_chat_id=(SELECT (payload->>'chat_id')::bigint FROM inbox
 WHERE payload->>'update_type'='bot_started' AND payload->'user'->>'user_id'=u.id::text
 ORDER BY received_at DESC LIMIT 1);
CREATE TABLE winner_notifications (
    winner_id bigint PRIMARY KEY REFERENCES winners(id),
    body jsonb NOT NULL,
    response_seconds integer NOT NULL CHECK (response_seconds > 0),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','sent','failed')),
    attempts integer NOT NULL DEFAULT 0,
    next_enqueue_at timestamptz NOT NULL DEFAULT now(),
    sent_at timestamptz,
    CHECK ((status = 'sent') = (sent_at IS NOT NULL))
);
CREATE INDEX winner_notifications_pending ON winner_notifications(next_enqueue_at) WHERE status='pending';

-- +goose Down
DROP TABLE winner_notifications;
ALTER TABLE users DROP COLUMN private_chat_id;
