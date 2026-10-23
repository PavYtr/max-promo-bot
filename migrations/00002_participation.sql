-- +goose Up
CREATE TABLE promo_requests (
    event_key text PRIMARY KEY REFERENCES inbox(event_key),
    user_id bigint NOT NULL REFERENCES users(id),
    partner_id bigint REFERENCES partners(id),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE outgoing ADD COLUMN daily_entry_id bigint REFERENCES daily_entries(id);

-- +goose Down
ALTER TABLE outgoing DROP COLUMN daily_entry_id;
DROP TABLE promo_requests;
