-- +goose Up
ALTER TABLE winners ADD COLUMN replaces_winner_id bigint UNIQUE REFERENCES winners(id);
CREATE INDEX winners_due ON winners(deadline) WHERE state='notified';

-- +goose Down
DROP INDEX winners_due;
ALTER TABLE winners DROP COLUMN replaces_winner_id;
