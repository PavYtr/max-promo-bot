-- +goose Up
ALTER TABLE winner_notifications ADD COLUMN data_recipient text CHECK (data_recipient IS NULL OR btrim(data_recipient)<>'');
UPDATE winner_notifications n SET data_recipient=p.data_recipient FROM winners w JOIN draws d ON d.id=w.draw_id JOIN partners p ON p.id=d.partner_id WHERE n.winner_id=w.id;
ALTER TABLE winners ADD COLUMN reserves_exhausted_at timestamptz;
ALTER TABLE winners ADD CONSTRAINT exhausted_only_expired CHECK (reserves_exhausted_at IS NULL OR state='expired');
UPDATE winners w SET reserves_exhausted_at=clock_timestamp()
 WHERE state='expired' AND NOT EXISTS (SELECT 1 FROM winners replacement WHERE replacement.replaces_winner_id=w.id);

-- +goose Down
ALTER TABLE winner_notifications DROP COLUMN data_recipient;
ALTER TABLE winners DROP CONSTRAINT exhausted_only_expired;
ALTER TABLE winners DROP COLUMN reserves_exhausted_at;
