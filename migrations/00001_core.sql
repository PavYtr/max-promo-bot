-- +goose Up
CREATE TABLE users (
    id bigint PRIMARY KEY CHECK (id > 0),
    first_name text NOT NULL,
    last_name text NOT NULL DEFAULT '',
    username text NOT NULL DEFAULT '',
    first_started_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE partners (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    festival_day date NOT NULL UNIQUE,
    name text NOT NULL CHECK (btrim(name) <> ''),
    chat_id bigint NOT NULL CHECK (chat_id <> 0),
    link text NOT NULL CHECK (link LIKE 'https://%'),
    promo_expires_at timestamptz NOT NULL,
    promo_instructions text NOT NULL CHECK (btrim(promo_instructions) <> ''),
    winner_count integer NOT NULL CHECK (winner_count > 0),
    prizes jsonb NOT NULL CHECK (jsonb_typeof(prizes) = 'array' AND jsonb_array_length(prizes) > 0),
    reserve_count integer NOT NULL CHECK (reserve_count >= 0),
    response_seconds integer NOT NULL CHECK (response_seconds > 0),
    entry_closes_at timestamptz NOT NULL,
    claim_instructions text NOT NULL CHECK (btrim(claim_instructions) <> ''),
    data_recipient text NOT NULL CHECK (btrim(data_recipient) <> '')
);
CREATE TABLE promocodes (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    partner_id bigint NOT NULL REFERENCES partners(id),
    code text NOT NULL CHECK (btrim(code) <> ''),
    user_id bigint REFERENCES users(id),
    issued_at timestamptz,
    UNIQUE (partner_id, code),
    CHECK ((user_id IS NULL) = (issued_at IS NULL))
);
CREATE UNIQUE INDEX promocodes_recipient ON promocodes (partner_id, user_id) WHERE user_id IS NOT NULL;
CREATE INDEX promocodes_available ON promocodes (partner_id, id) WHERE user_id IS NULL;
CREATE TABLE daily_entries (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id bigint NOT NULL REFERENCES users(id),
    partner_id bigint NOT NULL REFERENCES partners(id),
    registered_at timestamptz NOT NULL DEFAULT now(),
    subscription_checked_at timestamptz NOT NULL,
    confirmation_status text NOT NULL DEFAULT 'pending' CHECK (confirmation_status IN ('pending','sent','failed')),
    confirmed_at timestamptz,
    UNIQUE (partner_id, user_id),
    CHECK ((confirmation_status = 'sent') = (confirmed_at IS NOT NULL))
);
CREATE TABLE user_chances (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id bigint NOT NULL REFERENCES users(id),
    festival_day date NOT NULL,
    partner_id bigint NOT NULL REFERENCES partners(id),
    granted_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, festival_day)
);
CREATE TABLE draws (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('daily','grand')),
    partner_id bigint REFERENCES partners(id),
    status text NOT NULL CHECK (status IN ('pending_approval','approved','completed')),
    drawn_at timestamptz NOT NULL,
    approved_at timestamptz,
    approved_by bigint,
    CHECK ((kind = 'daily') = (partner_id IS NOT NULL)),
    CHECK ((approved_at IS NULL) = (approved_by IS NULL)),
    CHECK ((status = 'pending_approval') = (approved_at IS NULL))
);
CREATE UNIQUE INDEX draws_daily ON draws(partner_id) WHERE kind = 'daily';
CREATE UNIQUE INDEX draws_grand ON draws(kind) WHERE kind = 'grand';
CREATE TABLE draw_participants (
    draw_id bigint NOT NULL REFERENCES draws(id),
    user_id bigint NOT NULL REFERENCES users(id),
    weight integer NOT NULL CHECK (weight BETWEEN 1 AND 10),
    -- Snapshot includes registration time or all chance days, partner and user data.
    snapshot jsonb NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
    PRIMARY KEY (draw_id, user_id)
);
-- +goose StatementBegin
CREATE FUNCTION protect_draw_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'draw participants are immutable';
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER draw_snapshot_immutable BEFORE UPDATE OR DELETE ON draw_participants FOR EACH ROW EXECUTE FUNCTION protect_draw_snapshot();
CREATE TABLE winners (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    draw_id bigint NOT NULL,
    user_id bigint NOT NULL,
    role text NOT NULL CHECK (role IN ('primary','reserve')),
    reserve_order integer CHECK (reserve_order > 0),
    prize text NOT NULL CHECK (btrim(prize) <> ''),
    prize_quantity integer NOT NULL CHECK (prize_quantity > 0),
    state text NOT NULL CHECK (state IN ('reserve','pending_notification','notified','claimed','expired','unreachable')),
    notified_at timestamptz,
    deadline timestamptz,
    first_name text,
    last_name text,
    consent_at timestamptz,
    data_recipient text,
    UNIQUE (draw_id, user_id),
    UNIQUE (draw_id, reserve_order),
    FOREIGN KEY (draw_id,user_id) REFERENCES draw_participants(draw_id,user_id),
    CHECK ((role = 'reserve') = (reserve_order IS NOT NULL)),
    CHECK (state <> 'reserve' OR (deadline IS NULL AND notified_at IS NULL)),
    CHECK (state <> 'claimed' OR (first_name IS NOT NULL AND last_name IS NOT NULL AND consent_at IS NOT NULL AND data_recipient IS NOT NULL))
);
-- Inbox is acknowledged only after commit; processing and replies commit together.
CREATE TABLE inbox (
    event_key text PRIMARY KEY,
    payload jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);
CREATE INDEX inbox_pending ON inbox(received_at) WHERE processed_at IS NULL;
CREATE TABLE outgoing (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_key text NOT NULL REFERENCES inbox(event_key),
    chat_id bigint NOT NULL,
    callback_id text,
    body jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','sent','failed')),
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    sent_at timestamptz,
    UNIQUE (event_key)
);
CREATE INDEX outgoing_pending ON outgoing(available_at,id) WHERE status = 'pending';
CREATE TABLE poll_cursor (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    marker bigint
);
INSERT INTO poll_cursor(singleton) VALUES (true);

-- +goose Down
DROP TABLE poll_cursor, outgoing, inbox, winners, draw_participants, draws, user_chances, daily_entries, promocodes, partners, users;
DROP FUNCTION protect_draw_snapshot();
