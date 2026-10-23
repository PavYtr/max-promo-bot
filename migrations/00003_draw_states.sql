-- +goose Up
-- Reserves receive a concrete prize only when a winning place is vacated.
ALTER TABLE winners ALTER COLUMN prize DROP NOT NULL;
ALTER TABLE winners ALTER COLUMN prize_quantity DROP NOT NULL;
ALTER TABLE winners ADD CONSTRAINT winner_prize_assignment CHECK (
    (prize IS NOT NULL AND prize_quantity IS NOT NULL) OR
    (state = 'reserve' AND prize IS NULL AND prize_quantity IS NULL)
);
-- +goose StatementBegin
CREATE FUNCTION enforce_draw_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind IS DISTINCT FROM OLD.kind OR NEW.partner_id IS DISTINCT FROM OLD.partner_id
       OR NEW.drawn_at IS DISTINCT FROM OLD.drawn_at THEN
        RAISE EXCEPTION 'draw identity and time are immutable';
    END IF;
    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'pending_approval' AND NEW.status = 'approved') OR
        (OLD.status = 'approved' AND NEW.status = 'completed')
    ) THEN RAISE EXCEPTION 'invalid draw transition'; END IF;
    IF OLD.approved_at IS NOT NULL AND
       (NEW.approved_at IS DISTINCT FROM OLD.approved_at OR NEW.approved_by IS DISTINCT FROM OLD.approved_by) THEN
        RAISE EXCEPTION 'draw approval is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER draw_transition BEFORE UPDATE ON draws FOR EACH ROW EXECUTE FUNCTION enforce_draw_transition();
-- +goose StatementBegin
CREATE FUNCTION enforce_winner_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.draw_id <> OLD.draw_id OR NEW.user_id <> OLD.user_id OR NEW.role <> OLD.role
       OR NEW.reserve_order IS DISTINCT FROM OLD.reserve_order THEN
        RAISE EXCEPTION 'winner identity and selection order are immutable';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'reserve' AND NEW.state = 'pending_notification') OR
        (OLD.state = 'pending_notification' AND NEW.state IN ('notified','unreachable')) OR
        (OLD.state = 'notified' AND NEW.state IN ('claimed','expired'))
    ) THEN RAISE EXCEPTION 'invalid winner transition'; END IF;
    IF NEW.state <> 'reserve' AND
       NOT EXISTS (SELECT 1 FROM draws WHERE id=NEW.draw_id AND status IN ('approved','completed')) THEN
        RAISE EXCEPTION 'winner action requires approved draw';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER winner_transition BEFORE UPDATE ON winners FOR EACH ROW EXECUTE FUNCTION enforce_winner_transition();
ALTER TABLE winners ADD CONSTRAINT pending_no_timer CHECK (state <> 'pending_notification' OR (deadline IS NULL AND notified_at IS NULL));
ALTER TABLE winners ADD CONSTRAINT notified_timer CHECK (state NOT IN ('notified','claimed','expired') OR (notified_at IS NOT NULL AND deadline IS NOT NULL AND deadline > notified_at));
ALTER TABLE winners ADD CONSTRAINT reserve_role CHECK (state <> 'reserve' OR role = 'reserve');

-- +goose Down
ALTER TABLE winners DROP CONSTRAINT reserve_role;
ALTER TABLE winners DROP CONSTRAINT notified_timer;
ALTER TABLE winners DROP CONSTRAINT pending_no_timer;
ALTER TABLE winners DROP CONSTRAINT winner_prize_assignment;
UPDATE winners SET prize='Приз освободившегося места',prize_quantity=1 WHERE prize IS NULL;
ALTER TABLE winners ALTER COLUMN prize SET NOT NULL;
ALTER TABLE winners ALTER COLUMN prize_quantity SET NOT NULL;
DROP TRIGGER winner_transition ON winners;
DROP FUNCTION enforce_winner_transition();
DROP TRIGGER draw_transition ON draws;
DROP FUNCTION enforce_draw_transition();
