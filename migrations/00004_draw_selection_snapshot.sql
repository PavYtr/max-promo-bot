-- +goose Up
ALTER TABLE draws ADD COLUMN selection_snapshot jsonb CHECK (jsonb_typeof(selection_snapshot)='array');
UPDATE draws d SET selection_snapshot=(SELECT jsonb_agg(jsonb_build_object(
 'winner_id',id,'user_id',user_id,'role',role,'reserve_order',reserve_order,
 'prize',prize,'prize_quantity',prize_quantity) ORDER BY id) FROM winners WHERE draw_id=d.id);
-- +goose StatementBegin
CREATE FUNCTION protect_draw_selection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.selection_snapshot IS NOT NULL AND NEW.selection_snapshot IS DISTINCT FROM OLD.selection_snapshot THEN
        RAISE EXCEPTION 'draw selection is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER draw_selection_immutable BEFORE UPDATE ON draws FOR EACH ROW EXECUTE FUNCTION protect_draw_selection();
-- +goose StatementBegin
CREATE FUNCTION protect_sealed_participants() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM draws WHERE id=NEW.draw_id AND selection_snapshot IS NOT NULL) THEN
        RAISE EXCEPTION 'draw participants are sealed';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER draw_participants_sealed BEFORE INSERT ON draw_participants FOR EACH ROW EXECUTE FUNCTION protect_sealed_participants();

-- +goose Down
DROP TRIGGER draw_participants_sealed ON draw_participants;
DROP FUNCTION protect_sealed_participants();
DROP TRIGGER draw_selection_immutable ON draws;
DROP FUNCTION protect_draw_selection();
ALTER TABLE draws DROP COLUMN selection_snapshot;
