-- name: UpsertUser :exec
INSERT INTO users(id, first_name, last_name, username, first_started_at)
VALUES ($1, $2, $3, $4, COALESCE(sqlc.narg(first_started_at)::timestamptz, now()))
ON CONFLICT (id) DO NOTHING;

-- name: GetUser :one
SELECT * FROM users WHERE id = $1;
