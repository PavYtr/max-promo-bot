-- name: GetPartnerByDay :one
SELECT * FROM partners WHERE festival_day = $1;
