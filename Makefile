.PHONY: up down infra run build migrate migrate-down generate test test-race vet integration check
up:
	docker compose up --build -d --wait

down:
	docker compose down

infra:
	docker compose up -d --wait postgres redis

run:
	go run ./cmd/bot serve

build:
	go build -o bin/bot ./cmd/bot

migrate:
	go run ./cmd/bot migrate up

migrate-down:
	go run ./cmd/bot migrate down

generate:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.29.0 generate

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

integration:
	TEST_DATABASE_URL='postgres://festival:festival@localhost:15432/festival?sslmode=disable' TEST_REDIS_URL='redis://localhost:16379/15' go test -race -tags=integration ./...

check: test-race vet
