FROM golang:1.26.1-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /bot ./cmd/bot

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /bot /app/bot
COPY migrations /app/migrations
COPY certs /app/certs
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/bot"]
CMD ["serve"]
