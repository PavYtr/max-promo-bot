package main

import (
	"context"
	"database/sql"
	"errors"
	"example.com/max-festival-bot/internal/bot"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/jobs"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"example.com/max-festival-bot/internal/transport"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, log); err != nil {
		log.Error("process stopped", "reason", err.Error())
		os.Exit(1)
	}
}
func run(ctx context.Context, log *slog.Logger) error {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		addr := os.Getenv("HTTP_ADDR")
		if addr == "" {
			addr = ":8080"
		}
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return errors.New("invalid HTTP_ADDR")
		}
		client := &http.Client{Timeout: 2 * time.Second}
		res, err := client.Get("http://127.0.0.1:" + port + "/healthz")
		if err != nil {
			return errors.New("healthcheck failed")
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return errors.New("healthcheck failed")
		}
		return nil
	}
	c, err := config.Load()
	if err != nil {
		return err
	}
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	if command == "migrate" {
		db, err := sql.Open("pgx", c.DatabaseURL)
		if err != nil {
			return errors.New("open migration database")
		}
		defer db.Close()
		goose.SetLogger(goose.NopLogger())
		if err = goose.SetDialect("postgres"); err != nil {
			return errors.New("migration dialect")
		}
		direction := "up"
		if len(os.Args) > 2 {
			direction = os.Args[2]
		}
		if err = goose.RunContext(ctx, direction, db, "migrations"); err != nil {
			return errors.New("migration failed")
		}
		log.Info("migration completed")
		return nil
	}
	if command != "serve" && command != "import" && command != "subscribe" {
		return errors.New("usage: bot [serve|migrate [up|down|status]|import FILE|subscribe]")
	}
	if (command == "serve" || command == "subscribe") && c.Token == "" {
		return errors.New("MAX_TOKEN is required")
	}
	pool, err := pgxpool.New(ctx, c.DatabaseURL)
	if err != nil {
		return errors.New("database configuration failed")
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return errors.New("database unavailable")
	}
	if command == "import" {
		if len(os.Args) != 3 {
			return errors.New("usage: bot import FILE")
		}
		raw, e := os.ReadFile(os.Args[2])
		if e != nil {
			return errors.New("cannot read import file")
		}
		data, e := storage.ParseImport(raw, c.Location)
		if e != nil {
			return e
		}
		tx, e := pool.Begin(ctx)
		if e != nil {
			return errors.New("begin import")
		}
		defer tx.Rollback(ctx)
		if e = storage.ImportPartners(ctx, tx, data); e != nil {
			return e
		}
		if e = tx.Commit(ctx); e != nil {
			return errors.New("commit import")
		}
		log.Info("import completed")
		return nil
	}
	options, err := redis.ParseURL(c.RedisURL)
	if err != nil {
		return errors.New("invalid REDIS_URL")
	}
	rdb := redis.NewClient(options)
	defer rdb.Close()
	if err = rdb.Ping(ctx).Err(); err != nil {
		return errors.New("redis unavailable")
	}
	api, err := maxapi.New(c.APIURL, c.Token, c.CAFile, maxapi.NewLimiter(rdb, c.Token))
	if err != nil {
		return err
	}
	if command == "subscribe" {
		if c.Mode != "webhook" || c.WebhookURL == "" {
			return errors.New("subscribe requires BOT_MODE=webhook and WEBHOOK_URL")
		}
		if err = api.Subscribe(ctx, c.WebhookURL, c.WebhookSecret); err != nil {
			return errors.New("MAX subscription failed")
		}
		log.Info("webhook subscribed")
		return nil
	}
	store := storage.Store{Pool: pool}
	handler := bot.Handler{Config: c, Membership: api}
	appCtx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	start := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); fn() }() }
	queue := jobs.Queue{Client: asynq.NewClientFromRedisClient(rdb)}
	worker := asynq.NewServerFromRedisClient(rdb, asynq.Config{Concurrency: 4, Queues: map[string]int{"winners": 1}, BaseContext: func() context.Context { return appCtx }, ShutdownTimeout: 10 * time.Second})
	if err = worker.Start(jobs.Handler(store, api)); err != nil {
		return errors.New("notification worker failed to start")
	}
	defer worker.Shutdown()
	start(func() {
		for appCtx.Err() == nil {
			done, e := store.DispatchNotification(appCtx, queue)
			if e != nil && appCtx.Err() == nil {
				log.Warn("notification dispatch failed")
			}
			if !done || e != nil {
				_ = maxapi.Pause(appCtx, time.Second)
			}
		}
	})
	if c.Mode == "polling" {
		// A dedicated connection holds the session lock; two pollers would lose cursor progress.
		conn, e := pool.Acquire(ctx)
		if e != nil {
			return errors.New("poller lock connection")
		}
		defer conn.Release()
		var locked bool
		if e = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(725301)`).Scan(&locked); e != nil || !locked {
			return errors.New("another polling process is running")
		}
		defer func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(725301)`)
		}()
		start(func() { transport.Poll(appCtx, api, store, log) })
	}
	start(func() {
		for appCtx.Err() == nil {
			done, e := store.ProcessOne(appCtx, handler.Handle)
			if e != nil && appCtx.Err() == nil {
				log.Warn("event processing failed")
			}
			if !done || e != nil {
				_ = maxapi.Pause(appCtx, 200*time.Millisecond)
			}
		}
	})
	start(func() {
		for appCtx.Err() == nil {
			workCtx, cancel := context.WithTimeout(appCtx, 60*time.Second)
			done, e := store.DeliverOne(workCtx, api)
			cancel()
			if e != nil && appCtx.Err() == nil {
				log.Warn("outgoing delivery interrupted")
			}
			if !done || e != nil {
				_ = maxapi.Pause(appCtx, 200*time.Millisecond)
			}
		}
	})
	mux := http.NewServeMux()
	if c.Mode == "webhook" {
		mux.Handle("/webhook", transport.Webhook(c.WebhookSecret, store))
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		check, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if pool.Ping(check) != nil || rdb.Ping(check).Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: c.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	result := make(chan error, 1)
	go func() { result <- server.ListenAndServe() }()
	log.Info("bot started", "transport", c.Mode)
	if c.BannerURL == "" && c.BannerToken == "" {
		log.Warn("welcome banner is not configured")
	}
	select {
	case <-ctx.Done():
	case err = <-result:
	}
	stop()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	_ = server.Shutdown(shutdown)
	wg.Wait()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("HTTP server failed")
	}
	log.Info("bot stopped")
	return nil
}
