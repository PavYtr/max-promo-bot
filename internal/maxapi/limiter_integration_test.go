//go:build integration

package maxapi

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestSharedLimitsOnSimulator(t *testing.T) {
	dsn := os.Getenv("TEST_REDIS_URL")
	if dsn == "" {
		t.Fatal("TEST_REDIS_URL required")
	}
	opts, err := redis.ParseURL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	r1 := redis.NewClient(opts)
	defer r1.Close()
	r2 := redis.NewClient(opts)
	defer r2.Close()
	token := fmt.Sprint(time.Now().UnixNano())
	l1, l2 := NewLimiter(r1, token), NewLimiter(r2, token)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var mu sync.Mutex
	var hits []time.Time
	byChat := map[string][]time.Time{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		now := time.Now()
		hits = append(hits, now)
		chat := r.URL.Query().Get("chat_id")
		byChat[chat] = append(byChat[chat], now)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"message":{}}`))
	}))
	defer srv.Close()
	c1, _ := New(srv.URL, token, "", l1)
	c2, _ := New(srv.URL, token, "", l2)
	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := c1
			if i%2 == 0 {
				c = c2
			}
			if e := c.Send(ctx, int64(i+1), "", Body{Text: "x"}); e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	// Two separate clients act as HTTP process and future worker, sharing the bot token.
	for i := range 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := c1
			if i%2 == 0 {
				c = c2
			}
			if e := c.Send(ctx, 999, "", Body{Text: "x"}); e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(hits, func(i, j int) bool { return hits[i].Before(hits[j]) })
	for i := range hits {
		j := i
		for j < len(hits) && hits[j].Sub(hits[i]) < 990*time.Millisecond {
			j++
		}
		if j-i > 25 {
			t.Fatalf("global limit exceeded: %d", j-i)
		}
	}
	times := byChat["999"]
	if len(times) != 3 {
		t.Fatal("missing requests")
	}
	for i := 1; i < len(times); i++ {
		if times[i].Sub(times[i-1]) < 990*time.Millisecond {
			t.Fatal("per-chat limit exceeded")
		}
	}
	if err = l1.Cooldown(ctx, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	short, cancelShort := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelShort()
	if err = l2.Wait(short, 0); err == nil {
		t.Fatal("429 cooldown not shared")
	}
	r3 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer r3.Close()
	if err = NewLimiter(r3, token).Wait(ctx, 0); err == nil {
		t.Fatal("limiter must fail closed when Redis is down")
	}
}
