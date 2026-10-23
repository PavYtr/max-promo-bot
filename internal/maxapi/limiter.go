package maxapi

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

// Both allowances are consumed atomically, using the Redis clock across processes.
var reserve = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
local wait = 0
for i,key in ipairs(KEYS) do
  wait = math.max(wait, (tonumber(redis.call('GET',key)) or 0) - now)
end
if wait > 0 then return wait end
redis.call('SET',KEYS[1],now+40,'PX',60000)
if #KEYS == 3 then redis.call('SET',KEYS[3],now+1000,'PX',60000) end
return 0
`)
var cooldown = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
local untilAt = math.max(tonumber(redis.call('GET',KEYS[1])) or 0,now+tonumber(ARGV[1]))
redis.call('SET',KEYS[1],untilAt,'PX',math.ceil(untilAt-now))
return 1
`)

type Limiter struct {
	rdb    *redis.Client
	prefix string
}

func NewLimiter(rdb *redis.Client, token string) *Limiter {
	h := sha256.Sum256([]byte(token))
	return &Limiter{rdb: rdb, prefix: fmt.Sprintf("max:{%x}:", h[:8])}
}
func (l *Limiter) Wait(ctx context.Context, chat int64) error {
	keys := []string{l.prefix + "global", l.prefix + "cooldown"}
	if chat != 0 {
		keys = append(keys, l.prefix+"chat:"+strconv.FormatInt(chat, 10))
	}
	for {
		ms, err := reserve.Run(ctx, l.rdb, keys).Int64()
		if err != nil {
			return fmt.Errorf("rate limiter unavailable")
		}
		if ms <= 0 {
			return nil
		}
		if err = Pause(ctx, time.Duration(ms)*time.Millisecond); err != nil {
			return err
		}
	}
}
func (l *Limiter) Cooldown(ctx context.Context, d time.Duration) error {
	return cooldown.Run(ctx, l.rdb, []string{l.prefix + "cooldown"}, max(d.Milliseconds(), int64(1))).Err()
}
func Pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
