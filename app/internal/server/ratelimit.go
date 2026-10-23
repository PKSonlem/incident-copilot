package server

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
)

// Счётчик фиксированного окна: окно открывается первым запросом и живёт Window. Всё выполняется
// одним скриптом, поэтому несколько экземпляров гейтвея не расходятся в счёте.
var windowCounter = redis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return {count, redis.call('PTTL', KEYS[1])}
`)

// rateLimiter хранит счётчики запросов по ключам клиентов в Redis.
type rateLimiter struct {
	rdb *redis.Client
}

func newRateLimiter(addr string) *rateLimiter {
	return &rateLimiter{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

// allow учитывает запрос клиента. Если лимит исчерпан, возвращает через сколько повторить.
func (l *rateLimiter) allow(ctx context.Context, c config.Client) (ok bool, retryAfter time.Duration, err error) {
	rl := c.RateLimit
	if rl.Requests == 0 {
		return true, 0, nil
	}
	res, err := windowCounter.Run(ctx, l.rdb, []string{"ratelimit:" + c.Name}, rl.Window.Milliseconds()).Int64Slice()
	if err != nil || len(res) != 2 {
		return false, 0, fmt.Errorf("rate limit counter: %w", err)
	}
	if res[0] <= int64(rl.Requests) {
		return true, 0, nil
	}
	return false, time.Duration(res[1]) * time.Millisecond, nil
}

// retryAfterSeconds округляет вверх до целых секунд, не меньше одной.
func retryAfterSeconds(d time.Duration) int {
	return max(1, int(math.Ceil(d.Seconds())))
}
