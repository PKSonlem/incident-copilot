// Package ratelimit считает запросы клиентов в фиксированном окне поверх Redis.
package ratelimit

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// Счётчик фиксированного окна: окно открывается первым запросом и живёт Window. Всё выполняется
// одним скриптом, поэтому несколько экземпляров гейтвея не расходятся в счёте.
var windowCounter = redis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return {count, redis.call('PTTL', KEYS[1])}
`)

// Limiter хранит счётчики запросов по ключам клиентов в Redis.
type Limiter struct {
	rdb *redis.Client
}

// New создаёт лимитер поверх Redis по адресу addr.
func New(addr string) *Limiter {
	return &Limiter{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

// Allow учитывает запрос клиента name: не больше limit запросов за window. limit == 0 отключает
// ограничение. Если лимит исчерпан, возвращает через сколько повторить.
func (l *Limiter) Allow(ctx context.Context, name string, limit int, window time.Duration) (ok bool, retryAfter time.Duration, err error) {
	if limit == 0 {
		return true, 0, nil
	}
	res, err := windowCounter.Run(ctx, l.rdb, []string{"ratelimit:" + name}, window.Milliseconds()).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("rate limit counter: %w", err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("rate limit counter: unexpected script result %v", res)
	}
	if res[0] <= int64(limit) {
		return true, 0, nil
	}
	return false, time.Duration(res[1]) * time.Millisecond, nil
}

// RetryAfterSeconds округляет вверх до целых секунд, не меньше одной.
func RetryAfterSeconds(d time.Duration) int {
	return max(1, int(math.Ceil(d.Seconds())))
}
