package cache

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache — точный кэш ответов в Redis. Хранит готовый JSON chat.completion: из одной записи
// отдаются и обычный, и потоковый ответ. Сбой Redis не ломает запрос: это промах.
type Cache struct {
	rdb *redis.Client
	ttl time.Duration
}

// New создаёт кэш поверх Redis по адресу addr.
func New(addr string, ttl time.Duration) *Cache {
	return &Cache{rdb: redis.NewClient(&redis.Options{Addr: addr}), ttl: ttl}
}

// Lookup возвращает сохранённый ответ, если запрос кэшируем и запись есть.
func (c *Cache) Lookup(ctx context.Context, plan Plan) ([]byte, bool) {
	if plan.Status != Miss {
		return nil, false
	}
	data, err := c.rdb.Get(ctx, plan.key).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			slog.WarnContext(ctx, "cache get", "error", err)
		}
		return nil, false
	}
	return data, true
}

// Store сохраняет ответ провайдера, если запрос кэшируем.
func (c *Cache) Store(ctx context.Context, plan Plan, completion []byte) {
	if plan.Status != Miss {
		return
	}
	if err := c.rdb.Set(ctx, plan.key, completion, c.ttl).Err(); err != nil {
		slog.WarnContext(ctx, "cache set", "error", err)
	}
}
