package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// responseCache — точный кэш ответов в Redis. Хранит готовый JSON chat.completion: из одной
// записи отдаются и обычный, и потоковый ответ. Сбой Redis не ломает запрос: это промах.
type responseCache struct {
	rdb *redis.Client
	ttl time.Duration
}

func newResponseCache(addr string, ttl time.Duration) *responseCache {
	return &responseCache{rdb: redis.NewClient(&redis.Options{Addr: addr}), ttl: ttl}
}

// serve отвечает из кэша, если запись есть, и сообщает об этом. Иначе выставляет X-Cache
// по плану и возвращает false: дальше запрос идёт к провайдеру.
// TODO(#13): при повторе из кэша отдавать usage и учитывать его.
func (c *responseCache) serve(w http.ResponseWriter, r *http.Request, plan cachePlan, stream bool) bool {
	if plan.status == cacheMiss {
		if completion, ok := c.get(r.Context(), plan.key); ok {
			w.Header().Set(cacheHeader, string(cacheHit))
			if stream {
				replayStream(w, r, completion)
			} else {
				writeJSON(w, r, http.StatusOK, completion)
			}
			return true
		}
	}
	w.Header().Set(cacheHeader, string(plan.status))
	return false
}

// store сохраняет ответ провайдера, если запрос кэшируем.
func (c *responseCache) store(ctx context.Context, plan cachePlan, completion []byte) {
	if plan.status == cacheMiss {
		c.set(ctx, plan.key, completion)
	}
}

func (c *responseCache) get(ctx context.Context, key string) ([]byte, bool) {
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			slog.WarnContext(ctx, "cache get", "error", err)
		}
		return nil, false
	}
	return data, true
}

func (c *responseCache) set(ctx context.Context, key string, data []byte) {
	if err := c.rdb.Set(ctx, key, data, c.ttl).Err(); err != nil {
		slog.WarnContext(ctx, "cache set", "error", err)
	}
}
