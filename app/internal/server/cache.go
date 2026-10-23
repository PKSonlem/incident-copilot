package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Заголовок ответа с состоянием кэша: HIT, MISS или BYPASS.
const (
	cacheHeader = "X-Cache"
	cacheHit    = "HIT"
	cacheMiss   = "MISS"
	cacheBypass = "BYPASS"

	defaultCacheTTL = time.Hour
	cacheKeyPrefix  = "cache:v1:"
)

// responseCache — точный кэш ответов в Redis. Хранит готовый JSON chat.completion: из одной
// записи отдаются и обычный, и потоковый ответ. Сбой Redis не ломает запрос: это промах.
type responseCache struct {
	rdb *redis.Client
	ttl time.Duration
}

func newResponseCache(addr string, ttl time.Duration) *responseCache {
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	return &responseCache{rdb: redis.NewClient(&redis.Options{Addr: addr}), ttl: ttl}
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

// cacheKey строит ключ из модели, параметров и сообщений. Поля stream и stream_options в ключ
// не входят. Пустая строка означает, что запрос обходит кэш: кэшируются только запросы с явным
// temperature=0 и одним вариантом ответа.
func cacheKey(req map[string]json.RawMessage, logicalModel string) string {
	var temp float64
	if err := json.Unmarshal(req["temperature"], &temp); err != nil || temp != 0 {
		return ""
	}
	if raw, ok := req["n"]; ok {
		var n int
		if err := json.Unmarshal(raw, &n); err != nil || n != 1 {
			return ""
		}
	}
	keyed := make(map[string]json.RawMessage, len(req))
	for k, v := range req {
		if k != "stream" && k != "stream_options" {
			keyed[k] = v
		}
	}
	model, err := json.Marshal(logicalModel)
	if err != nil {
		return ""
	}
	keyed["model"] = model
	canonical, err := json.Marshal(keyed) // ключи map сортируются: порядок полей не влияет
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(canonical)
	return cacheKeyPrefix + hex.EncodeToString(sum[:])
}

// streamAssembler собирает из SSE-потока провайдера полный JSON chat.completion для кэша.
type streamAssembler struct {
	buf          []byte
	done         bool
	broken       bool
	id, model    string
	created      json.RawMessage
	role         string
	content      strings.Builder
	finishReason any
	usage        json.RawMessage
	toolCalls    map[int]*toolCall
}

type toolCall struct {
	ID, Type, Name string
	Args           strings.Builder
}

type streamChunk struct {
	ID      string          `json:"id"`
	Model   string          `json:"model"`
	Created json.RawMessage `json:"created"`
	Usage   json.RawMessage `json:"usage"`
	Choices []struct {
		Delta struct {
			Role      string  `json:"role"`
			Content   *string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason any `json:"finish_reason"`
	} `json:"choices"`
	Error json.RawMessage `json:"error"`
}

func (a *streamAssembler) feed(p []byte) {
	a.buf = append(a.buf, p...)
	for {
		i := bytes.IndexByte(a.buf, '\n')
		if i < 0 {
			return
		}
		line := strings.TrimSpace(string(a.buf[:i]))
		a.buf = a.buf[i+1:]
		if payload, ok := strings.CutPrefix(line, "data:"); ok {
			a.event(strings.TrimSpace(payload))
		}
	}
}

func (a *streamAssembler) event(payload string) {
	if payload == "[DONE]" {
		a.done = true
		return
	}
	var ch streamChunk
	if err := json.Unmarshal([]byte(payload), &ch); err != nil || len(ch.Error) > 0 {
		a.broken = true
		return
	}
	if ch.ID != "" {
		a.id, a.model, a.created = ch.ID, ch.Model, ch.Created
	}
	if len(ch.Usage) > 0 && string(ch.Usage) != "null" {
		a.usage = ch.Usage
	}
	for _, c := range ch.Choices {
		if c.Delta.Role != "" {
			a.role = c.Delta.Role
		}
		if c.Delta.Content != nil {
			a.content.WriteString(*c.Delta.Content)
		}
		for _, tc := range c.Delta.ToolCalls {
			if a.toolCalls == nil {
				a.toolCalls = map[int]*toolCall{}
			}
			t := a.toolCalls[tc.Index]
			if t == nil {
				t = &toolCall{}
				a.toolCalls[tc.Index] = t
			}
			if tc.ID != "" {
				t.ID = tc.ID
			}
			if tc.Type != "" {
				t.Type = tc.Type
			}
			t.Name += tc.Function.Name
			t.Args.WriteString(tc.Function.Arguments)
		}
		if c.FinishReason != nil {
			a.finishReason = c.FinishReason
		}
	}
}

// completion возвращает собранный ответ; false, если поток не завершился корректно.
func (a *streamAssembler) completion() ([]byte, bool) {
	if !a.done || a.broken {
		return nil, false
	}
	role := a.role
	if role == "" {
		role = "assistant"
	}
	msg := map[string]any{"role": role, "content": a.content.String()}
	if len(a.toolCalls) > 0 {
		idx := make([]int, 0, len(a.toolCalls))
		for i := range a.toolCalls {
			idx = append(idx, i)
		}
		sort.Ints(idx)
		calls := make([]map[string]any, 0, len(idx))
		for _, i := range idx {
			t := a.toolCalls[i]
			typ := t.Type
			if typ == "" {
				typ = "function"
			}
			calls = append(calls, map[string]any{
				"id": t.ID, "type": typ,
				"function": map[string]any{"name": t.Name, "arguments": t.Args.String()},
			})
		}
		msg["tool_calls"] = calls
	}
	out := map[string]any{
		"id": a.id, "object": "chat.completion", "model": a.model, "created": a.created,
		"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": a.finishReason}},
	}
	if len(a.created) == 0 {
		delete(out, "created")
	}
	if len(a.usage) > 0 {
		out["usage"] = a.usage
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, false
	}
	return data, true
}

// replayStream отдаёт сохранённый chat.completion как поток SSE.
func replayStream(w http.ResponseWriter, r *http.Request, completion []byte) {
	var c struct {
		ID      string          `json:"id"`
		Model   string          `json:"model"`
		Created json.RawMessage `json:"created"`
		Choices []struct {
			Message      map[string]any `json:"message"`
			FinishReason any            `json:"finish_reason"`
		} `json:"choices"`
	}
	flusher, ok := w.(http.Flusher)
	if err := json.Unmarshal(completion, &c); err != nil || len(c.Choices) == 0 || !ok {
		writeError(w, &apiError{http.StatusInternalServerError, errAPI, "cannot replay cached response"})
		return
	}
	delta := map[string]any{}
	for k, v := range c.Choices[0].Message {
		delta[k] = v
	}
	if calls, ok := delta["tool_calls"].([]any); ok {
		for i, call := range calls {
			if m, ok := call.(map[string]any); ok {
				m["index"] = i
			}
		}
	}
	chunk := func(d map[string]any, finish any) []byte {
		ch := map[string]any{
			"id": c.ID, "object": "chat.completion.chunk", "model": c.Model,
			"choices": []any{map[string]any{"index": 0, "delta": d, "finish_reason": finish}},
		}
		if len(c.Created) > 0 {
			ch["created"] = c.Created
		}
		data, err := json.Marshal(ch)
		if err != nil {
			return nil
		}
		return []byte("data: " + string(data) + "\n\n")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set(cacheHeader, cacheHit)
	w.WriteHeader(http.StatusOK)
	for _, part := range [][]byte{
		chunk(delta, nil), chunk(map[string]any{}, c.Choices[0].FinishReason), []byte("data: [DONE]\n\n"),
	} {
		if part == nil {
			return
		}
		if _, err := w.Write(part); err != nil {
			slog.WarnContext(r.Context(), "write cached stream", "error", err)
			return
		}
	}
	flusher.Flush()
}
