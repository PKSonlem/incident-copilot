// Package server содержит HTTP-обработчики сервиса.
package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/PKSonlem/incident-copilot/app/internal/chatwire"
	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/provider"
)

// Типы ошибок OpenAI-формата.
const (
	errInvalidRequest = "invalid_request_error"
	errAuthentication = "authentication_error"
	errNotFound       = "not_found_error"
	errAPI            = "api_error"
	errRateLimit      = "rate_limit_error"
)

const maxBodyBytes = 4 << 20

type route struct {
	name          string
	kind          string
	provider      provider.Provider
	providerModel string
}

// Значения заголовка X-Complexity и алиас модели, включающий маршрутизацию.
const (
	complexityHeader  = "X-Complexity"
	complexitySimple  = "simple"
	complexityComplex = "complex"
	modelAuto         = "auto"
)

type gateway struct {
	routes map[string]route
	// simpleRoute и complexRoute — первые модели конфига с провайдером kind=openai и kind=claude.
	simpleRoute, complexRoute *route
	clients                   []config.Client
	cache                     *responseCache
	limiter                   *rateLimiter
}

// New возвращает обработчик гейтвея: live, ready и /v1/chat/completions.
func New(cfg config.Config) (http.Handler, error) {
	providers := map[string]provider.Provider{}
	kinds := map[string]string{}
	for _, pc := range cfg.Providers {
		p, err := provider.New(pc.Kind, pc.BaseURL, pc.APIKey, provider.Options{DefaultMaxTokens: pc.EffectiveMaxTokens()})
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", pc.Name, err)
		}
		providers[pc.Name] = p
		kinds[pc.Name] = pc.Kind
	}
	g := &gateway{
		routes:  map[string]route{},
		clients: cfg.Clients,
		cache:   newResponseCache(cfg.Redis.Addr, cfg.Cache.EffectiveTTL()),
		limiter: newRateLimiter(cfg.Redis.Addr),
	}
	for _, m := range cfg.Models {
		p, found := providers[m.Provider]
		if !found {
			return nil, fmt.Errorf("model %q: unknown provider %q", m.Name, m.Provider)
		}
		rt := route{name: m.Name, kind: kinds[m.Provider], provider: p, providerModel: m.ProviderModel}
		g.routes[m.Name] = rt
		switch {
		case rt.kind == provider.KindOpenAI && g.simpleRoute == nil:
			g.simpleRoute = &rt
		case rt.kind == provider.KindClaude && g.complexRoute == nil:
			g.complexRoute = &rt
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /live", respondOK)
	mux.HandleFunc("GET /ready", respondOK)
	mux.HandleFunc("POST /v1/chat/completions", g.chatCompletions)
	return mux, nil
}

func respondOK(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// apiError — ошибка, которую обработчик превращает в ответ OpenAI-формата.
type apiError struct {
	status int
	typ    string
	msg    string
}

func (e *apiError) Error() string { return e.msg }

func (g *gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	client, found := g.authorize(r)
	if !found {
		writeError(w, &apiError{http.StatusUnauthorized, errAuthentication, "invalid or missing API key"})
		return
	}
	allowed, retryAfter, err := g.limiter.allow(r.Context(), client)
	switch {
	case err != nil:
		slog.ErrorContext(r.Context(), "rate limiter", "error", err)
		writeError(w, &apiError{http.StatusServiceUnavailable, errAPI, "rate limiter is unavailable"})
		return
	case !allowed:
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(retryAfter)))
		writeError(w, &apiError{http.StatusTooManyRequests, errRateLimit, "rate limit exceeded"})
		return
	}
	prep, aerr := g.prepare(w, r)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	if g.cache.serve(w, r, prep.cache, prep.stream) {
		return
	}
	if prep.stream {
		g.streamCompletion(w, r, prep)
		return
	}
	resp, err := prep.route.provider.Complete(r.Context(), prep.body)
	switch {
	case err != nil:
		writeError(w, providerFailure(err))
	case resp.Status >= 500 || resp.Status < 200:
		// TODO(#9): различать причины сбоя провайдера для fallback.
		writeError(w, &apiError{http.StatusBadGateway, errAPI, "provider request failed"})
	case resp.Status >= 400:
		// Ошибки клиента (включая 429) провайдер уже оформил в OpenAI-формате: отдаём как есть.
		writeJSON(w, r, resp.Status, resp.Body)
	default:
		g.cache.store(r.Context(), prep.cache, resp.Body)
		writeJSON(w, r, http.StatusOK, resp.Body)
	}
}

// providerFailure превращает ошибку вызова провайдера в ответ: запрос, который провайдер
// не поддерживает, это 400, всё остальное 502.
func providerFailure(err error) *apiError {
	var clientErr *provider.ClientError
	if errors.As(err, &clientErr) {
		return &apiError{http.StatusBadRequest, errInvalidRequest, clientErr.Message}
	}
	return &apiError{http.StatusBadGateway, errAPI, "provider request failed"}
}

// streamCompletion отдаёт клиенту поток SSE от провайдера по мере поступления. Отмена запроса
// клиентом (r.Context()) прерывает запрос к провайдеру.
func (g *gateway) streamCompletion(w http.ResponseWriter, r *http.Request, prep prepared) {
	upstream, err := prep.route.provider.Stream(r.Context(), prep.body)
	if err != nil {
		writeError(w, providerFailure(err))
		return
	}
	defer func() {
		if cerr := upstream.Body.Close(); cerr != nil {
			slog.WarnContext(r.Context(), "close provider stream", "error", cerr)
		}
	}()
	if upstream.Status != http.StatusOK {
		data, rerr := io.ReadAll(upstream.Body)
		if rerr != nil || upstream.Status >= 500 || upstream.Status < 200 {
			writeError(w, &apiError{http.StatusBadGateway, errAPI, "provider request failed"})
			return
		}
		writeJSON(w, r, upstream.Status, data)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, &apiError{http.StatusInternalServerError, errAPI, "streaming is not supported by the server"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	capture := g.cache.capture(prep.cache)
	buf := make([]byte, 4096)
	for {
		n, rerr := upstream.Body.Read(buf)
		if n > 0 {
			capture.feed(buf[:n])
			if _, werr := w.Write(buf[:n]); werr != nil {
				slog.WarnContext(r.Context(), "write stream", "error", werr)
				return
			}
			flusher.Flush()
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				capture.commit(r.Context())
			} else if r.Context().Err() == nil {
				slog.WarnContext(r.Context(), "read provider stream", "error", rerr)
				writeStreamError(w, flusher, r)
			}
			return
		}
	}
}

// prepared — проверенный запрос: маршрут, тело для провайдера, признак потока и решение кэша.
type prepared struct {
	route  route
	body   []byte
	stream bool
	cache  cachePlan
}

// prepare читает и проверяет запрос.
func (g *gateway) prepare(w http.ResponseWriter, r *http.Request) (prepared, *apiError) {
	bad := func(msg string) *apiError { return &apiError{http.StatusBadRequest, errInvalidRequest, msg} }

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return prepared{}, bad("cannot read request body")
	}
	var req map[string]json.RawMessage
	if err := json.Unmarshal(raw, &req); err != nil {
		return prepared{}, bad("request body is not valid JSON")
	}
	stream := isTrue(req["stream"])
	if err := checkContent(req["messages"]); err != nil {
		return prepared{}, bad(err.Error())
	}
	var model string
	if raw, present := req["model"]; present {
		if err := json.Unmarshal(raw, &model); err != nil {
			return prepared{}, bad("model must be a string")
		}
	}
	complexity := r.Header.Get(complexityHeader)
	if complexity != "" && complexity != complexitySimple && complexity != complexityComplex {
		return prepared{}, bad(fmt.Sprintf("%s must be %q or %q", complexityHeader, complexitySimple, complexityComplex))
	}
	rt, aerr := g.resolve(model, complexity)
	if aerr != nil {
		return prepared{}, aerr
	}
	model = rt.name
	plan := planCache(req, model)
	req["model"], err = json.Marshal(rt.providerModel)
	if err != nil {
		return prepared{}, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return prepared{}, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	return prepared{route: rt, body: body, stream: stream, cache: plan}, nil
}

// resolve выбирает маршрут: конкретное имя модели закрепляет запрос, "auto" и пустое имя
// включают маршрутизацию по сложности (без заголовка запрос сложный).
func (g *gateway) resolve(model, complexity string) (route, *apiError) {
	if model != "" && model != modelAuto {
		rt, found := g.routes[model]
		if !found {
			return route{}, &apiError{http.StatusNotFound, errNotFound, fmt.Sprintf("model %q not found", model)}
		}
		return rt, nil
	}
	target := g.complexRoute
	if complexity == complexitySimple {
		target = g.simpleRoute
	}
	if target == nil {
		return route{}, &apiError{http.StatusInternalServerError, errAPI, "no model is configured for routing"}
	}
	return *target, nil
}

func isTrue(raw json.RawMessage) bool {
	var b bool
	return json.Unmarshal(raw, &b) == nil && b
}

// authorize возвращает клиента, чей ключ передан в заголовке Authorization.
func (g *gateway) authorize(r *http.Request) (config.Client, bool) {
	key, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || key == "" {
		return config.Client{}, false
	}
	// Цикл намеренно не прерывается на первом совпадении: время ответа не должно
	// зависеть от того, какой по счёту ключ подошёл.
	var match config.Client
	matched := false
	for _, c := range g.clients {
		if subtle.ConstantTimeCompare([]byte(key), []byte(c.Key)) == 1 {
			match, matched = c, true
		}
	}
	return match, matched
}

// checkContent допускает content в виде строки, null или массива текстовых частей.
// Всё остальное (изображения, аудио, объекты, числа) отвергается.
func checkContent(rawMessages json.RawMessage) error {
	var messages []struct {
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(rawMessages, &messages); err != nil {
		return errors.New("messages must be an array")
	}
	for _, m := range messages {
		trimmed := strings.TrimSpace(string(m.Content))
		switch {
		case chatwire.IsNullOrEmpty(m.Content) || strings.HasPrefix(trimmed, `"`):
			continue
		case strings.HasPrefix(trimmed, "["):
			var parts []struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(m.Content, &parts); err != nil {
				return errors.New("message content parts are malformed")
			}
			for _, part := range parts {
				if part.Type != "text" {
					return fmt.Errorf("unsupported content type %q: only text is supported", part.Type)
				}
			}
		default:
			return errors.New("message content must be a string or an array of text parts")
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		slog.WarnContext(r.Context(), "write response", "error", err)
	}
}

func writeError(w http.ResponseWriter, e *apiError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	body := map[string]any{"error": map[string]any{"message": e.msg, "type": e.typ, "param": nil, "code": nil}}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Warn("write error response", "error", err)
	}
}

// writeStreamError завершает поток SSE-событием с ошибкой: статус ответа уже отправлен, повтор невозможен.
func writeStreamError(w http.ResponseWriter, flusher http.Flusher, r *http.Request) {
	event, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": "provider stream failed", "type": errAPI, "param": nil, "code": nil,
	}})
	if err != nil {
		slog.ErrorContext(r.Context(), "encode stream error", "error", err)
		return
	}
	if _, err := w.Write(append(append([]byte("data: "), event...), '\n', '\n')); err != nil {
		slog.WarnContext(r.Context(), "write stream error", "error", err)
		return
	}
	flusher.Flush()
}
