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
	provider      provider.Provider
	providerModel string
}

type gateway struct {
	routes  map[string]route
	clients []config.Client
	limiter *rateLimiter
}

// New возвращает обработчик гейтвея: live, ready и /v1/chat/completions.
func New(cfg config.Config) (http.Handler, error) {
	providers := map[string]provider.Provider{}
	for _, pc := range cfg.Providers {
		p, err := provider.New(pc.Kind, pc.BaseURL, pc.APIKey)
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", pc.Name, err)
		}
		providers[pc.Name] = p
	}
	g := &gateway{routes: map[string]route{}, clients: cfg.Clients, limiter: newRateLimiter(cfg.Redis.Addr)}
	for _, m := range cfg.Models {
		p, found := providers[m.Provider]
		if !found {
			return nil, fmt.Errorf("model %q: unknown provider %q", m.Name, m.Provider)
		}
		g.routes[m.Name] = route{provider: p, providerModel: m.ProviderModel}
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
	rt, body, aerr := g.prepare(w, r)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	resp, err := rt.provider.Complete(r.Context(), body)
	switch {
	case err != nil || resp.Status >= 500 || resp.Status < 200:
		writeError(w, &apiError{http.StatusBadGateway, errAPI, "provider request failed"})
	case resp.Status >= 400:
		// Ошибки клиента (включая 429) провайдер уже оформил в OpenAI-формате: отдаём как есть.
		writeJSON(w, r, resp.Status, resp.Body)
	default:
		writeJSON(w, r, http.StatusOK, resp.Body)
	}
}

// prepare читает и проверяет запрос и возвращает маршрут и тело для провайдера.
func (g *gateway) prepare(w http.ResponseWriter, r *http.Request) (route, []byte, *apiError) {
	bad := func(msg string) *apiError { return &apiError{http.StatusBadRequest, errInvalidRequest, msg} }

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return route{}, nil, bad("cannot read request body")
	}
	var req map[string]json.RawMessage
	if err := json.Unmarshal(raw, &req); err != nil {
		return route{}, nil, bad("request body is not valid JSON")
	}
	if isTrue(req["stream"]) {
		return route{}, nil, bad("streaming is not supported yet")
	}
	if err := checkContent(req["messages"]); err != nil {
		return route{}, nil, bad(err.Error())
	}
	var model string
	if err := json.Unmarshal(req["model"], &model); err != nil {
		return route{}, nil, bad("model must be a string")
	}
	rt, found := g.routes[model]
	if !found {
		return route{}, nil, &apiError{http.StatusNotFound, errNotFound, fmt.Sprintf("model %q not found", model)}
	}
	req["model"], err = json.Marshal(rt.providerModel)
	if err != nil {
		return route{}, nil, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return route{}, nil, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	return rt, body, nil
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
		case trimmed == "" || trimmed == "null" || strings.HasPrefix(trimmed, `"`):
			continue
		case strings.HasPrefix(trimmed, "["):
			var parts []struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(m.Content, &parts); err != nil {
				return errors.New("message content parts are malformed")
			}
			for _, p := range parts {
				if p.Type != "text" {
					return fmt.Errorf("unsupported content type %q: only text is supported", p.Type)
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
