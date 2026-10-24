// Package httpapi содержит HTTP-слой гейтвея: обработчики, разбор запроса и формат ошибок.
// Бизнес-правила (кэш, лимиты, маршрутизация, провайдеры) живут в соседних пакетах.
package httpapi

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/PKSonlem/incident-copilot/app/internal/auth"
	"github.com/PKSonlem/incident-copilot/app/internal/cache"
	"github.com/PKSonlem/incident-copilot/app/internal/ratelimit"
	"github.com/PKSonlem/incident-copilot/app/internal/routing"
)

type gateway struct {
	auth    *auth.Authenticator
	router  *routing.Router
	cache   *cache.Cache
	limiter *ratelimit.Limiter
}

func (g *gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /live", respondOK)
	mux.HandleFunc("GET /ready", respondOK)
	mux.HandleFunc("POST /v1/chat/completions", g.chatCompletions)
	return mux
}

func respondOK(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (g *gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	client, found := g.auth.Authenticate(r.Header.Get("Authorization"))
	if !found {
		writeError(w, &apiError{http.StatusUnauthorized, errAuthentication, "invalid or missing API key"})
		return
	}
	allowed, retryAfter, err := g.limiter.Allow(r.Context(), client.Name, client.RateLimit.Requests, client.RateLimit.Window)
	switch {
	case err != nil:
		slog.ErrorContext(r.Context(), "rate limiter", "error", err)
		writeError(w, &apiError{http.StatusServiceUnavailable, errAPI, "rate limiter is unavailable"})
		return
	case !allowed:
		w.Header().Set("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(retryAfter)))
		writeError(w, &apiError{http.StatusTooManyRequests, errRateLimit, "rate limit exceeded"})
		return
	}
	prep, aerr := g.prepare(w, r)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	if g.serveFromCache(w, r, prep.cache, prep.stream) {
		return
	}
	if prep.stream {
		g.streamCompletion(w, r, prep)
		return
	}
	resp, err := prep.route.Provider.Complete(r.Context(), prep.body)
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
		g.cache.Store(r.Context(), prep.cache, resp.Body)
		writeJSON(w, r, http.StatusOK, resp.Body)
	}
}
