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
)

const maxBodyBytes = 4 << 20

type route struct {
	provider      provider.Provider
	providerModel string
}

type gateway struct {
	routes  map[string]route
	clients []config.Client
	cache   *responseCache
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
	g := &gateway{
		routes:  map[string]route{},
		clients: cfg.Clients,
		cache:   newResponseCache(cfg.Redis.Addr, cfg.Cache.TTL),
	}
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
	if !g.authorized(r) {
		writeError(w, &apiError{http.StatusUnauthorized, errAuthentication, "invalid or missing API key"})
		return
	}
	p, aerr := g.prepare(w, r)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	rt, body, stream := p.rt, p.body, p.stream
	if p.cacheKey != "" {
		if cached, hit := g.cache.get(r.Context(), p.cacheKey); hit {
			if stream {
				replayStream(w, r, cached)
				return
			}
			w.Header().Set(cacheHeader, cacheHit)
			writeJSON(w, r, http.StatusOK, cached)
			return
		}
		w.Header().Set(cacheHeader, cacheMiss)
	} else {
		w.Header().Set(cacheHeader, cacheBypass)
	}
	if stream {
		g.streamCompletion(w, r, p)
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
		if p.cacheKey != "" {
			g.cache.set(r.Context(), p.cacheKey, resp.Body)
		}
		writeJSON(w, r, http.StatusOK, resp.Body)
	}
}

// streamCompletion отдаёт клиенту поток SSE от провайдера по мере поступления. Отмена запроса
// клиентом (r.Context()) прерывает запрос к провайдеру.
func (g *gateway) streamCompletion(w http.ResponseWriter, r *http.Request, p prepared) {
	st, err := p.rt.provider.Stream(r.Context(), p.body)
	if err != nil {
		writeError(w, &apiError{http.StatusBadGateway, errAPI, "provider request failed"})
		return
	}
	defer func() {
		if cerr := st.Body.Close(); cerr != nil {
			slog.WarnContext(r.Context(), "close provider stream", "error", cerr)
		}
	}()
	if st.Status != http.StatusOK {
		data, rerr := io.ReadAll(st.Body)
		if rerr != nil || st.Status >= 500 || st.Status < 200 {
			writeError(w, &apiError{http.StatusBadGateway, errAPI, "provider request failed"})
			return
		}
		writeJSON(w, r, st.Status, data)
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
	var asm *streamAssembler
	if p.cacheKey != "" {
		asm = &streamAssembler{}
	}
	buf := make([]byte, 4096)
	for {
		n, rerr := st.Body.Read(buf)
		if n > 0 {
			if asm != nil {
				asm.feed(buf[:n])
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				slog.WarnContext(r.Context(), "write stream", "error", werr)
				return
			}
			flusher.Flush()
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) && asm != nil {
				if completion, ok := asm.completion(); ok {
					g.cache.set(r.Context(), p.cacheKey, completion)
				}
			}
			if !errors.Is(rerr, io.EOF) && r.Context().Err() == nil {
				slog.WarnContext(r.Context(), "read provider stream", "error", rerr)
				writeStreamError(w, flusher, r)
			}
			return
		}
	}
}

// prepared — проверенный запрос: маршрут, тело для провайдера, признак потока и ключ кэша
// (пустой, если запрос обходит кэш).
type prepared struct {
	rt       route
	body     []byte
	stream   bool
	cacheKey string
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
	if err := json.Unmarshal(req["model"], &model); err != nil {
		return prepared{}, bad("model must be a string")
	}
	rt, found := g.routes[model]
	if !found {
		return prepared{}, &apiError{http.StatusNotFound, errNotFound, fmt.Sprintf("model %q not found", model)}
	}
	key := cacheKey(req, model)
	req["model"], err = json.Marshal(rt.providerModel)
	if err != nil {
		return prepared{}, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return prepared{}, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	return prepared{rt: rt, body: body, stream: stream, cacheKey: key}, nil
}

func isTrue(raw json.RawMessage) bool {
	var b bool
	return json.Unmarshal(raw, &b) == nil && b
}

func (g *gateway) authorized(r *http.Request) bool {
	key, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || key == "" {
		return false
	}
	// Цикл намеренно не прерывается на первом совпадении: время ответа не должно
	// зависеть от того, какой по счёту ключ подошёл.
	match := false
	for _, c := range g.clients {
		if subtle.ConstantTimeCompare([]byte(key), []byte(c.Key)) == 1 {
			match = true
		}
	}
	return match
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
