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

type route struct {
	provider      provider.Provider
	providerModel string
}

type gateway struct {
	routes  map[string]route
	clients []config.Client
}

// New возвращает обработчик гейтвея: live, ready и /v1/chat/completions.
func New(cfg config.Config) (http.Handler, error) {
	providers := map[string]provider.Provider{}
	for _, p := range cfg.Providers {
		if p.Kind != "openai" {
			return nil, fmt.Errorf("provider %q: unsupported kind %q", p.Name, p.Kind)
		}
		providers[p.Name] = provider.NewOpenAI(p.BaseURL, p.APIKey)
	}
	g := &gateway{routes: map[string]route{}, clients: cfg.Clients}
	for _, m := range cfg.Models {
		p, ok := providers[m.Provider]
		if !ok {
			return nil, fmt.Errorf("model %q: unknown provider %q", m.Name, m.Provider)
		}
		g.routes[m.Name] = route{provider: p, providerModel: m.ProviderModel}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /live", ok)
	mux.HandleFunc("GET /ready", ok)
	mux.HandleFunc("POST /v1/chat/completions", g.chatCompletions)
	return mux, nil
}

func ok(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (g *gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	if !g.authorized(r) {
		writeError(w, http.StatusUnauthorized, "authentication_error", "invalid or missing API key")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "cannot read request body")
		return
	}
	var req map[string]json.RawMessage
	if err := json.Unmarshal(raw, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "request body is not valid JSON")
		return
	}
	if err := checkContent(req["messages"]); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	var model string
	if err := json.Unmarshal(req["model"], &model); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "model must be a string")
		return
	}
	rt, found := g.routes[model]
	if !found {
		writeError(w, http.StatusNotFound, "not_found_error", fmt.Sprintf("model %q not found", model))
		return
	}
	req["model"], _ = json.Marshal(rt.providerModel) //nolint:errcheck // marshal строки не падает
	body, err := json.Marshal(req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "api_error", "cannot encode request")
		return
	}
	resp, err := rt.provider.Complete(r.Context(), body)
	if err != nil || resp.Status != http.StatusOK {
		writeError(w, http.StatusBadGateway, "api_error", "provider request failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(resp.Body); err != nil {
		slog.WarnContext(r.Context(), "write response", "error", err)
	}
}

func (g *gateway) authorized(r *http.Request) bool {
	key, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || key == "" {
		return false
	}
	match := false
	for _, c := range g.clients {
		if subtle.ConstantTimeCompare([]byte(key), []byte(c.Key)) == 1 {
			match = true
		}
	}
	return match
}

// checkContent пропускает строковый контент и текстовые части, остальное отвергает.
func checkContent(rawMessages json.RawMessage) error {
	var messages []struct {
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(rawMessages, &messages); err != nil {
		return errors.New("messages must be an array")
	}
	for _, m := range messages {
		var parts []struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(m.Content, &parts) != nil {
			continue // строка или null
		}
		for _, p := range parts {
			if p.Type != "text" {
				return fmt.Errorf("unsupported content type %q: only text is supported", p.Type)
			}
		}
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]any{"error": map[string]any{"message": msg, "type": typ}}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Warn("write error response", "error", err)
	}
}
