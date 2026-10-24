package httpapi

import (
	"fmt"
	"net/http"

	"github.com/PKSonlem/incident-copilot/app/internal/auth"
	"github.com/PKSonlem/incident-copilot/app/internal/cache"
	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/provider"
	"github.com/PKSonlem/incident-copilot/app/internal/provider/claude"
	"github.com/PKSonlem/incident-copilot/app/internal/provider/openai"
	"github.com/PKSonlem/incident-copilot/app/internal/ratelimit"
	"github.com/PKSonlem/incident-copilot/app/internal/routing"
)

// New собирает гейтвей из конфигурации и возвращает его обработчик: live, ready и
// /v1/chat/completions.
func New(cfg config.Config) (http.Handler, error) {
	providers := map[string]provider.Provider{}
	kinds := map[string]string{}
	for _, pc := range cfg.Providers {
		p, err := newProvider(pc)
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", pc.Name, err)
		}
		providers[pc.Name] = p
		kinds[pc.Name] = pc.Kind
	}
	routes := make([]routing.Route, 0, len(cfg.Models))
	for _, m := range cfg.Models {
		p, found := providers[m.Provider]
		if !found {
			return nil, fmt.Errorf("model %q: unknown provider %q", m.Name, m.Provider)
		}
		routes = append(routes, routing.Route{Name: m.Name, Kind: kinds[m.Provider], Provider: p, ProviderModel: m.ProviderModel})
	}
	g := &gateway{
		auth:    auth.New(cfg.Clients),
		router:  routing.New(routes),
		cache:   cache.New(cfg.Redis.Addr, cfg.Cache.EffectiveTTL()),
		limiter: ratelimit.New(cfg.Redis.Addr),
	}
	return g.routes(), nil
}

// newProvider создаёт провайдера по типу из конфигурации. Новый тип добавляется здесь и больше нигде.
func newProvider(pc config.Provider) (provider.Provider, error) {
	switch pc.Kind {
	case provider.KindOpenAI:
		return openai.New(pc.BaseURL, pc.APIKey), nil
	case provider.KindClaude:
		return claude.New(pc.BaseURL, pc.APIKey, pc.EffectiveMaxTokens()), nil
	default:
		return nil, fmt.Errorf("unsupported provider kind %q", pc.Kind)
	}
}
