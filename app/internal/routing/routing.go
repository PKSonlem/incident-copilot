// Package routing выбирает модель и провайдера для запроса.
package routing

import (
	"errors"
	"fmt"

	"github.com/PKSonlem/incident-copilot/app/internal/provider"
)

// Complexity — оценка сложности запроса, присланная клиентом. Пустое значение считается сложным.
type Complexity string

// Значения сложности.
const (
	Unspecified Complexity = ""
	Simple      Complexity = "simple"
	Complex     Complexity = "complex"
)

// ModelAuto — алиас модели, включающий маршрутизацию по сложности.
const ModelAuto = "auto"

// ErrNoRoute означает, что в конфигурации нет модели для маршрутизации по сложности.
var ErrNoRoute = errors.New("no model is configured for routing")

// ModelNotFoundError означает, что в конфигурации нет модели с запрошенным именем.
type ModelNotFoundError struct{ Model string }

func (e *ModelNotFoundError) Error() string { return fmt.Sprintf("model %q not found", e.Model) }

// Route — модель конфигурации, привязанная к провайдеру.
type Route struct {
	Name          string // алиас модели, как его знает клиент
	Kind          string // provider.KindOpenAI или provider.KindClaude
	Provider      provider.Provider
	ProviderModel string // имя модели у провайдера
}

// Router выбирает маршрут по имени модели и сложности запроса.
type Router struct {
	routes map[string]Route
	// simple и complex — первые модели конфига с провайдером kind=openai и kind=claude.
	simple, complex *Route
}

// New собирает маршруты в порядке конфигурации.
func New(routes []Route) *Router {
	r := &Router{routes: map[string]Route{}}
	for _, rt := range routes {
		r.routes[rt.Name] = rt
		switch {
		case rt.Kind == provider.KindOpenAI && r.simple == nil:
			r.simple = &rt
		case rt.Kind == provider.KindClaude && r.complex == nil:
			r.complex = &rt
		}
	}
	return r
}

// Resolve выбирает маршрут: конкретное имя модели закрепляет запрос, "auto" и пустое имя
// включают маршрутизацию по сложности (без указания сложности запрос сложный).
func (r *Router) Resolve(model string, complexity Complexity) (Route, error) {
	if model != "" && model != ModelAuto {
		rt, found := r.routes[model]
		if !found {
			return Route{}, &ModelNotFoundError{Model: model}
		}
		return rt, nil
	}
	target := r.complex
	if complexity == Simple {
		target = r.simple
	}
	if target == nil {
		return Route{}, ErrNoRoute
	}
	return *target, nil
}
