package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/PKSonlem/incident-copilot/app/internal/cache"
	"github.com/PKSonlem/incident-copilot/app/internal/chatwire"
	"github.com/PKSonlem/incident-copilot/app/internal/routing"
)

const maxBodyBytes = 4 << 20

// complexityHeader — заголовок, в котором клиент сообщает сложность запроса.
const complexityHeader = "X-Complexity"

// prepared — проверенный запрос: маршрут, тело для провайдера, признак потока и решение кэша.
type prepared struct {
	route  routing.Route
	body   []byte
	stream bool
	cache  cache.Plan
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
	complexity := routing.Complexity(r.Header.Get(complexityHeader))
	if complexity != routing.Unspecified && complexity != routing.Simple && complexity != routing.Complex {
		return prepared{}, bad(fmt.Sprintf("%s must be %q or %q", complexityHeader, routing.Simple, routing.Complex))
	}
	rt, err := g.router.Resolve(model, complexity)
	if err != nil {
		return prepared{}, routeFailure(err)
	}
	plan := cache.NewPlan(req, rt.Name)
	req["model"], err = json.Marshal(rt.ProviderModel)
	if err != nil {
		return prepared{}, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return prepared{}, &apiError{http.StatusInternalServerError, errAPI, "cannot encode request"}
	}
	return prepared{route: rt, body: body, stream: stream, cache: plan}, nil
}

func isTrue(raw json.RawMessage) bool {
	var b bool
	return json.Unmarshal(raw, &b) == nil && b
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

// routeFailure превращает ошибку маршрутизации в ответ.
func routeFailure(err error) *apiError {
	var notFound *routing.ModelNotFoundError
	if errors.As(err, &notFound) {
		return &apiError{http.StatusNotFound, errNotFound, notFound.Error()}
	}
	return &apiError{http.StatusInternalServerError, errAPI, err.Error()}
}
