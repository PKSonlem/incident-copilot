package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

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

// apiError — ошибка, которую обработчик превращает в ответ OpenAI-формата.
type apiError struct {
	status int
	typ    string
	msg    string
}

func (e *apiError) Error() string { return e.msg }

// providerFailure превращает ошибку вызова провайдера в ответ: запрос, который провайдер
// не поддерживает, это 400, всё остальное 502.
func providerFailure(err error) *apiError {
	var clientErr *provider.ClientError
	if errors.As(err, &clientErr) {
		return &apiError{http.StatusBadRequest, errInvalidRequest, clientErr.Message}
	}
	return &apiError{http.StatusBadGateway, errAPI, "provider request failed"}
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
