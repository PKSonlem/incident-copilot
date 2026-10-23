package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// translateRequest переводит тело chat completions (OpenAI) в тело Anthropic Messages.
// Второй результат: просил ли клиент usage в потоке (stream_options.include_usage).
func translateRequest(body []byte, stream bool) ([]byte, bool, error) {
	var in struct {
		Model               string            `json:"model"`
		Messages            []json.RawMessage `json:"messages"`
		MaxTokens           *int              `json:"max_tokens"`
		MaxCompletionTokens *int              `json:"max_completion_tokens"`
		Temperature         *float64          `json:"temperature"`
		TopP                *float64          `json:"top_p"`
		Stop                json.RawMessage   `json:"stop"`
		StreamOptions       struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, false, fmt.Errorf("decode request: %w", err)
	}
	out := map[string]any{"model": in.Model, "max_tokens": defaultMaxTokens}
	switch {
	case in.MaxTokens != nil:
		out["max_tokens"] = *in.MaxTokens
	case in.MaxCompletionTokens != nil:
		out["max_tokens"] = *in.MaxCompletionTokens
	}
	if in.Temperature != nil {
		out["temperature"] = *in.Temperature
	}
	if in.TopP != nil {
		out["top_p"] = *in.TopP
	}
	if stop, err := stopSequences(in.Stop); err != nil {
		return nil, false, err
	} else if len(stop) > 0 {
		out["stop_sequences"] = stop
	}
	if stream {
		out["stream"] = true
	}

	var system []string
	messages := []map[string]any{}
	for _, raw := range in.Messages {
		var m struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, false, fmt.Errorf("decode message: %w", err)
		}
		text, err := contentText(m.Content)
		if err != nil {
			return nil, false, err
		}
		if m.Role == "system" || m.Role == "developer" {
			system = append(system, text)
			continue
		}
		messages = append(messages, map[string]any{"role": m.Role, "content": text})
	}
	if len(system) > 0 {
		out["system"] = strings.Join(system, "\n\n")
	}
	out["messages"] = messages
	data, err := json.Marshal(out)
	if err != nil {
		return nil, false, fmt.Errorf("encode request: %w", err)
	}
	return data, in.StreamOptions.IncludeUsage, nil
}

// contentText приводит content (строка, null или массив текстовых частей) к строке.
func contentText(raw json.RawMessage) (string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return "", nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", errors.New("unsupported message content")
	}
	var sb strings.Builder
	for _, p := range parts {
		if p.Type != "text" {
			return "", fmt.Errorf("unsupported content type %q", p.Type)
		}
		sb.WriteString(p.Text)
	}
	return sb.String(), nil
}

func stopSequences(raw json.RawMessage) ([]string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return []string{one}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, errors.New("stop must be a string or an array of strings")
	}
	return many, nil
}

// finishReason переводит stop_reason Anthropic в finish_reason OpenAI.
func finishReason(stopReason string) string {
	switch stopReason {
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	default:
		return "stop"
	}
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func openAIUsage(promptTokens, completionTokens int) map[string]int {
	return map[string]int{
		"prompt_tokens": promptTokens, "completion_tokens": completionTokens, "total_tokens": promptTokens + completionTokens,
	}
}

// translateResponse переводит ответ Anthropic Messages в chat.completion.
func translateResponse(data []byte) ([]byte, error) {
	var in struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StopReason string         `json:"stop_reason"`
		Usage      anthropicUsage `json:"usage"`
	}
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("decode provider response: %w", err)
	}
	var text strings.Builder
	for _, b := range in.Content {
		if b.Type == "text" {
			text.WriteString(b.Text)
		}
	}
	out, err := json.Marshal(map[string]any{
		"id":      "chatcmpl-" + in.ID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   in.Model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": text.String()},
			"finish_reason": finishReason(in.StopReason),
		}},
		"usage": openAIUsage(in.Usage.InputTokens, in.Usage.OutputTokens),
	})
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return out, nil
}

// openAIErrorBody собирает тело ошибки в формате OpenAI.
func openAIErrorBody(message, typ string) []byte {
	out, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": message, "type": typ, "param": nil, "code": nil,
	}})
	if err != nil {
		return []byte(`{"error":{"message":"provider request failed","type":"api_error","param":null,"code":null}}`)
	}
	return out
}

// translateError переводит ошибку Anthropic в статус и тело в формате OpenAI. Ошибки запроса (400)
// и лимита (429) проходят со своим статусом. Остальное (ключ гейтвея, перегрузка, сбои) клиент
// исправить не может, это 502 без подробностей провайдера.
func translateError(status int, data []byte) (int, []byte) {
	var in struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	msg := "provider request failed"
	if json.Unmarshal(data, &in) == nil && in.Error.Message != "" {
		msg = in.Error.Message
	}
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return http.StatusBadRequest, openAIErrorBody(msg, "invalid_request_error")
	case http.StatusTooManyRequests:
		return http.StatusTooManyRequests, openAIErrorBody(msg, "rate_limit_error")
	default:
		return http.StatusBadGateway, openAIErrorBody("provider request failed", "api_error")
	}
}
