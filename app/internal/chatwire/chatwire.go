// Package chatwire содержит мелкие помощники формата OpenAI chat completions, общие для
// провайдеров и сервера.
package chatwire

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DoneEvent — завершающее событие потока SSE.
const DoneEvent = "data: [DONE]\n\n"

// IsNullOrEmpty сообщает, что JSON-значение отсутствует, пусто или равно null.
func IsNullOrEmpty(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed == "" || trimmed == "null"
}

// Chunk описывает один chat.completion.chunk.
type Chunk struct {
	ID      string
	Model   string
	Created json.RawMessage // не задан: поле created не пишется
	Choices []map[string]any
	Usage   any // не nil: чанк с usage, как последний чанк include_usage
}

// Choice собирает элемент choices потокового чанка. finishReason nil даёт finish_reason: null.
func Choice(index int, delta map[string]any, finishReason any) map[string]any {
	return map[string]any{"index": index, "delta": delta, "finish_reason": finishReason}
}

// SSE кодирует чанк как событие SSE.
func (c Chunk) SSE() ([]byte, error) {
	choices := c.Choices
	if choices == nil {
		choices = []map[string]any{}
	}
	body := map[string]any{"id": c.ID, "object": "chat.completion.chunk", "model": c.Model, "choices": choices}
	if len(c.Created) > 0 {
		body["created"] = c.Created
	}
	if c.Usage != nil {
		body["usage"] = c.Usage
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode chunk: %w", err)
	}
	return append(append([]byte("data: "), data...), '\n', '\n'), nil
}
