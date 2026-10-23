package provider

import (
	"context"
	"errors"
)

// Claude вызывает Anthropic Messages API, переводя запрос и ответ из формата OpenAI и обратно.
type Claude struct {
	baseURL string
	apiKey  string
}

// NewClaude создаёт провайдера Claude. Заглушка: реализация появится после утверждения тестов.
func NewClaude(baseURL, apiKey string) *Claude {
	return &Claude{baseURL: baseURL, apiKey: apiKey}
}

// Complete пока не реализован.
func (c *Claude) Complete(context.Context, []byte) (Response, error) {
	return Response{}, errors.New("claude provider: not implemented")
}

// Stream пока не реализован.
func (c *Claude) Stream(context.Context, []byte) (Stream, error) {
	return Stream{}, errors.New("claude provider: not implemented")
}
