// Package provider содержит интерфейс LLM-провайдера и его реализации.
package provider

import (
	"context"
	"fmt"
)

// KindOpenAI — любой OpenAI-совместимый сервер (llama-server, llama-swap).
const KindOpenAI = "openai"

// Response — ответ провайдера на запрос chat completions.
type Response struct {
	Status int
	Body   []byte
}

// Provider выполняет обычный (не потоковый) запрос chat completions в формате OpenAI.
// Тело уже содержит имя модели, как её знает провайдер.
type Provider interface {
	Complete(ctx context.Context, body []byte) (Response, error)
}

// New создаёт провайдера по типу из конфигурации. Новый тип добавляется здесь и больше нигде.
func New(kind, baseURL, apiKey string) (Provider, error) {
	switch kind {
	case KindOpenAI:
		return NewOpenAI(baseURL, apiKey), nil
	default:
		return nil, fmt.Errorf("unsupported provider kind %q", kind)
	}
}
