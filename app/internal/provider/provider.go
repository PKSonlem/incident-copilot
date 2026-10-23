// Package provider содержит интерфейс LLM-провайдера и его реализации.
package provider

import (
	"context"
	"fmt"
	"io"
)

// KindOpenAI — любой OpenAI-совместимый сервер (llama-server, llama-swap).
const KindOpenAI = "openai"

// Response — ответ провайдера на запрос chat completions.
type Response struct {
	Status int
	Body   []byte
}

// Stream — потоковый ответ провайдера. Вызывающий обязан закрыть Body.
type Stream struct {
	Status int
	Body   io.ReadCloser
}

// Provider выполняет запрос chat completions в формате OpenAI: обычный или потоковый.
// Тело уже содержит имя модели, как её знает провайдер. Отмена ctx прерывает запрос к провайдеру.
type Provider interface {
	Complete(ctx context.Context, body []byte) (Response, error)
	Stream(ctx context.Context, body []byte) (Stream, error)
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
