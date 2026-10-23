// Package provider содержит интерфейс LLM-провайдера и его реализации.
package provider

import (
	"context"
	"fmt"
	"io"
)

// KindOpenAI — любой OpenAI-совместимый сервер (llama-server, llama-swap).
const KindOpenAI = "openai"

// KindClaude — Anthropic Messages API.
const KindClaude = "claude"

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

// ClientError — запрос клиента, который провайдер не может выполнить (например, неподдерживаемый
// тип контента). Complete и Stream возвращают её как ошибку; сервер отвечает на неё статусом 400.
type ClientError struct {
	Message string
}

func (e *ClientError) Error() string { return e.Message }

// Options — настройки провайдера, не относящиеся к подключению.
type Options struct {
	// DefaultMaxTokens — max_tokens для Claude, если клиент его не задал.
	DefaultMaxTokens int
}

// Provider выполняет запрос chat completions в формате OpenAI: обычный или потоковый.
// Тело уже содержит имя модели, как её знает провайдер. Отмена ctx прерывает запрос к провайдеру.
type Provider interface {
	Complete(ctx context.Context, body []byte) (Response, error)
	Stream(ctx context.Context, body []byte) (Stream, error)
}

// New создаёт провайдера по типу из конфигурации. Новый тип добавляется здесь и больше нигде.
func New(kind, baseURL, apiKey string, opts Options) (Provider, error) {
	switch kind {
	case KindOpenAI:
		return NewOpenAI(baseURL, apiKey), nil
	case KindClaude:
		return NewClaude(baseURL, apiKey, opts.DefaultMaxTokens), nil
	default:
		return nil, fmt.Errorf("unsupported provider kind %q", kind)
	}
}
