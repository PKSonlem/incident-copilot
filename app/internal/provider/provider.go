// Package provider содержит интерфейс LLM-провайдера и его реализации.
package provider

import (
	"context"
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

// Provider выполняет запрос chat completions в формате OpenAI: обычный или потоковый.
// Тело уже содержит имя модели, как её знает провайдер. Отмена ctx прерывает запрос к провайдеру.
type Provider interface {
	Complete(ctx context.Context, body []byte) (Response, error)
	Stream(ctx context.Context, body []byte) (Stream, error)
}
