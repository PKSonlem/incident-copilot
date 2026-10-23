// Package provider содержит интерфейс LLM-провайдера и его реализации.
package provider

import "context"

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
