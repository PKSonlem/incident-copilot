package provider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// OpenAI вызывает любой OpenAI-совместимый сервер (llama-server, llama-swap).
type OpenAI struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// NewOpenAI создаёт провайдера. apiKey может быть пустым.
func NewOpenAI(baseURL, apiKey string) *OpenAI {
	return &OpenAI{baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, client: &http.Client{}}
}

// Complete отправляет запрос на /v1/chat/completions.
func (o *OpenAI) Complete(ctx context.Context, body []byte) (_ Response, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Response{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if o.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.apiKey)
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("call provider: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close provider response: %w", cerr))
		}
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{}, fmt.Errorf("read provider response: %w", err)
	}
	return Response{Status: resp.StatusCode, Body: data}, nil
}
