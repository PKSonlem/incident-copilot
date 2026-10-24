package openai

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/PKSonlem/incident-copilot/app/internal/provider"
)

// Client вызывает любой OpenAI-совместимый сервер (llama-server, llama-swap).
type Client struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// New создаёт провайдера. apiKey может быть пустым.
func New(baseURL, apiKey string) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, client: &http.Client{}}
}

// Complete отправляет запрос на /v1/chat/completions.
func (c *Client) Complete(ctx context.Context, body []byte) (_ provider.Response, err error) {
	resp, err := c.do(ctx, body)
	if err != nil {
		return provider.Response{}, err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close provider response: %w", cerr))
		}
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return provider.Response{}, fmt.Errorf("read provider response: %w", err)
	}
	return provider.Response{Status: resp.StatusCode, Body: data}, nil
}

// Stream отправляет запрос и отдаёт тело ответа, не читая его.
func (c *Client) Stream(ctx context.Context, body []byte) (provider.Stream, error) {
	resp, err := c.do(ctx, body)
	if err != nil {
		return provider.Stream{}, err
	}
	return provider.Stream{Status: resp.StatusCode, Body: resp.Body}, nil
}

func (c *Client) do(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call provider: %w", err)
	}
	return resp, nil
}
