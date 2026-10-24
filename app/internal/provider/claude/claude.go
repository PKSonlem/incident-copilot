package claude

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

const anthropicVersion = "2023-06-01"

// Client вызывает Anthropic Messages API, переводя запрос и ответ из формата OpenAI и обратно.
type Client struct {
	baseURL string
	apiKey  string
	client  *http.Client
	// defaultMaxTokens подставляется, если клиент не задал max_tokens: Anthropic требует его в каждом запросе.
	defaultMaxTokens int
}

// New создаёт провайдера Claude.
func New(baseURL, apiKey string, defaultMaxTokens int) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, client: &http.Client{},
		defaultMaxTokens: defaultMaxTokens,
	}
}

// Complete переводит запрос в формат Anthropic и возвращает ответ в формате OpenAI. Ошибки Claude
// тоже приходят в формате OpenAI, со статусом по правилам гейтвея.
func (c *Client) Complete(ctx context.Context, body []byte) (_ provider.Response, err error) {
	resp, _, err := c.do(ctx, body, false)
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
	if resp.StatusCode != http.StatusOK {
		status, out := translateError(resp.StatusCode, data)
		return provider.Response{Status: status, Body: out}, nil
	}
	out, err := translateResponse(data)
	if err != nil {
		return provider.Response{}, err
	}
	return provider.Response{Status: http.StatusOK, Body: out}, nil
}

// Stream отдаёт тело в виде OpenAI SSE, переводя события Anthropic по мере поступления.
func (c *Client) Stream(ctx context.Context, body []byte) (provider.Stream, error) {
	resp, includeUsage, err := c.do(ctx, body, true)
	if err != nil {
		return provider.Stream{}, err
	}
	if resp.StatusCode != http.StatusOK {
		data, rerr := io.ReadAll(resp.Body)
		if cerr := resp.Body.Close(); cerr != nil && rerr == nil {
			rerr = cerr
		}
		if rerr != nil {
			return provider.Stream{}, fmt.Errorf("read provider response: %w", rerr)
		}
		status, out := translateError(resp.StatusCode, data)
		return provider.Stream{Status: status, Body: io.NopCloser(bytes.NewReader(out))}, nil
	}
	return provider.Stream{Status: http.StatusOK, Body: newSSETranslator(resp.Body, includeUsage)}, nil
}

func (c *Client) do(ctx context.Context, body []byte, stream bool) (*http.Response, bool, error) {
	payload, includeUsage, err := translateRequest(body, stream, c.defaultMaxTokens)
	if err != nil {
		return nil, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/messages", bytes.NewReader(payload))
	if err != nil {
		return nil, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Anthropic-Version", anthropicVersion)
	req.Header.Set("X-Api-Key", c.apiKey)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("call provider: %w", err)
	}
	return resp, includeUsage, nil
}
