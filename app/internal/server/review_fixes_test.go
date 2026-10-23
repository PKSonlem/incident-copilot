package server_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/server"
)

// --- Claude: инструменты пока не переводятся (#6), запрос с ними получает 400 ---

func TestClaudeRequestWithToolsIsRejectedWith400(t *testing.T) {
	cases := map[string]string{
		"tools":       `"tools":[{"type":"function","function":{"name":"f","parameters":{}}}],` + pingMessages,
		"tool_choice": `"tool_choice":"auto",` + pingMessages,
		"роль tool": `"messages":[{"role":"user","content":"ping"},` +
			`{"role":"tool","tool_call_id":"c1","content":"42"}]`,
		"assistant.tool_calls": `"messages":[{"role":"user","content":"ping"},` +
			`{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]`,
	}
	for name, body := range cases {
		for _, stream := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				fa := newFakeAnthropic(t)
				ts := newClaudeGateway(t, fa)
				reqBody := claudeRequest(body)
				if stream {
					reqBody = claudeRequest(body + `,"stream":true`)
				}

				resp, got := post(t, ts, clientKey, reqBody)

				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status %d, want 400; body: %s", resp.StatusCode, got)
				}
				var env struct {
					Error struct {
						Message string `json:"message"`
						Type    string `json:"type"`
					} `json:"error"`
				}
				if err := json.Unmarshal(got, &env); err != nil || env.Error.Type != "invalid_request_error" ||
					!strings.Contains(env.Error.Message, "tools not supported") {
					t.Errorf("ожидали invalid_request_error «tools not supported…», got %s", got)
				}
				if n := len(fa.received()); n != 0 {
					t.Errorf("Anthropic получил %d запросов, want 0", n)
				}
			})
		}
	}
}

// --- Claude: причины остановки ---

func TestClaudeRefusalMapsToContentFilterAndUnknownStopReasonToStop(t *testing.T) {
	for stopReason, want := range map[string]string{"refusal": "content_filter", "something_new": "stop"} {
		t.Run(stopReason, func(t *testing.T) {
			fa := newFakeAnthropic(t)
			fa.reply = strings.Replace(anthropicReply, `"end_turn"`, `"`+stopReason+`"`, 1)
			ts := newClaudeGateway(t, fa)

			_, body := post(t, ts, clientKey, claudeRequest(pingMessages))

			var got struct {
				Choices []struct {
					FinishReason string `json:"finish_reason"`
				} `json:"choices"`
			}
			if err := json.Unmarshal(body, &got); err != nil || len(got.Choices) != 1 {
				t.Fatalf("плохой ответ: %v; body: %s", err, body)
			}
			if got.Choices[0].FinishReason != want {
				t.Errorf("finish_reason %q, want %q", got.Choices[0].FinishReason, want)
			}
		})
	}
}

// --- max_tokens по умолчанию задаётся конфигурацией ---

func TestClaudeDefaultMaxTokensComesFromConfig(t *testing.T) {
	for name, tc := range map[string]struct {
		configured int
		want       float64
	}{"из конфига": {configured: 123, want: 123}, "по умолчанию": {configured: 0, want: 4096}} {
		t.Run(name, func(t *testing.T) {
			fa := newFakeAnthropic(t)
			h, err := server.New(config.Config{
				Redis: config.Redis{Addr: startRedis(t)},
				Providers: []config.Provider{{
					Name: "anthropic", Kind: "claude", BaseURL: fa.URL, APIKey: anthropicKey, DefaultMaxTokens: tc.configured,
				}},
				Models:  []config.Model{{Name: claudeAlias, Provider: "anthropic", ProviderModel: claudeModel}},
				Clients: []config.Client{{Name: "agent", Key: clientKey}},
			})
			if err != nil {
				t.Fatalf("server.New: %v", err)
			}
			ts := httptest.NewServer(h)
			t.Cleanup(ts.Close)

			post(t, ts, clientKey, claudeRequest(pingMessages))

			reqs := fa.received()
			if len(reqs) != 1 || reqs[0].Body["max_tokens"] != tc.want {
				t.Errorf("max_tokens в запросе к Anthropic: %+v, want %v", reqs, tc.want)
			}
		})
	}
}

// --- ключ кэша ---

func TestFieldsThatDoNotAffectTheAnswerDoNotChangeTheCacheKey(t *testing.T) {
	cases := map[string]string{
		"user":           `"temperature":0,"user":"alice"`,
		"metadata":       `"temperature":0,"metadata":{"trace":"x"}`,
		"stream_options": `"temperature":0,"stream_options":{"include_usage":true}`,
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			dp := newDualProvider(t)
			ts := newCacheGateway(t, dp, time.Minute)

			post(t, ts, clientKey, chatBody("local-small", "ping", `"temperature":0`))
			resp, _ := post(t, ts, clientKey, chatBody("local-small", "ping", extra))

			if got := resp.Header.Get(cacheHeader); got != cacheHit {
				t.Errorf("%s %q, want %q: поле %s не влияет на ответ", cacheHeader, got, cacheHit, name)
			}
			if n := dp.count(); n != 1 {
				t.Errorf("провайдер получил %d запросов, want 1", n)
			}
		})
	}
}

func TestRequestForSeveralChoicesBypassesCache(t *testing.T) {
	dp := newDualProvider(t)
	ts := newCacheGateway(t, dp, time.Minute)
	reqBody := chatBody("local-small", "ping", `"temperature":0,"n":2`)

	post(t, ts, clientKey, reqBody)
	resp, _ := post(t, ts, clientKey, reqBody)

	if got := resp.Header.Get(cacheHeader); got != "BYPASS" {
		t.Errorf("%s %q, want BYPASS", cacheHeader, got)
	}
	if n := dp.count(); n != 2 {
		t.Errorf("провайдер получил %d запросов, want 2", n)
	}
}

// --- повтор из кэша с вызовами инструментов ---

func newToolCallProvider(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write := func(s string) {
			if _, err := w.Write([]byte(s)); err != nil {
				t.Errorf("fake provider write: %v", err)
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, chunk := range []string{
			`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":null,` +
				`"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":""}}]}}]}`,
			`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{` +
				`"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":1}"}}]},"finish_reason":"tool_calls"}]}`,
			`[DONE]`,
		} {
			write("data: " + chunk + "\n\n")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestCachedToolCallIsReplayedWithoutEmptyContent(t *testing.T) {
	provider := newToolCallProvider(t)
	h, err := server.New(config.Config{
		Redis:     config.Redis{Addr: startRedis(t)},
		Providers: []config.Provider{{Name: "local", Kind: "openai", BaseURL: provider.URL}},
		Models:    []config.Model{{Name: "local-small", Provider: "local", ProviderModel: "qwen-local"}},
		Clients:   []config.Client{{Name: "agent", Key: clientKey}},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	reqBody := chatBody("local-small", "ping", `"temperature":0,"stream":true`)

	first := postStream(t, ts, reqBody)
	_ = sseContent(t, first)
	second := postStream(t, ts, reqBody)

	if got := second.Header.Get(cacheHeader); got != "HIT" {
		t.Fatalf("%s %q, want HIT", cacheHeader, got)
	}
	var sawToolCall bool
	var finish string
	sc := bufio.NewScanner(second.Body)
	for sc.Scan() {
		payload, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || payload == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta        map[string]any `json:"delta"`
				FinishReason *string        `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			t.Fatalf("событие не JSON: %v: %s", err, payload)
		}
		for _, c := range chunk.Choices {
			if _, has := c.Delta["content"]; has {
				t.Errorf("delta с tool_calls содержит content: %s", payload)
			}
			if calls, ok := c.Delta["tool_calls"].([]any); ok && len(calls) == 1 {
				sawToolCall = true
			}
			if c.FinishReason != nil {
				finish = *c.FinishReason
			}
		}
	}
	if !sawToolCall || finish != "tool_calls" {
		t.Errorf("tool_calls=%v finish_reason=%q: вызов инструмента потерян при повторе из кэша", sawToolCall, finish)
	}
}
