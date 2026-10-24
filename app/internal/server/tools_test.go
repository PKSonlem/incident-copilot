package server_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/server"
)

// Тесты tool calling (#6) через HTTP-шов гейтвея: клиент шлёт tools в формате OpenAI и получает
// tool_calls в формате OpenAI независимо от провайдера (локальный OpenAI-совместимый или Claude).

const (
	toolsDef = `"tools":[{"type":"function","function":{"name":"get_metrics","description":"Query service metrics",` +
		`"parameters":{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]}}}]`
	toolsUserMsg = `{"role":"user","content":"why is checkout slow?"}`
	// Ход агента: модель вызвала tool, клиент вернул его результат.
	toolsAssistantCall = `{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function",` +
		`"function":{"name":"get_metrics","arguments":"{\"service\":\"checkout\"}"}}]}`
	toolsResultMsg = `{"role":"tool","tool_call_id":"call_1","content":"p99=2300ms"}`
)

// --- общие помощники ---

// toolCall — вызов tool в ответе гейтвея (формат OpenAI).
type toolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type toolsCompletion struct {
	Choices []struct {
		Message struct {
			Role      string     `json:"role"`
			Content   *string    `json:"content"`
			ToolCalls []toolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

func decodeToolsCompletion(t *testing.T, resp *http.Response, body []byte) toolsCompletion {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	var got toolsCompletion
	if err := json.Unmarshal(body, &got); err != nil || len(got.Choices) != 1 {
		t.Fatalf("плохой ответ: %v; body: %s", err, body)
	}
	return got
}

// streamedToolCalls — вызовы tools, собранные из потока чанков, плюс текст и finish_reason.
type streamedToolCalls struct {
	calls   map[int]*toolCall
	firstAt map[int]toolCall // содержимое самого первого фрагмента вызова: id и имя обязаны быть в нём
	text    string
	finish  string
	nCalls  int
}

// readToolStream читает SSE до [DONE] и собирает фрагменты tool_calls по index.
func readToolStream(t *testing.T, resp *http.Response) streamedToolCalls {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	out := streamedToolCalls{calls: map[int]*toolCall{}, firstAt: map[int]toolCall{}}
	sc := bufio.NewScanner(resp.Body)
	for {
		payload := nextData(t, sc)
		if payload == "[DONE]" {
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   string `json:"content"`
					ToolCalls []struct {
						Index    *int   `json:"index"`
						ID       string `json:"id"`
						Type     string `json:"type"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			t.Fatalf("событие не JSON: %v; %q", err, payload)
		}
		for _, ch := range chunk.Choices {
			out.text += ch.Delta.Content
			if ch.FinishReason != nil {
				out.finish = *ch.FinishReason
			}
			for _, frag := range ch.Delta.ToolCalls {
				if frag.Index == nil {
					t.Fatalf("фрагмент tool_calls без index: %s", payload)
				}
				call, seen := out.calls[*frag.Index]
				if !seen {
					call = &toolCall{}
					out.calls[*frag.Index] = call
					first := toolCall{ID: frag.ID, Type: frag.Type}
					first.Function.Name = frag.Function.Name
					out.firstAt[*frag.Index] = first
					out.nCalls++
				}
				if frag.ID != "" {
					call.ID = frag.ID
				}
				if frag.Type != "" {
					call.Type = frag.Type
				}
				if frag.Function.Name != "" {
					call.Function.Name = frag.Function.Name
				}
				call.Function.Arguments += frag.Function.Arguments
			}
		}
	}
	return out
}

func assertJSONArgs(t *testing.T, args, wantKey, wantValue string) {
	t.Helper()
	var parsed map[string]any
	if err := json.Unmarshal([]byte(args), &parsed); err != nil {
		t.Fatalf("arguments %q не JSON-объект: %v", args, err)
	}
	if parsed[wantKey] != wantValue {
		t.Errorf("arguments %q: %s=%v, want %s", args, wantKey, parsed[wantKey], wantValue)
	}
}

// --- локальный провайдер (OpenAI-совместимый): tools проходят насквозь ---

// toolsLocalProvider — фейковый OpenAI-совместимый сервер, который записывает тела запросов и отвечает
// вызовом tool (обычный JSON или SSE по полю stream).
type toolsLocalProvider struct {
	*httptest.Server
	mu   sync.Mutex
	seen []map[string]any
}

const (
	localToolReply = `{"id":"chatcmpl-1","object":"chat.completion","model":"qwen-local","choices":[{"index":0,` +
		`"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_7","type":"function",` +
		`"function":{"name":"get_metrics","arguments":"{\"service\":\"checkout\"}"}}]},"finish_reason":"tool_calls"}],` +
		`"usage":{"prompt_tokens":20,"completion_tokens":9,"total_tokens":29}}`
)

var localToolStream = []string{
	`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":null,` +
		`"tool_calls":[{"index":0,"id":"call_7","type":"function","function":{"name":"get_metrics","arguments":""}}]}}]}`,
	`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{` +
		`"tool_calls":[{"index":0,"function":{"arguments":"{\"service\":"}}]}}]}`,
	`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{` +
		`"tool_calls":[{"index":0,"function":{"arguments":"\"checkout\"}"}}]},"finish_reason":"tool_calls"}]}`,
}

func newToolsLocalProvider(t *testing.T) *toolsLocalProvider {
	t.Helper()
	lp := &toolsLocalProvider{}
	lp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lp.mu.Lock()
		lp.seen = append(lp.seen, body)
		lp.mu.Unlock()
		if body["stream"] != true {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(localToolReply)) //nolint:errcheck // фейк
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, c := range localToolStream {
			_, _ = w.Write([]byte("data: " + c + "\n\n")) //nolint:errcheck // фейк
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n")) //nolint:errcheck // фейк
	}))
	t.Cleanup(lp.Close)
	return lp
}

func (lp *toolsLocalProvider) received() []map[string]any {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	return append([]map[string]any(nil), lp.seen...)
}

func newToolsLocalGateway(t *testing.T, lp *toolsLocalProvider) *httptest.Server {
	t.Helper()
	h, err := server.New(config.Config{
		Redis:     config.Redis{Addr: startRedis(t)},
		Providers: []config.Provider{{Name: "local", Kind: "openai", BaseURL: lp.URL}},
		Models:    []config.Model{{Name: "local-small", Provider: "local", ProviderModel: "qwen-local"}},
		Clients:   []config.Client{{Name: "agent", Key: clientKey}},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func localToolsRequest(extra, messages string) string {
	return `{"model":"local-small",` + extra + toolsDef + `,"messages":[` + messages + `]}`
}

func TestToolsLocalPlainRequestForwardsToolsAndReturnsToolCalls(t *testing.T) {
	lp := newToolsLocalProvider(t)
	ts := newToolsLocalGateway(t, lp)

	resp, body := post(t, ts, clientKey, localToolsRequest(`"tool_choice":"auto",`, toolsUserMsg))

	got := decodeToolsCompletion(t, resp, body)
	msg := got.Choices[0].Message
	if got.Choices[0].FinishReason != "tool_calls" {
		t.Errorf("finish_reason %q, want tool_calls", got.Choices[0].FinishReason)
	}
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].ID != "call_7" || msg.ToolCalls[0].Type != "function" ||
		msg.ToolCalls[0].Function.Name != "get_metrics" {
		t.Fatalf("вызов tool потерян или искажён: %s", body)
	}
	assertJSONArgs(t, msg.ToolCalls[0].Function.Arguments, "service", "checkout")

	reqs := lp.received()
	if len(reqs) != 1 {
		t.Fatalf("провайдер получил %d запросов, want 1", len(reqs))
	}
	tools, _ := reqs[0]["tools"].([]any) //nolint:errcheck // длина проверяется ниже
	if len(tools) != 1 {
		t.Fatalf("tools не дошли до провайдера: %v", reqs[0]["tools"])
	}
	if !strings.Contains(mustJSON(t, tools[0]), `"name":"get_metrics"`) || !strings.Contains(mustJSON(t, tools[0]), `"required":["service"]`) {
		t.Errorf("описание tool искажено: %s", mustJSON(t, tools[0]))
	}
	if reqs[0]["tool_choice"] != "auto" {
		t.Errorf("tool_choice %v, want auto", reqs[0]["tool_choice"])
	}
}

func TestToolsLocalStreamReturnsToolCallChunks(t *testing.T) {
	lp := newToolsLocalProvider(t)
	ts := newToolsLocalGateway(t, lp)

	resp := postStream(t, ts, localToolsRequest(`"stream":true,`, toolsUserMsg))

	got := readToolStream(t, resp)
	if got.nCalls != 1 || got.calls[0] == nil {
		t.Fatalf("ждём один вызов tool с index 0, got %+v", got.calls)
	}
	call := got.calls[0]
	if call.ID != "call_7" || call.Type != "function" || call.Function.Name != "get_metrics" {
		t.Errorf("вызов искажён: %+v", call)
	}
	assertJSONArgs(t, call.Function.Arguments, "service", "checkout")
	if got.finish != "tool_calls" {
		t.Errorf("finish_reason %q, want tool_calls", got.finish)
	}
	reqs := lp.received()
	if len(reqs) != 1 || reqs[0]["stream"] != true || reqs[0]["tools"] == nil {
		t.Errorf("провайдер получил %v: ждём один потоковый запрос с tools", reqs)
	}
}

func TestToolsLocalToolResultsReachProviderUnchanged(t *testing.T) {
	lp := newToolsLocalProvider(t)
	ts := newToolsLocalGateway(t, lp)

	resp, body := post(t, ts, clientKey, localToolsRequest("", toolsUserMsg+","+toolsAssistantCall+","+toolsResultMsg))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	reqs := lp.received()
	if len(reqs) != 1 {
		t.Fatalf("провайдер получил %d запросов, want 1", len(reqs))
	}
	msgs, _ := reqs[0]["messages"].([]any) //nolint:errcheck // длина проверяется ниже
	if len(msgs) != 3 {
		t.Fatalf("сообщений %d, want 3: %v", len(msgs), msgs)
	}
	assistant, _ := msgs[1].(map[string]any)                                                                                       //nolint:errcheck // проверяется ниже
	if calls, _ := assistant["tool_calls"].([]any); len(calls) != 1 || !strings.Contains(mustJSON(t, calls[0]), `"id":"call_1"`) { //nolint:errcheck // проверка длиной
		t.Errorf("assistant.tool_calls потеряны: %v", assistant)
	}
	tool, _ := msgs[2].(map[string]any) //nolint:errcheck // проверяется ниже
	if tool["role"] != "tool" || tool["tool_call_id"] != "call_1" || tool["content"] != "p99=2300ms" {
		t.Errorf("сообщение роли tool искажено: %v", tool)
	}
}

// --- Claude: перевод в формат Anthropic и обратно ---

// toolsAnthropic — фейковый Anthropic Messages API: записывает запросы и отвечает заданным телом
// (обычный режим) или заданными SSE-событиями (потоковый режим).
type toolsAnthropic struct {
	*httptest.Server
	mu     sync.Mutex
	seen   []map[string]any
	reply  string
	events []string // готовые SSE-события, уже закодированные через sseEvent
}

func newToolsAnthropic(t *testing.T) *toolsAnthropic {
	t.Helper()
	ta := &toolsAnthropic{reply: anthropicToolUseReply, events: anthropicToolUseStream}
	ta.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ta.mu.Lock()
		ta.seen = append(ta.seen, body)
		ta.mu.Unlock()
		if body["stream"] != true {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(ta.reply)) //nolint:errcheck // фейк
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, e := range ta.events {
			_, _ = w.Write([]byte(e)) //nolint:errcheck // фейк
		}
	}))
	t.Cleanup(ta.Close)
	return ta
}

func (ta *toolsAnthropic) received() []map[string]any {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	return append([]map[string]any(nil), ta.seen...)
}

// Ответ Claude: текст и два tool_use подряд (параллельные вызовы).
const anthropicToolUseReply = `{"id":"msg_02","type":"message","role":"assistant","model":"claude-test-model",` +
	`"content":[{"type":"text","text":"Checking."},` +
	`{"type":"tool_use","id":"toolu_1","name":"get_metrics","input":{"service":"checkout"}},` +
	`{"type":"tool_use","id":"toolu_2","name":"get_logs","input":{"service":"payments"}}],` +
	`"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":30,"output_tokens":12}}`

var anthropicToolUseStream = []string{
	sseEvent("message_start", `{"type":"message_start","message":{"id":"msg_02","type":"message","role":"assistant",`+
		`"model":"claude-test-model","content":[],"stop_reason":null,"usage":{"input_tokens":30,"output_tokens":1}}}`),
	sseEvent("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`),
	sseEvent("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Checking."}}`),
	sseEvent("content_block_stop", `{"type":"content_block_stop","index":0}`),
	sseEvent("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use",`+
		`"id":"toolu_1","name":"get_metrics","input":{}}}`),
	sseEvent("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":""}}`),
	sseEvent("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"service\":"}}`),
	sseEvent("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"checkout\"}"}}`),
	sseEvent("content_block_stop", `{"type":"content_block_stop","index":1}`),
	sseEvent("content_block_start", `{"type":"content_block_start","index":2,"content_block":{"type":"tool_use",`+
		`"id":"toolu_2","name":"get_logs","input":{}}}`),
	sseEvent("content_block_delta", `{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"service\":\"payments\"}"}}`),
	sseEvent("content_block_stop", `{"type":"content_block_stop","index":2}`),
	sseEvent("message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":12}}`),
	sseEvent("message_stop", `{"type":"message_stop"}`),
}

func newToolsClaudeGateway(t *testing.T, ta *toolsAnthropic) *httptest.Server {
	t.Helper()
	h, err := server.New(config.Config{
		Redis:     config.Redis{Addr: startRedis(t)},
		Providers: []config.Provider{{Name: "anthropic", Kind: "claude", BaseURL: ta.URL, APIKey: anthropicKey}},
		Models:    []config.Model{{Name: claudeAlias, Provider: "anthropic", ProviderModel: claudeModel}},
		Clients:   []config.Client{{Name: "agent", Key: clientKey}},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func claudeToolsRequest(extra, messages string) string {
	return claudeRequest(extra + toolsDef + `,"messages":[` + messages + `]`)
}

// anthropicMessages возвращает messages единственного запроса к фейку Anthropic.
func anthropicMessages(t *testing.T, ta *toolsAnthropic) []map[string]any {
	t.Helper()
	reqs := ta.received()
	if len(reqs) != 1 {
		t.Fatalf("Anthropic получил %d запросов, want 1", len(reqs))
	}
	raw, _ := reqs[0]["messages"].([]any) //nolint:errcheck // проверяется ниже
	out := make([]map[string]any, 0, len(raw))
	for _, m := range raw {
		mm, ok := m.(map[string]any)
		if !ok {
			t.Fatalf("сообщение не объект: %v", m)
		}
		out = append(out, mm)
	}
	return out
}

// contentBlocks приводит content сообщения Anthropic к списку блоков.
func contentBlocks(t *testing.T, msg map[string]any) []map[string]any {
	t.Helper()
	raw, ok := msg["content"].([]any)
	if !ok {
		t.Fatalf("content %v: ждём массив блоков", msg["content"])
	}
	out := make([]map[string]any, 0, len(raw))
	for _, b := range raw {
		bm, ok := b.(map[string]any)
		if !ok {
			t.Fatalf("блок не объект: %v", b)
		}
		out = append(out, bm)
	}
	return out
}

func TestToolsClaudeRequestToolsAreTranslatedToAnthropicFormat(t *testing.T) {
	ta := newToolsAnthropic(t)
	ts := newToolsClaudeGateway(t, ta)

	resp, body := post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	reqs := ta.received()
	if len(reqs) != 1 {
		t.Fatalf("Anthropic получил %d запросов, want 1", len(reqs))
	}
	tools, _ := reqs[0]["tools"].([]any) //nolint:errcheck // длина проверяется ниже
	if len(tools) != 1 {
		t.Fatalf("tools не дошли до Anthropic: %v", reqs[0]["tools"])
	}
	tool, _ := tools[0].(map[string]any) //nolint:errcheck // поля проверяются ниже
	if tool["name"] != "get_metrics" || tool["description"] != "Query service metrics" {
		t.Errorf("name/description не на верхнем уровне tool: %v", tool)
	}
	if _, wrapped := tool["function"]; wrapped || tool["type"] == "function" {
		t.Errorf("tool остался в обёртке OpenAI function: %v", tool)
	}
	schema, _ := tool["input_schema"].(map[string]any) //nolint:errcheck // проверяется ниже
	if schema["type"] != "object" || !strings.Contains(mustJSON(t, schema["properties"]), `"service"`) ||
		!strings.Contains(mustJSON(t, schema["required"]), `"service"`) {
		t.Errorf("parameters должны уйти в input_schema без потерь: %v", tool["input_schema"])
	}
}

func TestToolsClaudeToolChoiceIsTranslated(t *testing.T) {
	cases := map[string]struct{ choice, wantType, wantName string }{
		"auto":     {`"auto"`, "auto", ""},
		"required": {`"required"`, "any", ""},
		"named":    {`{"type":"function","function":{"name":"get_metrics"}}`, "tool", "get_metrics"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ta := newToolsAnthropic(t)
			ts := newToolsClaudeGateway(t, ta)

			resp, body := post(t, ts, clientKey, claudeToolsRequest(`"tool_choice":`+tc.choice+`,`, toolsUserMsg))

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
			}
			reqs := ta.received()
			if len(reqs) != 1 {
				t.Fatalf("Anthropic получил %d запросов, want 1", len(reqs))
			}
			choice, _ := reqs[0]["tool_choice"].(map[string]any) //nolint:errcheck // проверяется ниже
			if choice["type"] != tc.wantType {
				t.Errorf("tool_choice %v, want type=%s", reqs[0]["tool_choice"], tc.wantType)
			}
			if tc.wantName != "" && choice["name"] != tc.wantName {
				t.Errorf("tool_choice %v, want name=%s", reqs[0]["tool_choice"], tc.wantName)
			}
		})
	}
}

func TestToolsClaudePlainToolUseBecomesToolCalls(t *testing.T) {
	ta := newToolsAnthropic(t)
	ts := newToolsClaudeGateway(t, ta)

	resp, body := post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg))

	got := decodeToolsCompletion(t, resp, body)
	msg := got.Choices[0].Message
	if got.Choices[0].FinishReason != "tool_calls" {
		t.Errorf("finish_reason %q, want tool_calls (stop_reason tool_use)", got.Choices[0].FinishReason)
	}
	if msg.Content == nil || *msg.Content != "Checking." {
		t.Errorf("текст рядом с вызовами потерян: %s", body)
	}
	if len(msg.ToolCalls) != 2 {
		t.Fatalf("вызовов %d, want 2: %s", len(msg.ToolCalls), body)
	}
	first, second := msg.ToolCalls[0], msg.ToolCalls[1]
	if first.ID != "toolu_1" || first.Type != "function" || first.Function.Name != "get_metrics" {
		t.Errorf("первый вызов искажён: %+v", first)
	}
	if second.ID != "toolu_2" || second.Function.Name != "get_logs" {
		t.Errorf("второй вызов искажён (порядок сохраняется): %+v", second)
	}
	// У OpenAI arguments это строка с JSON, а не объект, как input у Anthropic.
	assertJSONArgs(t, first.Function.Arguments, "service", "checkout")
	assertJSONArgs(t, second.Function.Arguments, "service", "payments")
}

func TestToolsClaudeToolUseOnlyResponseHasNoText(t *testing.T) {
	ta := newToolsAnthropic(t)
	ta.reply = `{"id":"msg_03","type":"message","role":"assistant","model":"claude-test-model","content":[` +
		`{"type":"tool_use","id":"toolu_9","name":"get_metrics","input":{"service":"checkout"}}],` +
		`"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":4}}`
	ts := newToolsClaudeGateway(t, ta)

	resp, body := post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg))

	got := decodeToolsCompletion(t, resp, body)
	msg := got.Choices[0].Message
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].ID != "toolu_9" {
		t.Fatalf("вызов потерян: %s", body)
	}
	if msg.Content != nil && *msg.Content != "" {
		t.Errorf("content %q: без текста ждём null или пусто", *msg.Content)
	}
}

func TestToolsClaudeStreamToolUseBecomesToolCallChunks(t *testing.T) {
	ta := newToolsAnthropic(t)
	ts := newToolsClaudeGateway(t, ta)

	resp := postStream(t, ts, claudeToolsRequest(`"stream":true,`, toolsUserMsg))

	got := readToolStream(t, resp)
	if got.text != "Checking." {
		t.Errorf("текст %q, want Checking.", got.text)
	}
	if got.nCalls != 2 || got.calls[0] == nil || got.calls[1] == nil {
		t.Fatalf("ждём вызовы с index 0 и 1 (счёт по вызовам, а не по блокам Anthropic), got %+v", got.calls)
	}
	for i, want := range []struct{ id, name, arg string }{
		{"toolu_1", "get_metrics", "checkout"},
		{"toolu_2", "get_logs", "payments"},
	} {
		call := got.calls[i]
		if call.ID != want.id || call.Type != "function" || call.Function.Name != want.name {
			t.Errorf("вызов %d искажён: %+v", i, call)
		}
		assertJSONArgs(t, call.Function.Arguments, "service", want.arg)
		if first := got.firstAt[i]; first.ID != want.id || first.Function.Name != want.name {
			t.Errorf("вызов %d: id и имя обязаны прийти в первом фрагменте, got %+v", i, first)
		}
	}
	if got.finish != "tool_calls" {
		t.Errorf("finish_reason %q, want tool_calls", got.finish)
	}
}

func TestToolsClaudeToolResultTurnIsTranslatedToAnthropicBlocks(t *testing.T) {
	ta := newToolsAnthropic(t)
	ts := newToolsClaudeGateway(t, ta)

	resp, body := post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg+","+toolsAssistantCall+","+toolsResultMsg))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	msgs := anthropicMessages(t, ta)
	if len(msgs) != 3 {
		t.Fatalf("сообщений %d, want 3 (user, assistant, user с tool_result): %v", len(msgs), msgs)
	}
	if msgs[1]["role"] != "assistant" {
		t.Fatalf("сообщение 1: роль %v, want assistant", msgs[1]["role"])
	}
	var use map[string]any
	for _, b := range contentBlocks(t, msgs[1]) {
		if b["type"] == "text" && b["text"] == "" {
			t.Errorf("пустой text-блок: Anthropic отвергает такие, content: null должен дать только tool_use")
		}
		if b["type"] == "tool_use" {
			use = b
		}
	}
	if use == nil || use["id"] != "call_1" || use["name"] != "get_metrics" {
		t.Fatalf("assistant.tool_calls не стали tool_use: %v", msgs[1]["content"])
	}
	input, ok := use["input"].(map[string]any)
	if !ok || input["service"] != "checkout" {
		t.Errorf("tool_use.input должен быть объектом, разобранным из arguments: %v", use["input"])
	}
	if msgs[2]["role"] != "user" {
		t.Fatalf("результат tool: роль %v, want user (в Anthropic tool_result лежит в сообщении user)", msgs[2]["role"])
	}
	blocks := contentBlocks(t, msgs[2])
	if len(blocks) != 1 || blocks[0]["type"] != "tool_result" || blocks[0]["tool_use_id"] != "call_1" ||
		!strings.Contains(mustJSON(t, blocks[0]["content"]), "p99=2300ms") {
		t.Errorf("сообщение роли tool не стало tool_result: %v", blocks)
	}
}

func TestToolsClaudeAssistantTextBeforeToolCallsIsKept(t *testing.T) {
	ta := newToolsAnthropic(t)
	ts := newToolsClaudeGateway(t, ta)
	assistant := strings.Replace(toolsAssistantCall, `"content":null`, `"content":"Let me check."`, 1)

	resp, body := post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg+","+assistant+","+toolsResultMsg))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	blocks := contentBlocks(t, anthropicMessages(t, ta)[1])
	if len(blocks) != 2 || blocks[0]["type"] != "text" || blocks[0]["text"] != "Let me check." || blocks[1]["type"] != "tool_use" {
		t.Errorf("ждём блоки [text, tool_use] в этом порядке: %v", blocks)
	}
}

func TestToolsClaudeParallelToolResultsShareOneUserMessage(t *testing.T) {
	ta := newToolsAnthropic(t)
	ts := newToolsClaudeGateway(t, ta)
	twoCalls := `{"role":"assistant","content":null,"tool_calls":[` +
		`{"id":"call_1","type":"function","function":{"name":"get_metrics","arguments":"{\"service\":\"checkout\"}"}},` +
		`{"id":"call_2","type":"function","function":{"name":"get_logs","arguments":"{\"service\":\"payments\"}"}}]}`
	results := toolsResultMsg + `,{"role":"tool","tool_call_id":"call_2","content":"5 errors"}`

	resp, body := post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg+","+twoCalls+","+results))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	msgs := anthropicMessages(t, ta)
	if len(msgs) != 3 {
		t.Fatalf("сообщений %d, want 3: все tool_result идут одним сообщением user: %v", len(msgs), msgs)
	}
	blocks := contentBlocks(t, msgs[2])
	if len(blocks) != 2 || blocks[0]["tool_use_id"] != "call_1" || blocks[1]["tool_use_id"] != "call_2" {
		t.Errorf("ждём tool_result для call_1 и call_2 по порядку: %v", blocks)
	}
	if uses := contentBlocks(t, msgs[1]); len(uses) != 2 || uses[0]["type"] != "tool_use" || uses[1]["type"] != "tool_use" {
		t.Errorf("ждём два tool_use в одном сообщении assistant: %v", uses)
	}
}

func TestToolsClaudeFullAgentLoopOverGateway(t *testing.T) {
	// Сквозной сценарий агента: ход 1 даёт вызовы, клиент берёт id из ответа гейтвея и возвращает
	// результаты; id из ответа обязан совпасть в tool_result второго запроса к Anthropic.
	ta := newToolsAnthropic(t)
	ts := newToolsClaudeGateway(t, ta)

	resp, body := post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg))
	first := decodeToolsCompletion(t, resp, body).Choices[0].Message
	if len(first.ToolCalls) == 0 {
		t.Fatalf("нет вызовов tools: %s", body)
	}
	callID := first.ToolCalls[0].ID

	assistant := `{"role":"assistant","content":null,"tool_calls":[{"id":"` + callID + `","type":"function",` +
		`"function":{"name":"get_metrics","arguments":` + mustJSON(t, first.ToolCalls[0].Function.Arguments) + `}}]}`
	result := `{"role":"tool","tool_call_id":"` + callID + `","content":"p99=2300ms"}`
	ta.reply = anthropicReply // второй ход: обычный текстовый ответ
	resp, body = post(t, ts, clientKey, claudeToolsRequest("", toolsUserMsg+","+assistant+","+result))

	final := decodeToolsCompletion(t, resp, body)
	if final.Choices[0].FinishReason != "stop" || final.Choices[0].Message.Content == nil || *final.Choices[0].Message.Content != "pong" {
		t.Errorf("второй ход должен дать итоговый текст: %s", body)
	}
	reqs := ta.received()
	if len(reqs) != 2 {
		t.Fatalf("Anthropic получил %d запросов, want 2", len(reqs))
	}
	msgs, _ := reqs[1]["messages"].([]any) //nolint:errcheck // проверка длиной
	if len(msgs) != 3 || !strings.Contains(mustJSON(t, msgs[2]), `"tool_use_id":"`+callID+`"`) {
		t.Errorf("tool_result со ссылкой на %s не дошёл до Anthropic: %v", callID, msgs)
	}
}
