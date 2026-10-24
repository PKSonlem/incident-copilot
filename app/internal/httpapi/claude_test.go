package httpapi_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/httpapi"
)

const (
	anthropicKey   = "anthropic-secret"
	claudeModel    = "claude-test-model"
	claudeAlias    = "claude-sonnet"
	pingMessages   = `"messages":[{"role":"user","content":"ping"}]`
	anthropicReply = `{"id":"msg_01","type":"message","role":"assistant","model":"claude-test-model",` +
		`"content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn","stop_sequence":null,` +
		`"usage":{"input_tokens":7,"output_tokens":3}}`
)

// fakeAnthropic — фейковый Anthropic Messages API. Обычный ответ отдаётся сразу; в потоковом режиме
// первая дельта уходит сразу, остальное после release (или событие error, если failMidStream).
type fakeAnthropic struct {
	*httptest.Server
	mu    sync.Mutex
	seen  []anthropicRequest
	reply string // тело обычного ответа

	status  int // не 0: ответить этим статусом и телом errBody
	errBody string

	release       chan struct{}
	cancelled     chan struct{}
	failMidStream bool
}

type anthropicRequest struct {
	Path   string
	Header http.Header
	Body   map[string]any
}

func sseEvent(name, data string) string { return "event: " + name + "\ndata: " + data + "\n\n" }

func newFakeAnthropic(t *testing.T) *fakeAnthropic {
	t.Helper()
	fa := &fakeAnthropic{reply: anthropicReply, release: make(chan struct{}), cancelled: make(chan struct{})}
	fa.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fa.mu.Lock()
		fa.seen = append(fa.seen, anthropicRequest{r.URL.Path, r.Header.Clone(), body})
		fa.mu.Unlock()

		if fa.status != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(fa.status)
			_, _ = w.Write([]byte(fa.errBody)) //nolint:errcheck // фейк
			return
		}
		if body["stream"] != true {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fa.reply)) //nolint:errcheck // фейк
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("fake anthropic: ResponseWriter не поддерживает Flush")
			return
		}
		write := func(s string) {
			if _, err := w.Write([]byte(s)); err != nil {
				return // клиент ушёл
			}
			flusher.Flush()
		}
		write(sseEvent("message_start", `{"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant",`+
			`"model":"claude-test-model","content":[],"stop_reason":null,"usage":{"input_tokens":7,"output_tokens":1}}}`))
		write(sseEvent("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
		write(sseEvent("ping", `{"type":"ping"}`))
		write(sseEvent("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hel"}}`))
		select {
		case <-fa.release:
		case <-r.Context().Done():
			close(fa.cancelled)
			return
		}
		if fa.failMidStream {
			write(sseEvent("error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`))
			return
		}
		write(sseEvent("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"lo"}}`))
		write(sseEvent("content_block_stop", `{"type":"content_block_stop","index":0}`))
		write(sseEvent("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":3}}`))
		write(sseEvent("message_stop", `{"type":"message_stop"}`))
	}))
	t.Cleanup(fa.Close)
	return fa
}

func (fa *fakeAnthropic) received() []anthropicRequest {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	return append([]anthropicRequest(nil), fa.seen...)
}

func newClaudeGateway(t *testing.T, fa *fakeAnthropic) *httptest.Server {
	t.Helper()
	cfg := config.Config{
		Redis:     config.Redis{Addr: startRedis(t)},
		Providers: []config.Provider{{Name: "anthropic", Kind: "claude", BaseURL: fa.URL, APIKey: anthropicKey}},
		Models:    []config.Model{{Name: claudeAlias, Provider: "anthropic", ProviderModel: claudeModel}},
		Clients:   []config.Client{{Name: "agent", Key: clientKey}},
	}
	h, err := httpapi.New(cfg)
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func claudeRequest(body string) string {
	return `{"model":"` + claudeAlias + `",` + body + `}`
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// --- обычный режим ---

func TestClaudePlainRequestIsTranslatedToAnthropicFormat(t *testing.T) {
	fa := newFakeAnthropic(t)
	ts := newClaudeGateway(t, fa)

	resp, body := post(t, ts, clientKey, claudeRequest(`"temperature":0.2,"max_tokens":50,`+
		`"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"ping"},`+
		`{"role":"assistant","content":"pong"},{"role":"user","content":[{"type":"text","text":"again"}]}]`))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	reqs := fa.received()
	if len(reqs) != 1 {
		t.Fatalf("Anthropic получил %d запросов, want 1", len(reqs))
	}
	got := reqs[0]
	if got.Path != "/v1/messages" {
		t.Errorf("путь %q, want /v1/messages", got.Path)
	}
	if got.Header.Get("X-Api-Key") != anthropicKey {
		t.Errorf("x-api-key %q: ключ провайдера должен подставляться", got.Header.Get("X-Api-Key"))
	}
	if got.Header.Get("Anthropic-Version") == "" {
		t.Error("нет обязательного заголовка anthropic-version")
	}
	if auth := got.Header.Get("Authorization"); auth != "" {
		t.Errorf("Authorization %q не должен уходить в Anthropic (ключ клиента не должен утекать)", auth)
	}
	if got.Body["model"] != claudeModel {
		t.Errorf("модель %v, want %s (имя у провайдера)", got.Body["model"], claudeModel)
	}
	if got.Body["system"] != "be brief" {
		t.Errorf("system %v: системное сообщение должно уйти в верхнеуровневое поле system", got.Body["system"])
	}
	if got.Body["max_tokens"] != float64(50) || got.Body["temperature"] != 0.2 {
		t.Errorf("параметры генерации потеряны: max_tokens=%v temperature=%v", got.Body["max_tokens"], got.Body["temperature"])
	}
	msgs, _ := got.Body["messages"].([]any) //nolint:errcheck // длина проверяется ниже
	wantRoles := []string{"user", "assistant", "user"}
	if len(msgs) != len(wantRoles) {
		t.Fatalf("сообщений %d, want %d (system не входит в messages): %v", len(msgs), len(wantRoles), msgs)
	}
	for i, role := range wantRoles {
		m, _ := msgs[i].(map[string]any) //nolint:errcheck // проверяется сравнением
		if m["role"] != role {
			t.Errorf("сообщение %d: роль %v, want %s", i, m["role"], role)
		}
	}
	last, _ := msgs[2].(map[string]any) //nolint:errcheck // проверяется ниже
	if !strings.Contains(mustJSON(t, last["content"]), "again") {
		t.Errorf("текстовые части потеряны: %v", last["content"])
	}
}

func TestClaudeRequestWithoutMaxTokensGetsDefault(t *testing.T) {
	fa := newFakeAnthropic(t)
	ts := newClaudeGateway(t, fa)

	resp, body := post(t, ts, clientKey, claudeRequest(pingMessages))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	reqs := fa.received()
	if len(reqs) != 1 {
		t.Fatalf("Anthropic получил %d запросов, want 1", len(reqs))
	}
	if n, ok := reqs[0].Body["max_tokens"].(float64); !ok || n <= 0 {
		t.Errorf("max_tokens %v: Anthropic требует его в каждом запросе, гейтвей должен подставить значение", reqs[0].Body["max_tokens"])
	}
}

func TestClaudePlainResponseIsReturnedInOpenAIFormatWithUsage(t *testing.T) {
	fa := newFakeAnthropic(t)
	ts := newClaudeGateway(t, fa)

	resp, body := post(t, ts, clientKey, claudeRequest(pingMessages))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	var got struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("ответ не JSON: %v; body: %s", err, body)
	}
	if got.ID == "" || got.Object != "chat.completion" {
		t.Errorf("id=%q object=%q: ждём OpenAI chat.completion", got.ID, got.Object)
	}
	if len(got.Choices) != 1 || got.Choices[0].Message.Role != "assistant" || got.Choices[0].Message.Content != "pong" {
		t.Fatalf("ответ Claude не переведён в choices[0].message: %s", body)
	}
	if got.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason %q, want stop (stop_reason end_turn)", got.Choices[0].FinishReason)
	}
	if got.Usage.PromptTokens != 7 || got.Usage.CompletionTokens != 3 || got.Usage.TotalTokens != 10 {
		t.Errorf("usage %+v, want prompt=7 completion=3 total=10", got.Usage)
	}
}

func TestClaudeStopReasonsMapToOpenAIFinishReasons(t *testing.T) {
	for stopReason, want := range map[string]string{"end_turn": "stop", "stop_sequence": "stop", "max_tokens": "length"} {
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

// --- потоковый режим ---

type openAIChunk struct {
	Object  string `json:"object"`
	Choices []struct {
		Delta struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

func openClaudeStream(ctx context.Context, t *testing.T, ts *httptest.Server, extra string) *http.Response {
	t.Helper()
	body := claudeRequest(`"stream":true,` + extra + pingMessages)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+clientKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST stream: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() }) //nolint:errcheck // закрытие тела в cleanup
	return resp
}

// nextChunk читает следующее data-событие и разбирает его как чанк; payload возвращается как есть.
func nextChunk(t *testing.T, sc *bufio.Scanner) (openAIChunk, string) {
	t.Helper()
	payload := nextData(t, sc)
	var c openAIChunk
	if payload != "[DONE]" {
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			t.Fatalf("событие не JSON: %v; %q", err, payload)
		}
	}
	return c, payload
}

func TestClaudeStreamIsTranslatedToOpenAISSE(t *testing.T) {
	fa := newFakeAnthropic(t)
	close(fa.release)
	ts := newClaudeGateway(t, fa)

	resp := openClaudeStream(context.Background(), t, ts, `"stream_options":{"include_usage":true},`)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type %q, want text/event-stream", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	var content strings.Builder
	var finish string
	var usage *openAIChunk
	sawRole := false
	for {
		c, payload := nextChunk(t, sc)
		if payload == "[DONE]" {
			break
		}
		if c.Object != "chat.completion.chunk" {
			t.Errorf("object %q, want chat.completion.chunk: %s", c.Object, payload)
		}
		for _, ch := range c.Choices {
			if ch.Delta.Role == "assistant" {
				sawRole = true
			}
			content.WriteString(ch.Delta.Content)
			if ch.FinishReason != nil {
				finish = *ch.FinishReason
			}
		}
		if c.Usage != nil {
			usage = &c
		}
	}
	if !sawRole {
		t.Error("ни один чанк не объявил роль assistant")
	}
	if content.String() != "hello" {
		t.Errorf("склеенный текст %q, want hello", content.String())
	}
	if finish != "stop" {
		t.Errorf("finish_reason %q, want stop", finish)
	}
	if usage == nil || usage.Usage.PromptTokens != 7 || usage.Usage.CompletionTokens != 3 || usage.Usage.TotalTokens != 10 {
		t.Errorf("при include_usage ждём чанк с usage prompt=7 completion=3 total=10, got %+v", usage)
	}
	reqs := fa.received()
	if len(reqs) != 1 || reqs[0].Body["stream"] != true || reqs[0].Path != "/v1/messages" {
		t.Errorf("Anthropic получил %+v: ждём один запрос /v1/messages со stream=true", reqs)
	}
}

func TestClaudeStreamChunksAreForwardedBeforeProviderFinishes(t *testing.T) {
	fa := newFakeAnthropic(t) // release не закрыт: провайдер держит остаток потока
	ts := newClaudeGateway(t, fa)

	resp := openClaudeStream(context.Background(), t, ts, "")
	sc := bufio.NewScanner(resp.Body)

	var first string
	for first == "" {
		c, payload := nextChunk(t, sc)
		if payload == "[DONE]" {
			t.Fatal("поток закончился до первой дельты")
		}
		for _, ch := range c.Choices {
			first += ch.Delta.Content
		}
	}
	if first != "hel" {
		t.Errorf("первая дельта %q, want hel", first)
	}
	close(fa.release)
}

func TestClaudeClientCancelReachesProvider(t *testing.T) {
	fa := newFakeAnthropic(t)
	ts := newClaudeGateway(t, fa)

	ctx, cancel := context.WithCancel(context.Background())
	resp := openClaudeStream(ctx, t, ts, "")
	nextData(t, bufio.NewScanner(resp.Body)) // поток пошёл
	cancel()

	select {
	case <-fa.cancelled:
	case <-time.After(waitLimit):
		t.Fatal("запрос к Anthropic не отменён после отключения клиента")
	}
}

func TestClaudeMidStreamErrorEndsWithOpenAIErrorEvent(t *testing.T) {
	fa := newFakeAnthropic(t)
	fa.failMidStream = true
	close(fa.release)
	ts := newClaudeGateway(t, fa)

	resp := openClaudeStream(context.Background(), t, ts, "")
	sc := bufio.NewScanner(resp.Body)

	for {
		payload := nextData(t, sc) // если события ошибки нет, nextData упадёт на конце потока
		var e struct {
			Error *struct {
				Message string `json:"message"`
				Type    string `json:"type"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(payload), &e); err != nil {
			continue
		}
		if e.Error != nil {
			if e.Error.Message == "" || e.Error.Type == "" {
				t.Errorf("ошибка потока не в OpenAI-формате: %s", payload)
			}
			return
		}
	}
}

// --- ошибки ---

func TestClaudeErrorsAreTranslatedToOpenAIFormat(t *testing.T) {
	anthropicErr := func(typ, msg string) string {
		return `{"type":"error","error":{"type":"` + typ + `","message":"` + msg + `"}}`
	}
	cases := []struct {
		name       string
		status     int
		body       string
		wantStatus int
		wantMsg    string // подстрока сообщения Claude, которая должна дойти до клиента
	}{
		{"неверный запрос", 400, anthropicErr("invalid_request_error", "text content blocks must be non-empty"), 400, "text content blocks must be non-empty"},
		{"лимит провайдера", 429, anthropicErr("rate_limit_error", "slow down"), 429, ""},
		{"ключ гейтвея у Anthropic отвергнут", 401, anthropicErr("authentication_error", "invalid x-api-key"), 502, ""},
		{"внутренняя ошибка", 500, anthropicErr("api_error", "boom"), 502, ""},
		{"перегрузка 529", 529, anthropicErr("overloaded_error", "Overloaded"), 502, ""},
		{"не JSON в теле ошибки", 500, "upstream exploded", 502, ""},
	}
	for _, tc := range cases {
		for _, stream := range []bool{false, true} {
			name := tc.name
			reqBody := claudeRequest(pingMessages)
			if stream {
				name += " (stream)"
				reqBody = claudeRequest(`"stream":true,` + pingMessages)
			}
			t.Run(name, func(t *testing.T) {
				fa := newFakeAnthropic(t)
				fa.status, fa.errBody = tc.status, tc.body
				ts := newClaudeGateway(t, fa)

				resp, data := post(t, ts, clientKey, reqBody)

				assertOpenAIError(t, resp, data, tc.wantStatus)
				if tc.wantMsg != "" && !strings.Contains(string(data), tc.wantMsg) {
					t.Errorf("сообщение Claude %q не дошло до клиента: %s", tc.wantMsg, data)
				}
				if strings.Contains(string(data), `"type":"error"`) {
					t.Errorf("ответ остался в формате Anthropic: %s", data)
				}
			})
		}
	}
}

func TestClaudeErrorDoesNotLeakProviderKey(t *testing.T) {
	fa := newFakeAnthropic(t)
	fa.status = 401
	fa.errBody = `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key ` + anthropicKey + `"}}`
	ts := newClaudeGateway(t, fa)

	_, data := post(t, ts, clientKey, claudeRequest(pingMessages))

	if strings.Contains(string(data), anthropicKey) {
		t.Errorf("ключ провайдера в ответе клиенту: %s", data)
	}
}
