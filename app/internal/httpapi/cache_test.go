package httpapi_test

import (
	"bufio"
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

// Признак попадания в кэш: заголовок ответа X-Cache со значением HIT (при промахе MISS).
const (
	cacheHeader = "X-Cache"
	cacheHit    = "HIT"
)

// dualProvider — фейковый провайдер, который отвечает JSON или SSE в зависимости от поля stream
// и считает полученные запросы. Ответ всегда "hello" (в потоке — двумя чанками "hel" и "lo").
type dualProvider struct {
	*httptest.Server
	mu       sync.Mutex
	requests []map[string]any
	status   int // 0 означает 200
}

func newDualProvider(t *testing.T) *dualProvider {
	t.Helper()
	dp := &dualProvider{}
	dp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		dp.mu.Lock()
		dp.requests = append(dp.requests, body)
		status := dp.status
		dp.mu.Unlock()
		write := func(s string) {
			if _, err := w.Write([]byte(s)); err != nil {
				t.Errorf("fake provider write: %v", err)
			}
		}
		if status != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			write(`{"error":{"message":"boom","type":"api_error","param":null,"code":null}}`)
			return
		}
		if isStream, _ := body["stream"].(bool); isStream { //nolint:errcheck // отсутствие поля означает false
			w.Header().Set("Content-Type", "text/event-stream")
			for _, chunk := range []string{
				`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hel"}}]}`,
				`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":"stop"}]}`,
				`[DONE]`,
			} {
				write("data: " + chunk + "\n\n")
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		write(`{"id":"chatcmpl-fake","object":"chat.completion","model":"qwen-local",` +
			`"choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`)
	}))
	t.Cleanup(dp.Close)
	return dp
}

func (dp *dualProvider) count() int {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return len(dp.requests)
}

func (dp *dualProvider) setStatus(s int) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.status = s
}

// newCacheGateway собирает гейтвей с двумя именами моделей на одном фейке и заданным TTL кэша.
func newCacheGateway(t *testing.T, dp *dualProvider, ttl time.Duration) *httptest.Server {
	t.Helper()
	cfg := config.Config{
		Redis:     config.Redis{Addr: startRedis(t)},
		Cache:     config.Cache{TTL: ttl},
		Providers: []config.Provider{{Name: "local", Kind: "openai", BaseURL: dp.URL}},
		Models: []config.Model{
			{Name: "local-small", Provider: "local", ProviderModel: "qwen-local"},
			{Name: "local-big", Provider: "local", ProviderModel: "qwen-local-big"},
		},
		Clients: []config.Client{{Name: "agent", Key: clientKey}},
	}
	h, err := httpapi.New(cfg)
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

// chatBody собирает тело запроса; extra — дополнительные поля JSON через запятую (без внешних скобок).
func chatBody(model, prompt, extra string) string {
	s := `{"model":"` + model + `","messages":[{"role":"user","content":"` + prompt + `"}]`
	if extra != "" {
		s += "," + extra
	}
	return s + "}"
}

// sseContent читает поток SSE до [DONE] и возвращает склеенное содержимое delta.content.
func sseContent(t *testing.T, resp *http.Response) string {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type %q, want text/event-stream", ct)
	}
	var out strings.Builder
	sc := bufio.NewScanner(resp.Body)
	done := false
	for sc.Scan() {
		payload, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		if payload == "[DONE]" {
			done = true
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			t.Fatalf("событие потока не JSON: %v: %s", err, payload)
		}
		for _, c := range chunk.Choices {
			out.WriteString(c.Delta.Content)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("чтение потока: %v", err)
	}
	if !done {
		t.Fatal("поток закончился без [DONE]")
	}
	return out.String()
}

func jsonContent(t *testing.T, body []byte) string {
	t.Helper()
	var got struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &got); err != nil || len(got.Choices) != 1 {
		t.Fatalf("ответ не chat.completion: %v; body: %s", err, body)
	}
	return got.Choices[0].Message.Content
}

// postStream отправляет потоковый запрос и возвращает ответ с открытым телом.
func postStream(t *testing.T, ts *httptest.Server, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(body))
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

func TestRepeatedIdenticalRequestIsServedFromCache(t *testing.T) {
	dp := newDualProvider(t)
	ts := newCacheGateway(t, dp, time.Minute)
	reqBody := chatBody("local-small", "ping", `"temperature":0`)

	first, firstBody := post(t, ts, clientKey, reqBody)
	second, secondBody := post(t, ts, clientKey, reqBody)

	if first.StatusCode != http.StatusOK || second.StatusCode != http.StatusOK {
		t.Fatalf("статусы %d и %d, want 200", first.StatusCode, second.StatusCode)
	}
	if got := first.Header.Get(cacheHeader); got == cacheHit {
		t.Errorf("первый запрос помечен %s: кэш был пуст", cacheHit)
	}
	if got := second.Header.Get(cacheHeader); got != cacheHit {
		t.Errorf("%s второго запроса %q, want %q", cacheHeader, got, cacheHit)
	}
	if n := dp.count(); n != 1 {
		t.Errorf("провайдер получил %d запросов, want 1: второй запрос должен прийти из кэша", n)
	}
	if jsonContent(t, secondBody) != "hello" || jsonContent(t, firstBody) != "hello" {
		t.Errorf("ответ из кэша отличается от ответа провайдера: %s / %s", firstBody, secondBody)
	}
}

func TestChangedModelParametersOrMessagesMissTheCache(t *testing.T) {
	base := chatBody("local-small", "ping", `"temperature":0,"max_tokens":50`)
	cases := map[string]string{
		"другая модель":       chatBody("local-big", "ping", `"temperature":0,"max_tokens":50`),
		"другой параметр":     chatBody("local-small", "ping", `"temperature":0,"max_tokens":51`),
		"добавлен параметр":   chatBody("local-small", "ping", `"temperature":0,"max_tokens":50,"top_p":0.5`),
		"другие сообщения":    chatBody("local-small", "pong", `"temperature":0,"max_tokens":50`),
		"добавлено сообщение": `{"model":"local-small","temperature":0,"max_tokens":50,"messages":[{"role":"user","content":"ping"},{"role":"user","content":"ping"}]}`,
	}
	for name, changed := range cases {
		t.Run(name, func(t *testing.T) {
			dp := newDualProvider(t)
			ts := newCacheGateway(t, dp, time.Minute)

			post(t, ts, clientKey, base)
			resp, _ := post(t, ts, clientKey, changed)

			if got := resp.Header.Get(cacheHeader); got == cacheHit {
				t.Errorf("запрос с изменением (%s) помечен как cache hit", name)
			}
			if n := dp.count(); n != 2 {
				t.Errorf("провайдер получил %d запросов, want 2", n)
			}
		})
	}
}

func TestEntryStoredByJSONRequestIsServedAsStream(t *testing.T) {
	dp := newDualProvider(t)
	ts := newCacheGateway(t, dp, time.Minute)

	post(t, ts, clientKey, chatBody("local-small", "ping", `"temperature":0`))
	resp := postStream(t, ts, chatBody("local-small", "ping", `"temperature":0,"stream":true`))

	if got := resp.Header.Get(cacheHeader); got != cacheHit {
		t.Errorf("%s %q, want %q", cacheHeader, got, cacheHit)
	}
	if got := sseContent(t, resp); got != "hello" {
		t.Errorf("содержимое потока из кэша %q, want hello", got)
	}
	if n := dp.count(); n != 1 {
		t.Errorf("провайдер получил %d запросов, want 1", n)
	}
}

func TestEntryStoredByStreamRequestIsServedAsJSON(t *testing.T) {
	dp := newDualProvider(t)
	ts := newCacheGateway(t, dp, time.Minute)

	first := postStream(t, ts, chatBody("local-small", "ping", `"temperature":0,"stream":true`))
	if got := sseContent(t, first); got != "hello" {
		t.Fatalf("содержимое потока %q, want hello", got)
	}
	resp, body := post(t, ts, clientKey, chatBody("local-small", "ping", `"temperature":0`))

	if got := resp.Header.Get(cacheHeader); got != cacheHit {
		t.Errorf("%s %q, want %q", cacheHeader, got, cacheHit)
	}
	if got := jsonContent(t, body); got != "hello" {
		t.Errorf("JSON из записи потока: содержимое %q, want hello; body: %s", got, body)
	}
	if n := dp.count(); n != 1 {
		t.Errorf("провайдер получил %d запросов, want 1", n)
	}
}

func TestEntryStoredByStreamRequestIsServedAsStream(t *testing.T) {
	dp := newDualProvider(t)
	ts := newCacheGateway(t, dp, time.Minute)
	reqBody := chatBody("local-small", "ping", `"temperature":0,"stream":true`)

	first := postStream(t, ts, reqBody)
	_ = sseContent(t, first)
	second := postStream(t, ts, reqBody)

	if got := second.Header.Get(cacheHeader); got != cacheHit {
		t.Errorf("%s %q, want %q", cacheHeader, got, cacheHit)
	}
	if got := sseContent(t, second); got != "hello" {
		t.Errorf("содержимое потока из кэша %q, want hello", got)
	}
	if n := dp.count(); n != 1 {
		t.Errorf("провайдер получил %d запросов, want 1", n)
	}
}

func TestNonZeroOrMissingTemperatureBypassesCache(t *testing.T) {
	cases := map[string]string{
		"temperature 0.7":       chatBody("local-small", "ping", `"temperature":0.7`),
		"temperature не задана": chatBody("local-small", "ping", ""),
	}
	for name, reqBody := range cases {
		t.Run(name, func(t *testing.T) {
			dp := newDualProvider(t)
			ts := newCacheGateway(t, dp, time.Minute)

			post(t, ts, clientKey, reqBody)
			resp, _ := post(t, ts, clientKey, reqBody)

			if got := resp.Header.Get(cacheHeader); got == cacheHit {
				t.Errorf("запрос (%s) помечен как cache hit", name)
			}
			if n := dp.count(); n != 2 {
				t.Errorf("провайдер получил %d запросов, want 2: кэш должен быть обойдён", n)
			}
		})
	}
}

func TestCacheEntryExpiresAfterTTL(t *testing.T) {
	dp := newDualProvider(t)
	ts := newCacheGateway(t, dp, time.Second)
	reqBody := chatBody("local-small", "ping", `"temperature":0`)

	post(t, ts, clientKey, reqBody)
	hit, _ := post(t, ts, clientKey, reqBody)
	if got := hit.Header.Get(cacheHeader); got != cacheHit {
		t.Fatalf("до истечения TTL %s %q, want %q", cacheHeader, got, cacheHit)
	}
	time.Sleep(1500 * time.Millisecond)
	expired, _ := post(t, ts, clientKey, reqBody)

	if got := expired.Header.Get(cacheHeader); got == cacheHit {
		t.Error("запись пережила TTL: запрос помечен как cache hit")
	}
	if n := dp.count(); n != 2 {
		t.Errorf("провайдер получил %d запросов, want 2 (первый и после TTL)", n)
	}
}

func TestProviderErrorIsNotCached(t *testing.T) {
	dp := newDualProvider(t)
	ts := newCacheGateway(t, dp, time.Minute)
	reqBody := chatBody("local-small", "ping", `"temperature":0`)

	dp.setStatus(http.StatusInternalServerError)
	failed, _ := post(t, ts, clientKey, reqBody)
	dp.setStatus(0)
	resp, body := post(t, ts, clientKey, reqBody)

	if failed.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", failed.StatusCode)
	}
	if resp.StatusCode != http.StatusOK || jsonContent(t, body) != "hello" {
		t.Errorf("после сбоя провайдера ожидался свежий ответ, got %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get(cacheHeader); got == cacheHit {
		t.Error("ошибка провайдера попала в кэш")
	}
}
