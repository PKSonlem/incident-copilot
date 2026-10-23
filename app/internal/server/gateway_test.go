package server_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/server"
)

const (
	clientKey   = "client-secret"
	providerKey = "provider-secret"
)

// fakeProvider — фейковый OpenAI-совместимый сервер, который подменяет llama-swap.
type fakeProvider struct {
	*httptest.Server
	status   int // 0 означает 200
	mu       sync.Mutex
	requests []recordedRequest
}

type recordedRequest struct {
	Path          string
	Authorization string
	Body          map[string]any
}

func newFakeProvider(t *testing.T, reply string) *fakeProvider {
	t.Helper()
	fp := &fakeProvider{}
	fp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fp.mu.Lock()
		fp.requests = append(fp.requests, recordedRequest{r.URL.Path, r.Header.Get("Authorization"), body})
		fp.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if fp.status != 0 {
			w.WriteHeader(fp.status)
		}
		if _, err := w.Write([]byte(`{"id":"chatcmpl-fake","object":"chat.completion","model":"qwen-local",` +
			`"choices":[{"index":0,"message":{"role":"assistant","content":"` + reply + `"},"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`)); err != nil {
			t.Errorf("fake provider write: %v", err)
		}
	}))
	t.Cleanup(fp.Close)
	return fp
}

func (fp *fakeProvider) received() []recordedRequest {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return append([]recordedRequest(nil), fp.requests...)
}

// startRedis поднимает настоящий redis-server на свободном порту на время теста.
func startRedis(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("redis-server")
	if err != nil {
		t.Fatalf("redis-server не найден в PATH: тесты гейтвея требуют настоящий Redis: %v", err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := l.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	cmd := exec.Command(path, "--port", port, "--bind", "127.0.0.1", "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() {
		if err := cmd.Process.Kill(); err != nil {
			t.Logf("kill redis: %v", err)
		}
		_ = cmd.Wait() //nolint:errcheck // код выхода убитого процесса не важен
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if redisPing(addr) {
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("redis на %s не ответил на PING", addr)
	return ""
}

func redisPing(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	defer func() { _ = c.Close() }() //nolint:errcheck // закрытие пробного соединения
	if _, err := c.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return false
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	return err == nil && strings.HasPrefix(line, "+PONG")
}

// newGateway собирает гейтвей с одним фейковым провайдером и настоящим Redis.
func newGateway(t *testing.T, fp *fakeProvider) *httptest.Server {
	t.Helper()
	cfg := config.Config{
		Redis:     config.Redis{Addr: startRedis(t)},
		Providers: []config.Provider{{Name: "local", Kind: "openai", BaseURL: fp.URL, APIKey: providerKey}},
		Models:    []config.Model{{Name: "local-small", Provider: "local", ProviderModel: "qwen-local"}},
		Clients:   []config.Client{{Name: "agent", Key: clientKey}},
	}
	h, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func post(t *testing.T, ts *httptest.Server, key, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // тело читается целиком выше
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, data
}

// assertOpenAIError проверяет статус и формат {"error":{"message","type"}}.
func assertOpenAIError(t *testing.T, resp *http.Response, body []byte, wantStatus int) {
	t.Helper()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status %d, want %d; body: %s", resp.StatusCode, wantStatus, body)
	}
	var e struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("тело ошибки не JSON: %v; body: %s", err, body)
	}
	var keys struct {
		Error map[string]json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &keys); err == nil {
		for _, k := range []string{"param", "code"} {
			if _, ok := keys.Error[k]; !ok {
				t.Errorf("в ошибке нет поля %q (OpenAI-формат): %s", k, body)
			}
		}
	}
	if e.Error.Message == "" || e.Error.Type == "" {
		t.Errorf("ошибка не в OpenAI-формате (нужны error.message и error.type): %s", body)
	}
}

func TestChatCompletionReachesProviderAndReturnsItsAnswer(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)

	resp, body := post(t, ts, clientKey,
		`{"model":"local-small","messages":[{"role":"user","content":"ping"}]}`)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	var got struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("ответ не JSON: %v", err)
	}
	if len(got.Choices) != 1 || got.Choices[0].Message.Content != "pong" {
		t.Errorf("ответ провайдера не дошёл до клиента: %s", body)
	}
	if got.Usage.PromptTokens != 7 {
		t.Errorf("usage провайдера потерян: %s", body)
	}

	reqs := fp.received()
	if len(reqs) != 1 {
		t.Fatalf("провайдер получил %d запросов, want 1", len(reqs))
	}
	if reqs[0].Path != "/v1/chat/completions" {
		t.Errorf("путь к провайдеру %q", reqs[0].Path)
	}
	if reqs[0].Body["model"] != "qwen-local" {
		t.Errorf("провайдеру ушла модель %v, want qwen-local (имя у провайдера)", reqs[0].Body["model"])
	}
	if reqs[0].Authorization != "Bearer "+providerKey {
		t.Errorf("провайдер получил Authorization %q: ключ клиента не должен утекать, ключ провайдера должен подставляться", reqs[0].Authorization)
	}
}

func TestRequestWithoutValidAPIKeyIsRejected(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)
	const reqBody = `{"model":"local-small","messages":[{"role":"user","content":"ping"}]}`

	for name, key := range map[string]string{"нет ключа": "", "чужой ключ": "wrong"} {
		t.Run(name, func(t *testing.T) {
			resp, body := post(t, ts, key, reqBody)
			assertOpenAIError(t, resp, body, http.StatusUnauthorized)
		})
	}
	if n := len(fp.received()); n != 0 {
		t.Errorf("отклонённый запрос дошёл до провайдера (%d)", n)
	}
}

func TestUnsupportedContentTypesAreRejectedWith400(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)

	cases := map[string]string{
		"изображение": `{"model":"local-small","messages":[{"role":"user","content":[` +
			`{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}]}`,
		"аудио": `{"model":"local-small","messages":[{"role":"user","content":[` +
			`{"type":"input_audio","input_audio":{"data":"AAAA","format":"wav"}}]}]}`,
	}
	for name, reqBody := range cases {
		t.Run(name, func(t *testing.T) {
			resp, body := post(t, ts, clientKey, reqBody)
			assertOpenAIError(t, resp, body, http.StatusBadRequest)
		})
	}
	if n := len(fp.received()); n != 0 {
		t.Errorf("запрос с неподдерживаемым контентом дошёл до провайдера (%d)", n)
	}
}

func TestMalformedContentShapesAreRejectedWith400(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)

	for name, content := range map[string]string{
		"объект": `{"type":"image_url"}`,
		"число":  `42`,
	} {
		t.Run(name, func(t *testing.T) {
			resp, body := post(t, ts, clientKey,
				`{"model":"local-small","messages":[{"role":"user","content":`+content+`}]}`)
			assertOpenAIError(t, resp, body, http.StatusBadRequest)
		})
	}
}

func TestStreamingRequestIsRejectedUntilSupported(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)

	resp, body := post(t, ts, clientKey,
		`{"model":"local-small","stream":true,"messages":[{"role":"user","content":"ping"}]}`)

	assertOpenAIError(t, resp, body, http.StatusBadRequest)
	if n := len(fp.received()); n != 0 {
		t.Errorf("потоковый запрос дошёл до провайдера (%d)", n)
	}
}

func TestProviderClientErrorIsPassedThrough(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	fp.status = http.StatusTooManyRequests
	ts := newGateway(t, fp)

	resp, _ := post(t, ts, clientKey,
		`{"model":"local-small","messages":[{"role":"user","content":"ping"}]}`)

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status %d, want 429 от провайдера", resp.StatusCode)
	}
}

func TestProviderServerErrorBecomes502(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	fp.status = http.StatusInternalServerError
	ts := newGateway(t, fp)

	resp, body := post(t, ts, clientKey,
		`{"model":"local-small","messages":[{"role":"user","content":"ping"}]}`)

	assertOpenAIError(t, resp, body, http.StatusBadGateway)
}

func TestTextContentPartsAreAccepted(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)

	resp, body := post(t, ts, clientKey,
		`{"model":"local-small","messages":[{"role":"user","content":[{"type":"text","text":"ping"}]}]}`)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
}

func TestUnknownModelIsRejectedWithOpenAIError(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)

	resp, body := post(t, ts, clientKey,
		`{"model":"no-such-model","messages":[{"role":"user","content":"ping"}]}`)

	assertOpenAIError(t, resp, body, http.StatusNotFound)
}

func TestMalformedJSONIsRejectedWith400(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := newGateway(t, fp)

	resp, body := post(t, ts, clientKey, `{not json`)

	assertOpenAIError(t, resp, body, http.StatusBadRequest)
}

func writeConfig(t *testing.T, yaml string) string {
	t.Helper()
	path := t.TempDir() + "/gateway.yaml"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestConfigSecretsAreReadOnlyFromEnvironment(t *testing.T) {
	const file = `
redis:
  addr: localhost:6379
providers:
  - name: local
    kind: openai
    base_url: http://localhost:7382
    api_key_env: LOCAL_PROVIDER_KEY
models:
  - name: local-small
    provider: local
    provider_model: qwen-local
clients:
  - name: agent
    key_env: AGENT_CLIENT_KEY
`
	env := map[string]string{"LOCAL_PROVIDER_KEY": "from-env-provider", "AGENT_CLIENT_KEY": "from-env-client"}
	cfg, err := config.Load(writeConfig(t, file), func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Providers[0].APIKey; got != "from-env-provider" {
		t.Errorf("ключ провайдера %q, want значение из окружения", got)
	}
	if got := cfg.Clients[0].Key; got != "from-env-client" {
		t.Errorf("ключ клиента %q, want значение из окружения", got)
	}

	t.Run("литерал секрета в файле отвергается", func(t *testing.T) {
		leaked := strings.Replace(file, "api_key_env: LOCAL_PROVIDER_KEY", "api_key: sk-literal", 1)
		if _, err := config.Load(writeConfig(t, leaked), func(k string) string { return env[k] }); err == nil {
			t.Error("Load принял секрет, записанный в файле; ожидалась ошибка")
		}
	})

	t.Run("пустая переменная окружения даёт ошибку", func(t *testing.T) {
		if _, err := config.Load(writeConfig(t, file), func(string) string { return "" }); err == nil {
			t.Error("Load принял пустые секреты; ожидалась ошибка")
		}
	})
}
