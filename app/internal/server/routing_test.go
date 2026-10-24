package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
)

// Маршрутизация (#7): сложность задаёт клиент заголовком X-Complexity, поле model — алиас.
// Допущение тестов: «локальная» модель конфига это модель провайдера kind=openai, «Claude» это
// модель провайдера kind=claude. Новых полей конфига тесты не требуют (см. вопросы в отчёте).

const (
	localReply    = "local-reply" // ответ локального фейка; Claude-фейк отвечает "pong"
	claudeReply   = "pong"
	autoAlias     = "auto"
	complexityHdr = "X-Complexity"
)

type routingStand struct {
	*httptest.Server
	local  *fakeProvider
	claude *fakeAnthropic
}

// newRoutingGateway поднимает гейтвей с двумя моделями: local-small (локальный фейк) и
// claude-sonnet (Claude-фейк), и настоящим Redis. Лимит задаёт client.RateLimit.
func newRoutingGateway(t *testing.T, limit config.RateLimit) *routingStand {
	t.Helper()
	local := newFakeProvider(t, localReply)
	claude := newFakeAnthropic(t)
	cfg := config.Config{
		Redis: config.Redis{Addr: startRedis(t)},
		Providers: []config.Provider{
			{Name: "local", Kind: "openai", BaseURL: local.URL},
			{Name: "anthropic", Kind: "claude", BaseURL: claude.URL, APIKey: anthropicKey},
		},
		Models: []config.Model{
			{Name: "local-small", Provider: "local", ProviderModel: "qwen-local"},
			{Name: claudeAlias, Provider: "anthropic", ProviderModel: claudeModel},
		},
		Clients: []config.Client{{Name: "agent", Key: clientKey, RateLimit: limit}},
	}
	return &routingStand{Server: startGateway(t, cfg), local: local, claude: claude}
}

// postComplexity отправляет запрос с заголовком X-Complexity (пустое значение: заголовка нет).
func postComplexity(t *testing.T, ts *httptest.Server, complexity, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+clientKey)
	if complexity != "" {
		req.Header.Set(complexityHdr, complexity)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // тело читается целиком ниже
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, data
}

// wantRouted проверяет, что запрос обслужил ровно один фейк: локальный или Claude.
func (s *routingStand) wantRouted(t *testing.T, toLocal bool) {
	t.Helper()
	wantLocal, wantClaude := 0, 1
	if toLocal {
		wantLocal, wantClaude = 1, 0
	}
	if n := len(s.local.received()); n != wantLocal {
		t.Errorf("локальный фейк получил %d запросов, want %d", n, wantLocal)
	}
	if n := len(s.claude.received()); n != wantClaude {
		t.Errorf("Claude-фейк получил %d запросов, want %d", n, wantClaude)
	}
}

func wantOK(t *testing.T, resp *http.Response, body []byte, wantContent string) {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	if got := jsonContent(t, body); got != wantContent {
		t.Errorf("ответ %q, want %q: запрос обслужил не тот провайдер", got, wantContent)
	}
}

func TestMissingModelFieldMeansRoutingByComplexity(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "simple", `{"messages":[{"role":"user","content":"classify"}]}`)

	wantOK(t, resp, body, localReply)
	s.wantRouted(t, true)
}

func TestSimpleComplexityGoesToLocalModel(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "simple", chatBody(autoAlias, "classify", ""))

	wantOK(t, resp, body, localReply)
	s.wantRouted(t, true)
}

func TestComplexComplexityGoesToClaude(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "complex", chatBody(autoAlias, "analyse", ""))

	wantOK(t, resp, body, claudeReply)
	s.wantRouted(t, false)
}

func TestMissingComplexityMeansComplexAndGoesToClaude(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "", chatBody(autoAlias, "analyse", ""))

	wantOK(t, resp, body, claudeReply)
	s.wantRouted(t, false)
}

func TestEmptyModelEnablesRoutingLikeAuto(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "simple", chatBody("", "classify", ""))

	wantOK(t, resp, body, localReply)
	s.wantRouted(t, true)
}

func TestEmptyModelWithoutComplexityGoesToClaude(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "", chatBody("", "analyse", ""))

	wantOK(t, resp, body, claudeReply)
	s.wantRouted(t, false)
}

func TestPinnedLocalModelIgnoresComplexComplexity(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "complex", chatBody("local-small", "analyse", ""))

	wantOK(t, resp, body, localReply)
	s.wantRouted(t, true)
}

func TestPinnedClaudeModelIgnoresSimpleComplexity(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "simple", chatBody(claudeAlias, "classify", ""))

	wantOK(t, resp, body, claudeReply)
	s.wantRouted(t, false)
}

func TestUnknownComplexityValueGets400InOpenAIFormat(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})

	resp, body := postComplexity(t, s.Server, "medium", chatBody(autoAlias, "classify", ""))

	assertOpenAIError(t, resp, body, http.StatusBadRequest)
	if n := len(s.local.received()) + len(s.claude.received()); n != 0 {
		t.Errorf("провайдеры получили %d запросов, want 0: отклонённый запрос не должен до них доходить", n)
	}
}

func TestPinnedModelDoesNotBypassCache(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})
	body := chatBody("local-small", "classify", `"temperature":0`)

	postComplexity(t, s.Server, "", body)
	resp, got := postComplexity(t, s.Server, "", body)

	wantOK(t, resp, got, localReply)
	if n := len(s.local.received()); n != 1 {
		t.Errorf("локальный фейк получил %d запросов, want 1: второй идентичный запрос должен прийти из кэша", n)
	}
}

func TestPinnedModelDoesNotBypassRateLimit(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{Requests: requestLimit, Window: time.Minute})
	body := chatBody("local-small", "classify", "")

	for range requestLimit {
		if resp, got := postComplexity(t, s.Server, "", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("запрос в пределах лимита: status %d, want 200; body: %s", resp.StatusCode, got)
		}
	}
	resp, got := postComplexity(t, s.Server, "", body)

	assertOpenAIError(t, resp, got, http.StatusTooManyRequests)
}

func TestRoutedRequestIsCachedByResolvedModel(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})
	body := chatBody(autoAlias, "classify", `"temperature":0`)

	postComplexity(t, s.Server, "simple", body)
	postComplexity(t, s.Server, "simple", body)
	resp, got := postComplexity(t, s.Server, "complex", body)

	wantOK(t, resp, got, claudeReply)
	if n := len(s.local.received()); n != 1 {
		t.Errorf("локальный фейк получил %d запросов, want 1: повтор simple должен прийти из кэша", n)
	}
	if n := len(s.claude.received()); n != 1 {
		t.Errorf("Claude-фейк получил %d запросов, want 1: ответ локальной модели из кэша нельзя отдавать на complex", n)
	}
}

func TestStreamRequestIsRoutedLikePlainOne(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{})
	close(s.claude.release)

	req, err := http.NewRequest(http.MethodPost, s.URL+"/v1/chat/completions",
		strings.NewReader(chatBody(autoAlias, "analyse", `"stream":true`)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+clientKey)
	req.Header.Set(complexityHdr, "complex")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // тело читается ниже
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}

	if resp.StatusCode != http.StatusOK || !strings.Contains(string(data), "[DONE]") {
		t.Fatalf("status %d, want SSE с [DONE]; body: %s", resp.StatusCode, data)
	}
	s.wantRouted(t, false)
}

func TestRoutedRequestCountsAgainstRateLimit(t *testing.T) {
	s := newRoutingGateway(t, config.RateLimit{Requests: requestLimit, Window: time.Minute})
	body := chatBody(autoAlias, "classify", "")

	for range requestLimit {
		if resp, got := postComplexity(t, s.Server, "simple", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("запрос в пределах лимита: status %d, want 200; body: %s", resp.StatusCode, got)
		}
	}
	resp, got := postComplexity(t, s.Server, "simple", body)

	assertOpenAIError(t, resp, got, http.StatusTooManyRequests)
}
