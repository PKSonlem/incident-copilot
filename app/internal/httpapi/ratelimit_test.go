package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/httpapi"
)

const (
	otherKey     = "other-secret"
	limitWindow  = time.Minute
	chatReqBody  = `{"model":"local-small","messages":[{"role":"user","content":"ping"}]}`
	requestLimit = 2
)

// limitedConfig — два клиента с лимитом requestLimit запросов в limitWindow и общий Redis.
func limitedConfig(fp *fakeProvider, redisAddr string) config.Config {
	limit := config.RateLimit{Requests: requestLimit, Window: limitWindow}
	return config.Config{
		Redis:     config.Redis{Addr: redisAddr},
		Providers: []config.Provider{{Name: "local", Kind: "openai", BaseURL: fp.URL}},
		Models:    []config.Model{{Name: "local-small", Provider: "local", ProviderModel: "qwen-local"}},
		Clients: []config.Client{
			{Name: "agent", Key: clientKey, RateLimit: limit},
			{Name: "other", Key: otherKey, RateLimit: limit},
		},
	}
}

func startGateway(t *testing.T, cfg config.Config) *httptest.Server {
	t.Helper()
	h, err := httpapi.New(cfg)
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func mustPass(t *testing.T, ts *httptest.Server, key string) {
	t.Helper()
	if resp, body := post(t, ts, key, chatReqBody); resp.StatusCode != http.StatusOK {
		t.Fatalf("запрос в пределах лимита: status %d, want 200; body: %s", resp.StatusCode, body)
	}
}

func TestRequestOverLimitGets429WithRetryAfter(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := startGateway(t, limitedConfig(fp, startRedis(t)))

	for range requestLimit {
		mustPass(t, ts, clientKey)
	}
	resp, body := post(t, ts, clientKey, chatReqBody)

	assertOpenAIError(t, resp, body, http.StatusTooManyRequests)
	seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || seconds < 1 || seconds > int(limitWindow.Seconds()) {
		t.Errorf("Retry-After %q: ждём целое число секунд от 1 до %d", resp.Header.Get("Retry-After"), int(limitWindow.Seconds()))
	}
	if n := len(fp.received()); n != requestLimit {
		t.Errorf("провайдер получил %d запросов, want %d: отклонённый по лимиту запрос не должен до него доходить", n, requestLimit)
	}
}

func TestLimitsOfDifferentKeysAreIndependent(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	ts := startGateway(t, limitedConfig(fp, startRedis(t)))

	for range requestLimit {
		mustPass(t, ts, clientKey)
	}
	if resp, body := post(t, ts, clientKey, chatReqBody); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("первый ключ исчерпан: status %d, want 429; body: %s", resp.StatusCode, body)
	}

	mustPass(t, ts, otherKey)
}

func TestTwoGatewayInstancesShareOneLimit(t *testing.T) {
	fp := newFakeProvider(t, "pong")
	redisAddr := startRedis(t)
	first := startGateway(t, limitedConfig(fp, redisAddr))
	second := startGateway(t, limitedConfig(fp, redisAddr))

	mustPass(t, first, clientKey)
	mustPass(t, second, clientKey)

	for name, ts := range map[string]*httptest.Server{"первый": first, "второй": second} {
		resp, body := post(t, ts, clientKey, chatReqBody)
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Errorf("%s экземпляр: status %d, want 429 (лимит общий через Redis); body: %s", name, resp.StatusCode, body)
		}
	}
}
