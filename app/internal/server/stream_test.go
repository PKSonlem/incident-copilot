package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/server"
)

const (
	sseChunk1 = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hel"}}]}`
	sseChunk2 = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":"stop"}]}`
	waitLimit = 5 * time.Second
)

// streamingProvider — фейковый провайдер с SSE. Первый чанк уходит сразу, второй и [DONE] только
// после release; если клиент ушёл раньше, закрывается cancelled.
type streamingProvider struct {
	*httptest.Server
	started   chan map[string]any
	release   chan struct{}
	cancelled chan struct{}
}

func newStreamingProvider(t *testing.T) *streamingProvider {
	t.Helper()
	sp := &streamingProvider{
		started:   make(chan map[string]any, 1),
		release:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
	sp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sp.started <- body
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("fake provider: ResponseWriter не поддерживает Flush")
			return
		}
		write := func(s string) {
			if _, err := w.Write([]byte(s)); err != nil {
				t.Errorf("fake provider write: %v", err)
			}
			flusher.Flush()
		}
		write("data: " + sseChunk1 + "\n\n")
		select {
		case <-sp.release:
		case <-r.Context().Done():
			close(sp.cancelled)
			return
		}
		write("data: " + sseChunk2 + "\n\n")
		write("data: [DONE]\n\n")
	}))
	t.Cleanup(sp.Close)
	return sp
}

func newStreamGateway(t *testing.T, sp *streamingProvider) *httptest.Server {
	t.Helper()
	cfg := config.Config{
		Redis:     config.Redis{Addr: startRedis(t)},
		Providers: []config.Provider{{Name: "local", Kind: "openai", BaseURL: sp.URL}},
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

const streamReqBody = `{"model":"local-small","stream":true,"messages":[{"role":"user","content":"ping"}]}`

func openStream(ctx context.Context, t *testing.T, ts *httptest.Server) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(streamReqBody))
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

// nextData возвращает полезную нагрузку следующего события "data: ...".
func nextData(t *testing.T, sc *bufio.Scanner) string {
	t.Helper()
	for sc.Scan() {
		if payload, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			return payload
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("чтение потока: %v", err)
	}
	t.Fatal("поток закончился без ожидаемого события")
	return ""
}

func TestStreamDeliversChunksAndEndsWithDone(t *testing.T) {
	sp := newStreamingProvider(t)
	close(sp.release)
	ts := newStreamGateway(t, sp)

	resp := openStream(context.Background(), t, ts)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type %q, want text/event-stream", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	for i, want := range []string{sseChunk1, sseChunk2, "[DONE]"} {
		if got := nextData(t, sc); got != want {
			t.Errorf("событие %d: %q, want %q", i, got, want)
		}
	}
	if started := <-sp.started; started["stream"] != true || started["model"] != "qwen-local" {
		t.Errorf("провайдер получил %v: ждём stream=true и модель провайдера", started)
	}
}

func TestStreamChunksAreForwardedBeforeProviderFinishes(t *testing.T) {
	sp := newStreamingProvider(t)
	ts := newStreamGateway(t, sp)

	resp := openStream(context.Background(), t, ts)
	sc := bufio.NewScanner(resp.Body)

	// Провайдер ещё не отдал остальное (release не закрыт), а первый чанк уже у клиента.
	if got := nextData(t, sc); got != sseChunk1 {
		t.Errorf("первый чанк %q, want %q", got, sseChunk1)
	}
	close(sp.release)
	if got := nextData(t, sc); got != sseChunk2 {
		t.Errorf("второй чанк %q, want %q", got, sseChunk2)
	}
}

func TestClientCancelReachesProvider(t *testing.T) {
	sp := newStreamingProvider(t)
	ts := newStreamGateway(t, sp)

	ctx, cancel := context.WithCancel(context.Background())
	resp := openStream(ctx, t, ts)
	if got := nextData(t, bufio.NewScanner(resp.Body)); got != sseChunk1 {
		t.Fatalf("первый чанк %q, want %q", got, sseChunk1)
	}
	cancel()

	select {
	case <-sp.cancelled:
	case <-time.After(waitLimit):
		t.Fatal("запрос к провайдеру не отменён после отключения клиента")
	}
}
