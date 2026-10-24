package main_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const waitLimit = 10 * time.Second

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

// TestSIGTERMLetsRunningStreamFinish собирает настоящий бинарник, запускает его на фейковом
// провайдере, шлёт SIGTERM посреди потока и ждёт, что поток дойдёт до [DONE], а процесс выйдет с кодом 0.
func TestSIGTERMLetsRunningStreamFinish(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher) //nolint:errcheck // httptest отдаёт Flusher
		write := func(s string) {
			if _, err := io.WriteString(w, s); err != nil {
				t.Errorf("fake provider write: %v", err)
			}
			flusher.Flush()
		}
		write("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n")
		close(started)
		<-release
		write("data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(provider.Close)

	redisAddr := startRedis(t)
	dir := t.TempDir()
	bin := filepath.Join(dir, "incident-copilot")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	cfgPath := filepath.Join(dir, "gateway.yaml")
	cfg := fmt.Sprintf("redis:\n  addr: %s\nproviders:\n  - name: local\n    kind: openai\n    base_url: %s\n  - name: anthropic\n    kind: claude\n    base_url: %s\n"+
		"models:\n  - name: local-small\n    provider: local\n    provider_model: qwen-local\n  - name: claude-sonnet\n    provider: anthropic\n    provider_model: claude-test-model\n"+
		"clients:\n  - name: agent\n    key_env: GATEWAY_CLIENT_KEY\n", redisAddr, provider.URL, provider.URL)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	addr := freeAddr(t)
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "CONFIG_PATH="+cfgPath, "LISTEN_ADDR="+addr, "GATEWAY_CLIENT_KEY=k")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() }) //nolint:errcheck // процесс мог уже завершиться

	waitForHTTP(t, "http://"+addr+"/live")

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions",
		strings.NewReader(`{"model":"local-small","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST stream: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() }) //nolint:errcheck // закрытие тела в cleanup
	sc := bufio.NewScanner(resp.Body)
	if !scanUntil(sc, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}") {
		t.Fatal("первый чанк не дошёл до клиента")
	}
	<-started

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	select {
	case err := <-exited:
		t.Fatalf("сервис завершился, не дождавшись текущего запроса: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)

	if !scanUntil(sc, "data: [DONE]") {
		t.Fatal("поток оборвался при остановке: нет [DONE]")
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Errorf("сервис вышел с ошибкой: %v", err)
		}
	case <-time.After(waitLimit):
		t.Fatal("сервис не завершился после окончания запроса")
	}
}

func scanUntil(sc *bufio.Scanner, line string) bool {
	for sc.Scan() {
		if sc.Text() == line {
			return true
		}
	}
	return false
}

func waitForHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(waitLimit)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec,noctx // адрес поднятого в тесте сервиса
		if err == nil {
			_ = resp.Body.Close() //nolint:errcheck // тело не читаем
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("сервис не ответил на %s", url)
}

// startRedis поднимает настоящий redis-server на свободном порту на время теста.
// Копия помощника из internal/server: тестовые пакеты не разделяют код.
func startRedis(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("redis-server")
	if err != nil {
		t.Fatalf("redis-server не найден в PATH: %v", err)
	}
	addr := freeAddr(t)
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	cmd := exec.Command(path, "--port", port, "--bind", "127.0.0.1", "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill() //nolint:errcheck // процесс мог уже завершиться
		_ = cmd.Wait()         //nolint:errcheck // код выхода убитого процесса не важен
	})
	deadline := time.Now().Add(waitLimit)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			_ = c.Close() //nolint:errcheck // пробное соединение
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("redis на %s не поднялся", addr)
	return ""
}
