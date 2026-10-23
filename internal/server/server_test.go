package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PKSonlem/incident-copilot/internal/server"
)

func TestLiveAndReady(t *testing.T) {
	ts := httptest.NewServer(server.New())
	defer ts.Close()

	for _, path := range []string{"/live", "/ready"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: status %d, want 200", path, resp.StatusCode)
		}
	}
}
