// Package server содержит HTTP-обработчики сервиса.
package server

import "net/http"

// New возвращает обработчик с эндпоинтами live и ready.
func New() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /live", ok)
	mux.HandleFunc("GET /ready", ok)
	return mux
}

func ok(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
