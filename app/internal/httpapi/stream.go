package httpapi

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
)

// streamCompletion отдаёт клиенту поток SSE от провайдера по мере поступления. Отмена запроса
// клиентом (r.Context()) прерывает запрос к провайдеру.
func (g *gateway) streamCompletion(w http.ResponseWriter, r *http.Request, prep prepared) {
	upstream, err := prep.route.Provider.Stream(r.Context(), prep.body)
	if err != nil {
		writeError(w, providerFailure(err))
		return
	}
	defer func() {
		if cerr := upstream.Body.Close(); cerr != nil {
			slog.WarnContext(r.Context(), "close provider stream", "error", cerr)
		}
	}()
	if upstream.Status != http.StatusOK {
		data, rerr := io.ReadAll(upstream.Body)
		if rerr != nil || upstream.Status >= 500 || upstream.Status < 200 {
			writeError(w, &apiError{http.StatusBadGateway, errAPI, "provider request failed"})
			return
		}
		writeJSON(w, r, upstream.Status, data)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, &apiError{http.StatusInternalServerError, errAPI, "streaming is not supported by the server"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	capture := g.cache.Capture(prep.cache)
	buf := make([]byte, 4096)
	for {
		n, rerr := upstream.Body.Read(buf)
		if n > 0 {
			capture.Feed(buf[:n])
			if _, werr := w.Write(buf[:n]); werr != nil {
				slog.WarnContext(r.Context(), "write stream", "error", werr)
				return
			}
			flusher.Flush()
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				capture.Commit(r.Context())
			} else if r.Context().Err() == nil {
				slog.WarnContext(r.Context(), "read provider stream", "error", rerr)
				writeStreamError(w, flusher, r)
			}
			return
		}
	}
}
