package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/PKSonlem/incident-copilot/app/internal/cache"
)

// serveFromCache отвечает из кэша, если запись есть, и сообщает об этом. Иначе выставляет X-Cache
// по плану и возвращает false: дальше запрос идёт к провайдеру.
// TODO(#13): при повторе из кэша отдавать usage и учитывать его.
func (g *gateway) serveFromCache(w http.ResponseWriter, r *http.Request, plan cache.Plan, stream bool) bool {
	if completion, ok := g.cache.Lookup(r.Context(), plan); ok {
		w.Header().Set(cache.Header, string(cache.Hit))
		if stream {
			replayStream(w, r, completion)
		} else {
			writeJSON(w, r, http.StatusOK, completion)
		}
		return true
	}
	w.Header().Set(cache.Header, string(plan.Status))
	return false
}

// replayStream отдаёт сохранённый chat.completion как поток SSE.
func replayStream(w http.ResponseWriter, r *http.Request, completion []byte) {
	flusher, canFlush := w.(http.Flusher)
	events, err := cache.ReplayEvents(completion)
	if err != nil || !canFlush {
		writeError(w, &apiError{http.StatusInternalServerError, errAPI, "cannot replay cached response"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	for _, event := range events {
		if _, err := w.Write(event); err != nil {
			slog.WarnContext(r.Context(), "write cached stream", "error", err)
			return
		}
	}
	flusher.Flush()
}
