package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/PKSonlem/incident-copilot/app/internal/chatwire"
)

type cachedCompletion struct {
	ID      string          `json:"id"`
	Model   string          `json:"model"`
	Created json.RawMessage `json:"created"`
	Choices []struct {
		Index        int            `json:"index"`
		Message      map[string]any `json:"message"`
		FinishReason any            `json:"finish_reason"`
	} `json:"choices"`
}

// replayStream отдаёт сохранённый chat.completion как поток SSE: для каждого выбора один чанк
// с полным содержимым и один завершающий чанк с finish_reason.
func replayStream(w http.ResponseWriter, r *http.Request, completion []byte) {
	var cached cachedCompletion
	flusher, canFlush := w.(http.Flusher)
	if err := json.Unmarshal(completion, &cached); err != nil || len(cached.Choices) == 0 || !canFlush {
		writeError(w, &apiError{http.StatusInternalServerError, errAPI, "cannot replay cached response"})
		return
	}
	events := make([][]byte, 0, 2*len(cached.Choices)+1)
	for _, choice := range cached.Choices {
		for _, choiceChunk := range []map[string]any{
			chatwire.Choice(choice.Index, replayDelta(choice.Message), nil),
			chatwire.Choice(choice.Index, map[string]any{}, choice.FinishReason),
		} {
			event, err := chatwire.Chunk{
				ID: cached.ID, Model: cached.Model, Created: cached.Created,
				Choices: []map[string]any{choiceChunk},
			}.SSE()
			if err != nil {
				writeError(w, &apiError{http.StatusInternalServerError, errAPI, "cannot replay cached response"})
				return
			}
			events = append(events, event)
		}
	}
	events = append(events, []byte(chatwire.DoneEvent))
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

// replayDelta превращает message в delta: tool_calls получают index, а пустой или null content
// рядом с tool_calls не отдаётся (OpenAI не шлёт content: "" вместе с вызовами инструментов).
func replayDelta(message map[string]any) map[string]any {
	delta := make(map[string]any, len(message))
	for key, value := range message {
		delta[key] = value
	}
	calls, hasCalls := delta["tool_calls"].([]any)
	if hasCalls {
		for index, call := range calls {
			if fields, ok := call.(map[string]any); ok {
				fields["index"] = index
			}
		}
	}
	if content, ok := delta["content"]; ok && hasCalls && (content == nil || content == "") {
		delete(delta, "content")
	}
	return delta
}
