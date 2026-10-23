package provider

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// sseTranslator читает SSE Anthropic из upstream и отдаёт SSE OpenAI. Работает через pipe, поэтому
// чанки уходят клиенту сразу. Ошибка потока передаётся читателю как ошибка чтения.
type sseTranslator struct {
	pr       *io.PipeReader
	upstream io.Closer
}

func newSSETranslator(upstream io.ReadCloser, includeUsage bool) io.ReadCloser {
	pr, pw := io.Pipe()
	t := &sseTranslator{pr: pr, upstream: upstream}
	go func() {
		err := translateStream(upstream, pw, includeUsage)
		if cerr := upstream.Close(); cerr != nil && err == nil {
			err = cerr
		}
		pw.CloseWithError(err) // nil превращается в io.EOF
	}()
	return t
}

func (t *sseTranslator) Read(p []byte) (int, error) { return t.pr.Read(p) } //nolint:wrapcheck // io.Reader

// Close останавливает перевод и закрывает соединение с провайдером.
func (t *sseTranslator) Close() error {
	err := t.pr.Close()
	if cerr := t.upstream.Close(); cerr != nil {
		err = errors.Join(err, cerr)
	}
	return err
}

type streamState struct {
	id           string
	model        string
	created      int64
	inputTokens  int
	outputTokens int
}

func translateStream(r io.Reader, w io.Writer, includeUsage bool) error {
	br := bufio.NewReader(r)
	st := streamState{created: time.Now().Unix()}
	for {
		line, rerr := br.ReadString('\n')
		if payload, ok := strings.CutPrefix(strings.TrimRight(line, "\r\n"), "data: "); ok {
			done, err := st.handle([]byte(payload), w, includeUsage)
			if err != nil || done {
				return err
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return io.ErrUnexpectedEOF // поток оборвался до message_stop
			}
			return fmt.Errorf("read provider stream: %w", rerr)
		}
	}
}

// handle переводит одно событие. done=true после message_stop.
func (s *streamState) handle(payload []byte, w io.Writer, includeUsage bool) (bool, error) {
	var ev struct {
		Type    string `json:"type"`
		Message struct {
			ID    string         `json:"id"`
			Model string         `json:"model"`
			Usage anthropicUsage `json:"usage"`
		} `json:"message"`
		Delta struct {
			Type       string `json:"type"`
			Text       string `json:"text"`
			StopReason string `json:"stop_reason"`
		} `json:"delta"`
		Usage anthropicUsage `json:"usage"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return false, fmt.Errorf("decode provider event: %w", err)
	}
	switch ev.Type {
	case "message_start":
		s.id, s.model = "chatcmpl-"+ev.Message.ID, ev.Message.Model
		s.inputTokens = ev.Message.Usage.InputTokens
		return false, s.chunk(w, map[string]any{"role": "assistant", "content": ""}, nil, nil)
	case "content_block_delta":
		if ev.Delta.Type != "text_delta" {
			return false, nil
		}
		return false, s.chunk(w, map[string]any{"content": ev.Delta.Text}, nil, nil)
	case "message_delta":
		s.outputTokens = ev.Usage.OutputTokens
		reason := finishReason(ev.Delta.StopReason)
		return false, s.chunk(w, map[string]any{}, &reason, nil)
	case "message_stop":
		if includeUsage {
			if err := s.chunk(w, nil, nil, openAIUsage(s.inputTokens, s.outputTokens)); err != nil {
				return false, err
			}
		}
		if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
			return false, fmt.Errorf("write done: %w", err)
		}
		return true, nil
	case "error":
		return false, fmt.Errorf("provider stream error: %s", ev.Error.Message)
	default: // ping, content_block_start, content_block_stop
		return false, nil
	}
}

// chunk пишет один chat.completion.chunk. При usage != nil чанк без choices, как у OpenAI.
func (s *streamState) chunk(w io.Writer, delta map[string]any, finish *string, usage map[string]int) error {
	choices := []any{}
	if usage == nil {
		choice := map[string]any{"index": 0, "delta": delta, "finish_reason": nil}
		if finish != nil {
			choice["finish_reason"] = *finish
		}
		choices = append(choices, choice)
	}
	c := map[string]any{
		"id": s.id, "object": "chat.completion.chunk", "created": s.created, "model": s.model, "choices": choices,
	}
	if usage != nil {
		c["usage"] = usage
	}
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode chunk: %w", err)
	}
	if _, err := w.Write(append(append([]byte("data: "), data...), '\n', '\n')); err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}
	return nil
}
