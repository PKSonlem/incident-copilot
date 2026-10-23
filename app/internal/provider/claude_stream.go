package provider

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/chatwire"
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

// streamState — что известно о потоке к текущему событию.
type streamState struct {
	id           string
	model        string
	created      json.RawMessage
	inputTokens  int
	outputTokens int
}

func translateStream(r io.Reader, w io.Writer, includeUsage bool) error {
	br := bufio.NewReader(r)
	created, err := json.Marshal(time.Now().Unix())
	if err != nil {
		return fmt.Errorf("encode created: %w", err)
	}
	state := streamState{created: created}
	for {
		line, rerr := br.ReadString('\n')
		if payload, ok := strings.CutPrefix(strings.TrimRight(line, "\r\n"), "data: "); ok {
			done, err := state.handle([]byte(payload), w, includeUsage)
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
		return false, s.writeChunk(w, map[string]any{"role": "assistant", "content": ""}, nil)
	case "content_block_delta":
		if ev.Delta.Type != "text_delta" {
			return false, nil
		}
		return false, s.writeChunk(w, map[string]any{"content": ev.Delta.Text}, nil)
	case "message_delta":
		s.outputTokens = ev.Usage.OutputTokens
		reason := finishReason(ev.Delta.StopReason)
		return false, s.writeChunk(w, map[string]any{}, &reason)
	case "message_stop":
		if includeUsage {
			if err := s.writeUsageChunk(w, openAIUsage(s.inputTokens, s.outputTokens)); err != nil {
				return false, err
			}
		}
		if _, err := io.WriteString(w, chatwire.DoneEvent); err != nil {
			return false, fmt.Errorf("write done: %w", err)
		}
		return true, nil
	case "error":
		return false, fmt.Errorf("provider stream error: %s", ev.Error.Message)
	default: // ping, content_block_start, content_block_stop
		return false, nil
	}
}

// writeChunk пишет один chat.completion.chunk с одним choice. finish nil: finish_reason null.
func (s *streamState) writeChunk(w io.Writer, delta map[string]any, finish *string) error {
	var finishReason any
	if finish != nil {
		finishReason = *finish
	}
	return s.write(w, chatwire.Chunk{Choices: []map[string]any{chatwire.Choice(0, delta, finishReason)}})
}

// writeUsageChunk пишет чанк с usage без choices, как OpenAI при include_usage.
func (s *streamState) writeUsageChunk(w io.Writer, usage map[string]int) error {
	return s.write(w, chatwire.Chunk{Usage: usage})
}

func (s *streamState) write(w io.Writer, chunk chatwire.Chunk) error {
	chunk.ID, chunk.Model, chunk.Created = s.id, s.model, s.created
	data, err := chunk.SSE()
	if err != nil {
		return fmt.Errorf("build chunk: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}
	return nil
}
