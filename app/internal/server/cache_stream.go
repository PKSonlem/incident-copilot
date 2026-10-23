package server

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/PKSonlem/incident-copilot/app/internal/chatwire"
)

// streamCapture копирует поток SSE провайдера и по его окончании кладёт собранный
// chat.completion в кэш. Методы безопасны для nil: nil означает «запрос не кэшируем».
type streamCapture struct {
	cache     *responseCache
	plan      cachePlan
	assembler completionAssembler
}

// capture начинает запись потока; для запроса вне кэша возвращает nil.
func (c *responseCache) capture(plan cachePlan) *streamCapture {
	if plan.status != cacheMiss {
		return nil
	}
	return &streamCapture{cache: c, plan: plan}
}

// feed передаёт очередную порцию потока, как её прочитали у провайдера.
func (sc *streamCapture) feed(data []byte) {
	if sc != nil {
		sc.assembler.feed(data)
	}
}

// commit сохраняет ответ, если поток закончился корректно (событием [DONE], без ошибок).
func (sc *streamCapture) commit(ctx context.Context) {
	if sc == nil {
		return
	}
	if completion, ok := sc.assembler.completion(); ok {
		sc.cache.store(ctx, sc.plan, completion)
	}
}

// completionAssembler собирает из SSE-потока провайдера полный JSON chat.completion.
type completionAssembler struct {
	pending []byte // неполная строка потока
	done    bool
	broken  bool
	id      string
	model   string
	created json.RawMessage
	usage   json.RawMessage
	choices map[int]*choiceAssembler
}

type choiceAssembler struct {
	role         string
	content      strings.Builder
	finishReason any
	toolCalls    map[int]*toolCallAssembler
}

type toolCallAssembler struct {
	id, typ, name string
	arguments     strings.Builder
}

type streamChunk struct {
	ID      string          `json:"id"`
	Model   string          `json:"model"`
	Created json.RawMessage `json:"created"`
	Usage   json.RawMessage `json:"usage"`
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Role      string  `json:"role"`
			Content   *string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason any `json:"finish_reason"`
	} `json:"choices"`
	Error json.RawMessage `json:"error"`
}

func (a *completionAssembler) feed(data []byte) {
	a.pending = append(a.pending, data...)
	for {
		end := bytes.IndexByte(a.pending, '\n')
		if end < 0 {
			return
		}
		line := strings.TrimSpace(string(a.pending[:end]))
		a.pending = a.pending[end+1:]
		if payload, ok := strings.CutPrefix(line, "data:"); ok {
			a.event(strings.TrimSpace(payload))
		}
	}
}

func (a *completionAssembler) event(payload string) {
	if payload == "[DONE]" {
		a.done = true
		return
	}
	var chunk streamChunk
	if err := json.Unmarshal([]byte(payload), &chunk); err != nil || len(chunk.Error) > 0 {
		a.broken = true
		return
	}
	if chunk.ID != "" {
		a.id, a.model, a.created = chunk.ID, chunk.Model, chunk.Created
	}
	if !chatwire.IsNullOrEmpty(chunk.Usage) {
		a.usage = chunk.Usage
	}
	for _, delta := range chunk.Choices {
		choice := a.choice(delta.Index)
		if delta.Delta.Role != "" {
			choice.role = delta.Delta.Role
		}
		if delta.Delta.Content != nil {
			choice.content.WriteString(*delta.Delta.Content)
		}
		for _, call := range delta.Delta.ToolCalls {
			assembled := choice.toolCall(call.Index)
			if call.ID != "" {
				assembled.id = call.ID
			}
			if call.Type != "" {
				assembled.typ = call.Type
			}
			assembled.name += call.Function.Name
			assembled.arguments.WriteString(call.Function.Arguments)
		}
		if delta.FinishReason != nil {
			choice.finishReason = delta.FinishReason
		}
	}
}

func (a *completionAssembler) choice(index int) *choiceAssembler {
	if a.choices == nil {
		a.choices = map[int]*choiceAssembler{}
	}
	choice := a.choices[index]
	if choice == nil {
		choice = &choiceAssembler{}
		a.choices[index] = choice
	}
	return choice
}

func (c *choiceAssembler) toolCall(index int) *toolCallAssembler {
	if c.toolCalls == nil {
		c.toolCalls = map[int]*toolCallAssembler{}
	}
	call := c.toolCalls[index]
	if call == nil {
		call = &toolCallAssembler{}
		c.toolCalls[index] = call
	}
	return call
}

// message собирает message выбора. При вызовах инструментов без текста content равен null,
// как у OpenAI.
func (c *choiceAssembler) message() map[string]any {
	role := c.role
	if role == "" {
		role = "assistant"
	}
	msg := map[string]any{"role": role}
	if len(c.toolCalls) == 0 {
		msg["content"] = c.content.String()
		return msg
	}
	if c.content.Len() > 0 {
		msg["content"] = c.content.String()
	} else {
		msg["content"] = nil
	}
	indexes := make([]int, 0, len(c.toolCalls))
	for index := range c.toolCalls {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	calls := make([]map[string]any, 0, len(indexes))
	for _, index := range indexes {
		call := c.toolCalls[index]
		typ := call.typ
		if typ == "" {
			typ = "function"
		}
		calls = append(calls, map[string]any{
			"id": call.id, "type": typ,
			"function": map[string]any{"name": call.name, "arguments": call.arguments.String()},
		})
	}
	msg["tool_calls"] = calls
	return msg
}

// completion возвращает собранный ответ; false, если поток не завершился корректно.
func (a *completionAssembler) completion() ([]byte, bool) {
	if !a.done || a.broken || len(a.choices) == 0 {
		return nil, false
	}
	indexes := make([]int, 0, len(a.choices))
	for index := range a.choices {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	choices := make([]any, 0, len(indexes))
	for _, index := range indexes {
		choice := a.choices[index]
		choices = append(choices, map[string]any{
			"index": index, "message": choice.message(), "finish_reason": choice.finishReason,
		})
	}
	out := map[string]any{"id": a.id, "object": "chat.completion", "model": a.model, "choices": choices}
	if len(a.created) > 0 {
		out["created"] = a.created
	}
	if len(a.usage) > 0 {
		out["usage"] = a.usage
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, false
	}
	return data, true
}
