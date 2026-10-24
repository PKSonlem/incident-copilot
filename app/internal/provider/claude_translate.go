package provider

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/chatwire"
)

// errLegacyFunctions — ответ на устаревшие functions, function_call и роль function: поддерживаются
// только tools.
var errLegacyFunctions = &ClientError{Message: "functions and function_call are not supported: use tools and tool_choice"}

// translateRequest переводит тело chat completions (OpenAI) в тело Anthropic Messages.
// Второй результат: просил ли клиент usage в потоке (stream_options.include_usage).
//
// Поля запроса, которых у Anthropic нет или которые здесь не переводятся (n, presence_penalty,
// frequency_penalty, seed, response_format, user, metadata и т. п.), намеренно отбрасываются:
// они не меняют смысл запроса настолько, чтобы отказывать клиенту, а OpenAI-клиенты шлют их
// по умолчанию. Инструменты (tools, tool_choice, роль tool, tool_calls) переводятся в формат
// Anthropic; устаревшие functions и function_call отвергаются ClientError.
func translateRequest(body []byte, stream bool, defaultMaxTokens int) ([]byte, bool, error) {
	var in struct {
		Model               string            `json:"model"`
		Messages            []json.RawMessage `json:"messages"`
		MaxTokens           *int              `json:"max_tokens"`
		MaxCompletionTokens *int              `json:"max_completion_tokens"`
		Temperature         *float64          `json:"temperature"`
		TopP                *float64          `json:"top_p"`
		Stop                json.RawMessage   `json:"stop"`
		Tools               json.RawMessage   `json:"tools"`
		ToolChoice          json.RawMessage   `json:"tool_choice"`
		Functions           json.RawMessage   `json:"functions"`
		FunctionCall        json.RawMessage   `json:"function_call"`
		StreamOptions       struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, false, fmt.Errorf("decode request: %w", err)
	}
	for _, field := range []json.RawMessage{in.Functions, in.FunctionCall} {
		if !chatwire.IsNullOrEmpty(field) {
			return nil, false, errLegacyFunctions
		}
	}
	out := map[string]any{"model": in.Model, "max_tokens": defaultMaxTokens}
	switch {
	case in.MaxTokens != nil:
		out["max_tokens"] = *in.MaxTokens
	case in.MaxCompletionTokens != nil:
		out["max_tokens"] = *in.MaxCompletionTokens
	}
	if in.Temperature != nil {
		out["temperature"] = *in.Temperature
	}
	if in.TopP != nil {
		out["top_p"] = *in.TopP
	}
	if stop, err := stopSequences(in.Stop); err != nil {
		return nil, false, err
	} else if len(stop) > 0 {
		out["stop_sequences"] = stop
	}
	if stream {
		out["stream"] = true
	}
	if tools, err := translateTools(in.Tools); err != nil {
		return nil, false, err
	} else if len(tools) > 0 {
		out["tools"] = tools
	}
	if choice, err := translateToolChoice(in.ToolChoice); err != nil {
		return nil, false, err
	} else if choice != nil {
		out["tool_choice"] = choice
	}

	var system []string
	messages := []map[string]any{}
	for _, raw := range in.Messages {
		var m struct {
			Role         string          `json:"role"`
			Content      json.RawMessage `json:"content"`
			ToolCalls    json.RawMessage `json:"tool_calls"`
			ToolCallID   string          `json:"tool_call_id"`
			FunctionCall json.RawMessage `json:"function_call"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, false, fmt.Errorf("decode message: %w", err)
		}
		if m.Role == "function" || !chatwire.IsNullOrEmpty(m.FunctionCall) {
			return nil, false, errLegacyFunctions
		}
		text, err := contentText(m.Content)
		if err != nil {
			return nil, false, err
		}
		switch {
		case m.Role == "tool":
			block := map[string]any{"type": "tool_result", "tool_use_id": m.ToolCallID, "content": text}
			// Результаты подряд идущих вызовов Anthropic ждёт одним сообщением user.
			if n := len(messages); n > 0 && isToolResultMessage(messages[n-1]) {
				blocks, _ := messages[n-1]["content"].([]map[string]any) //nolint:errcheck // тип проверен в isToolResultMessage
				messages[n-1]["content"] = append(blocks, block)
			} else {
				messages = append(messages, map[string]any{"role": "user", "content": []map[string]any{block}})
			}
		case m.Role == "system" || m.Role == "developer":
			system = append(system, text)
		case !chatwire.IsNullOrEmpty(m.ToolCalls):
			blocks, err := assistantBlocks(text, m.ToolCalls)
			if err != nil {
				return nil, false, err
			}
			messages = append(messages, map[string]any{"role": m.Role, "content": blocks})
		default:
			messages = append(messages, map[string]any{"role": m.Role, "content": text})
		}
	}
	if len(system) > 0 {
		out["system"] = strings.Join(system, "\n\n")
	}
	out["messages"] = messages
	data, err := json.Marshal(out)
	if err != nil {
		return nil, false, fmt.Errorf("encode request: %w", err)
	}
	return data, in.StreamOptions.IncludeUsage, nil
}

// openAIToolCall собирает элемент tool_calls формата OpenAI. Пустые id и name опускаются: так выглядят
// фрагменты аргументов в потоке, где id и имя приходят только в первом фрагменте.
func openAIToolCall(id, name, arguments string) map[string]any {
	function := map[string]any{"arguments": arguments}
	call := map[string]any{"function": function}
	if name != "" {
		function["name"] = name
	}
	if id != "" {
		call["id"], call["type"] = id, "function"
	}
	return call
}

// isToolResultMessage сообщает, что это сообщение user с блоками tool_result (в него склеиваются
// результаты подряд идущих tool).
func isToolResultMessage(msg map[string]any) bool {
	blocks, ok := msg["content"].([]map[string]any)
	return ok && len(blocks) > 0 && blocks[0]["type"] == "tool_result"
}

// translateTools переводит tools OpenAI ({type:function, function:{name, description, parameters}})
// в tools Anthropic ({name, description, input_schema}).
func translateTools(raw json.RawMessage) ([]map[string]any, error) {
	if chatwire.IsNullOrEmpty(raw) {
		return nil, nil
	}
	var in []struct {
		Type     string `json:"type"`
		Function struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			Parameters  json.RawMessage `json:"parameters"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, &ClientError{Message: "tools must be an array of function tools"}
	}
	out := make([]map[string]any, 0, len(in))
	for _, t := range in {
		if t.Type != "function" || t.Function.Name == "" {
			return nil, &ClientError{Message: "each tool must have type function and a function name"}
		}
		schema := json.RawMessage(`{"type":"object","properties":{}}`)
		if !chatwire.IsNullOrEmpty(t.Function.Parameters) {
			schema = t.Function.Parameters
		}
		tool := map[string]any{"name": t.Function.Name, "input_schema": schema}
		if t.Function.Description != "" {
			tool["description"] = t.Function.Description
		}
		out = append(out, tool)
	}
	return out, nil
}

// translateToolChoice переводит tool_choice: auto, none, required (any) и конкретная функция (tool).
func translateToolChoice(raw json.RawMessage) (map[string]any, error) {
	if chatwire.IsNullOrEmpty(raw) {
		return nil, nil
	}
	var mode string
	if json.Unmarshal(raw, &mode) == nil {
		switch mode {
		case "auto":
			return map[string]any{"type": "auto"}, nil
		case "none":
			return map[string]any{"type": "none"}, nil
		case "required":
			return map[string]any{"type": "any"}, nil
		}
		return nil, &ClientError{Message: fmt.Sprintf("unsupported tool_choice %q", mode)}
	}
	var named struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &named); err != nil || named.Type != "function" || named.Function.Name == "" {
		return nil, &ClientError{Message: "tool_choice must be auto, none, required or a function"}
	}
	return map[string]any{"type": "tool", "name": named.Function.Name}, nil
}

// assistantBlocks собирает content сообщения assistant с вызовами: текст (если есть) и tool_use.
func assistantBlocks(text string, rawCalls json.RawMessage) ([]map[string]any, error) {
	var calls []struct {
		ID       string `json:"id"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	if err := json.Unmarshal(rawCalls, &calls); err != nil {
		return nil, &ClientError{Message: "tool_calls must be an array of function calls"}
	}
	blocks := []map[string]any{}
	if text != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": text})
	}
	for _, c := range calls {
		input := map[string]any{}
		if strings.TrimSpace(c.Function.Arguments) != "" {
			if err := json.Unmarshal([]byte(c.Function.Arguments), &input); err != nil {
				return nil, &ClientError{Message: fmt.Sprintf("arguments of tool call %q must be a JSON object", c.ID)}
			}
		}
		blocks = append(blocks, map[string]any{"type": "tool_use", "id": c.ID, "name": c.Function.Name, "input": input})
	}
	return blocks, nil
}

// contentText приводит content (строка, null или массив текстовых частей) к строке.
func contentText(raw json.RawMessage) (string, error) {
	if chatwire.IsNullOrEmpty(raw) {
		return "", nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", &ClientError{Message: "message content must be a string or an array of text parts"}
	}
	var sb strings.Builder
	for _, p := range parts {
		if p.Type != "text" {
			return "", &ClientError{Message: fmt.Sprintf("unsupported content type %q: only text is supported", p.Type)}
		}
		sb.WriteString(p.Text)
	}
	return sb.String(), nil
}

func stopSequences(raw json.RawMessage) ([]string, error) {
	if chatwire.IsNullOrEmpty(raw) {
		return nil, nil
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return []string{one}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, &ClientError{Message: "stop must be a string or an array of strings"}
	}
	return many, nil
}

// finishReason переводит stop_reason Anthropic в finish_reason OpenAI. Неизвестная причина
// отдаётся как stop, но попадает в лог, чтобы новая причина не прошла незамеченной.
func finishReason(stopReason string) string {
	switch stopReason {
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "refusal":
		return "content_filter"
	default:
		slog.Warn("unknown anthropic stop_reason, reporting stop", "stop_reason", stopReason)
		return "stop"
	}
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func openAIUsage(promptTokens, completionTokens int) map[string]int {
	return map[string]int{
		"prompt_tokens": promptTokens, "completion_tokens": completionTokens, "total_tokens": promptTokens + completionTokens,
	}
}

// translateResponse переводит ответ Anthropic Messages в chat.completion.
func translateResponse(data []byte) ([]byte, error) {
	var in struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Content []struct {
			Type  string          `json:"type"`
			Text  string          `json:"text"`
			ID    string          `json:"id"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
		StopReason string         `json:"stop_reason"`
		Usage      anthropicUsage `json:"usage"`
	}
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("decode provider response: %w", err)
	}
	var text strings.Builder
	var calls []map[string]any
	for _, b := range in.Content {
		switch b.Type {
		case "text":
			text.WriteString(b.Text)
		case "tool_use":
			args := string(b.Input)
			if chatwire.IsNullOrEmpty(b.Input) {
				args = "{}"
			}
			calls = append(calls, openAIToolCall(b.ID, b.Name, args))
		}
	}
	message := map[string]any{"role": "assistant", "content": text.String()}
	if len(calls) > 0 {
		message["tool_calls"] = calls
		if text.Len() == 0 {
			message["content"] = nil
		}
	}
	out, err := json.Marshal(map[string]any{
		"id":      "chatcmpl-" + in.ID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   in.Model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       message,
			"finish_reason": finishReason(in.StopReason),
		}},
		"usage": openAIUsage(in.Usage.InputTokens, in.Usage.OutputTokens),
	})
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return out, nil
}

// openAIErrorBody собирает тело ошибки в формате OpenAI.
func openAIErrorBody(message, typ string) []byte {
	out, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": message, "type": typ, "param": nil, "code": nil,
	}})
	if err != nil {
		return []byte(`{"error":{"message":"provider request failed","type":"api_error","param":null,"code":null}}`)
	}
	return out
}

// translateError переводит ошибку Anthropic в статус и тело в формате OpenAI. Ошибки запроса (400)
// и лимита (429) проходят со своим статусом. Остальное (ключ гейтвея, перегрузка, сбои) клиент
// исправить не может, это 502 без подробностей провайдера.
func translateError(status int, data []byte) (int, []byte) {
	var in struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	msg := "provider request failed"
	if json.Unmarshal(data, &in) == nil && in.Error.Message != "" {
		msg = in.Error.Message
	}
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return http.StatusBadRequest, openAIErrorBody(msg, "invalid_request_error")
	case http.StatusTooManyRequests:
		return http.StatusTooManyRequests, openAIErrorBody(msg, "rate_limit_error")
	default:
		return http.StatusBadGateway, openAIErrorBody("provider request failed", "api_error")
	}
}
