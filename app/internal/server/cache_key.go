package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// cacheHeader — заголовок ответа с состоянием кэша.
const cacheHeader = "X-Cache"

// cacheStatus — значение заголовка X-Cache.
type cacheStatus string

const (
	cacheHit    cacheStatus = "HIT"    // ответ взят из кэша
	cacheMiss   cacheStatus = "MISS"   // запрос кэшируем, записи не было
	cacheBypass cacheStatus = "BYPASS" // запрос не кэшируем
)

const cacheKeyPrefix = "cache:v1:"

// cachePlan — решение кэша по запросу, принятое до обращения к провайдеру: что писать в X-Cache,
// если ответ не из кэша, и под каким ключом искать и сохранять.
type cachePlan struct {
	status cacheStatus // cacheMiss (key задан) или cacheBypass
	key    string
}

// keyedFields — единственные поля запроса, влияющие на ответ модели. user, metadata, stream,
// stream_options и прочее на ответ не влияют и в ключ не входят: иначе одинаковые запросы
// от разных пользователей не попадали бы в общий кэш.
var keyedFields = []string{
	"messages",
	"temperature", "top_p", "max_tokens", "max_completion_tokens", "stop",
	"seed", "presence_penalty", "frequency_penalty", "logit_bias", "logprobs", "top_logprobs",
	"response_format", "reasoning_effort",
	"tools", "tool_choice", "parallel_tool_calls", "functions", "function_call",
}

// planCache решает, кэшировать ли запрос. Кэшируются только запросы с явным temperature=0:
// при другой температуре ответ недетерминирован, и повтор из кэша подменил бы новую
// генерацию. Запросы с n != 1 тоже обходят кэш: в записи хранится один ответ, а n просит
// несколько независимых вариантов.
func planCache(req map[string]json.RawMessage, logicalModel string) cachePlan {
	bypass := cachePlan{status: cacheBypass}
	var temperature float64
	if err := json.Unmarshal(req["temperature"], &temperature); err != nil || temperature != 0 {
		return bypass
	}
	if raw, ok := req["n"]; ok {
		var n int
		if err := json.Unmarshal(raw, &n); err != nil || n != 1 {
			return bypass
		}
	}
	keyed := make(map[string]json.RawMessage, len(keyedFields)+1)
	for _, field := range keyedFields {
		if value, ok := req[field]; ok {
			keyed[field] = value
		}
	}
	model, err := json.Marshal(logicalModel)
	if err != nil {
		return bypass
	}
	keyed["model"] = model
	canonical, err := json.Marshal(keyed) // ключи map сортируются: порядок полей не влияет
	if err != nil {
		return bypass
	}
	sum := sha256.Sum256(canonical)
	return cachePlan{status: cacheMiss, key: cacheKeyPrefix + hex.EncodeToString(sum[:])}
}
