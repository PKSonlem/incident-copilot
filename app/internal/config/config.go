// Package config загружает конфигурацию гейтвея: структура из файла, секреты из окружения.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/PKSonlem/incident-copilot/app/internal/provider"
)

// Config описывает провайдеров, модели и клиентов гейтвея.
type Config struct {
	Redis     Redis      `yaml:"redis"`
	Cache     Cache      `yaml:"cache"`
	Providers []Provider `yaml:"providers"`
	Models    []Model    `yaml:"models"`
	Clients   []Client   `yaml:"clients"`
}

// Redis — адрес общего хранилища.
type Redis struct {
	Addr string `yaml:"addr"`
}

// Значения по умолчанию для необязательных настроек.
const (
	DefaultCacheTTL        = time.Hour
	DefaultClaudeMaxTokens = 4096
)

// Cache — настройки точного кэша ответов.
type Cache struct {
	TTL time.Duration `yaml:"ttl"` // не задан: DefaultCacheTTL
}

// EffectiveTTL возвращает срок жизни записей с учётом значения по умолчанию.
func (c Cache) EffectiveTTL() time.Duration {
	if c.TTL <= 0 {
		return DefaultCacheTTL
	}
	return c.TTL
}

// Provider — upstream-сервер. Секрет в файл не пишется: файл хранит только имя переменной окружения.
type Provider struct {
	Name      string `yaml:"name"`
	Kind      string `yaml:"kind"`
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
	APIKey    string `yaml:"-"`
	// DefaultMaxTokens подставляется провайдеру Claude, если клиент не задал max_tokens
	// (Anthropic требует его в каждом запросе). Не задан: DefaultClaudeMaxTokens.
	DefaultMaxTokens int `yaml:"default_max_tokens"`
}

// EffectiveMaxTokens возвращает max_tokens по умолчанию с учётом значения по умолчанию.
func (p Provider) EffectiveMaxTokens() int {
	if p.DefaultMaxTokens <= 0 {
		return DefaultClaudeMaxTokens
	}
	return p.DefaultMaxTokens
}

// Model связывает имя модели для клиента с провайдером и именем модели у него.
type Model struct {
	Name          string `yaml:"name"`
	Provider      string `yaml:"provider"`
	ProviderModel string `yaml:"provider_model"`
}

// Client — API-ключ потребителя гейтвея. Значение ключа берётся из окружения.
type Client struct {
	Name   string `yaml:"name"`
	KeyEnv string `yaml:"key_env"`
	Key    string `yaml:"-"`
	// RateLimit ограничивает число запросов ключа; нулевое значение означает «без ограничения».
	RateLimit RateLimit `yaml:"rate_limit"`
}

// RateLimit — не больше Requests запросов за окно Window.
type RateLimit struct {
	Requests int           `yaml:"requests"`
	Window   time.Duration `yaml:"window"`
}

// Load читает файл и подставляет секреты через getenv. Неизвестные поля (в том числе
// литеральные секреты) и пустые переменные окружения дают ошибку.
func Load(path string, getenv func(string) string) (Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // путь задаёт оператор
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Redis.Addr == "" {
		return Config{}, errors.New("redis.addr is required")
	}
	if cfg.Cache.TTL < 0 {
		return Config{}, errors.New("cache.ttl must not be negative")
	}
	for i := range cfg.Providers {
		p := &cfg.Providers[i]
		if p.DefaultMaxTokens < 0 {
			return Config{}, fmt.Errorf("provider %q: default_max_tokens must not be negative", p.Name)
		}
		if p.APIKeyEnv == "" {
			continue
		}
		if p.APIKey, err = secretFromEnv(getenv, p.APIKeyEnv); err != nil {
			return Config{}, fmt.Errorf("provider %q: %w", p.Name, err)
		}
	}
	for i := range cfg.Clients {
		c := &cfg.Clients[i]
		if c.Key, err = secretFromEnv(getenv, c.KeyEnv); err != nil {
			return Config{}, fmt.Errorf("client %q: %w", c.Name, err)
		}
	}
	for _, c := range cfg.Clients {
		if rl := c.RateLimit; rl.Requests < 0 || (rl.Requests > 0 && rl.Window <= 0) {
			return Config{}, fmt.Errorf("client %q: rate_limit needs requests > 0 and window > 0", c.Name)
		}
	}
	for _, m := range cfg.Models {
		if m.Name == "" || m.Provider == "" || m.ProviderModel == "" {
			return Config{}, fmt.Errorf("model %q: name, provider and provider_model are required", m.Name)
		}
	}
	if err := checkRoutingModels(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// checkRoutingModels требует модель на провайдере каждого вида: маршрутизация по сложности
// отправляет простые запросы на openai, сложные на claude.
func checkRoutingModels(cfg Config) error {
	kinds := map[string]string{}
	for _, p := range cfg.Providers {
		kinds[p.Name] = p.Kind
	}
	have := map[string]bool{}
	for _, m := range cfg.Models {
		have[kinds[m.Provider]] = true
	}
	for _, kind := range []string{provider.KindOpenAI, provider.KindClaude} {
		if !have[kind] {
			return fmt.Errorf("models: at least one model on a provider of kind %q is required for routing by complexity", kind)
		}
	}
	return nil
}

func secretFromEnv(getenv func(string) string, name string) (string, error) {
	if name == "" {
		return "", errors.New("env variable name is not set")
	}
	v := getenv(name)
	if v == "" {
		return "", fmt.Errorf("env %s is empty", name)
	}
	return v, nil
}
