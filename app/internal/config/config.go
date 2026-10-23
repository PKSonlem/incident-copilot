// Package config загружает конфигурацию гейтвея: структура из файла, секреты из окружения.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config описывает провайдеров, модели и клиентов гейтвея.
type Config struct {
	Redis     Redis      `yaml:"redis"`
	Providers []Provider `yaml:"providers"`
	Models    []Model    `yaml:"models"`
	Clients   []Client   `yaml:"clients"`
}

// Redis — адрес общего хранилища.
type Redis struct {
	Addr string `yaml:"addr"`
}

// Provider — upstream-сервер. Секрет в файл не пишется: файл хранит только имя переменной окружения.
type Provider struct {
	Name      string `yaml:"name"`
	Kind      string `yaml:"kind"`
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
	APIKey    string `yaml:"-"`
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
	for i := range cfg.Providers {
		p := &cfg.Providers[i]
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
	return cfg, nil
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
