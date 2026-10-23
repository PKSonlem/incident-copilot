// Package config загружает конфигурацию гейтвея: структура из файла, секреты из окружения.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"

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
	for i := range cfg.Providers {
		p := &cfg.Providers[i]
		if p.APIKeyEnv == "" {
			continue
		}
		if p.APIKey = getenv(p.APIKeyEnv); p.APIKey == "" {
			return Config{}, fmt.Errorf("provider %q: env %s is empty", p.Name, p.APIKeyEnv)
		}
	}
	for i := range cfg.Clients {
		c := &cfg.Clients[i]
		if c.KeyEnv == "" {
			return Config{}, fmt.Errorf("client %q: key_env is required", c.Name)
		}
		if c.Key = getenv(c.KeyEnv); c.Key == "" {
			return Config{}, fmt.Errorf("client %q: env %s is empty", c.Name, c.KeyEnv)
		}
	}
	return cfg, validate(cfg)
}

func validate(cfg Config) error {
	known := map[string]bool{}
	for _, p := range cfg.Providers {
		known[p.Name] = true
	}
	for _, m := range cfg.Models {
		if !known[m.Provider] {
			return errors.New("model " + m.Name + ": unknown provider " + m.Provider)
		}
	}
	return nil
}
