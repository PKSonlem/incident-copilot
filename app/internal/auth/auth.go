// Package auth проверяет API-ключи клиентов гейтвея.
package auth

import (
	"crypto/subtle"
	"strings"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
)

// Authenticator сопоставляет значение заголовка Authorization с клиентами из конфигурации.
type Authenticator struct {
	clients []config.Client
}

// New создаёт проверку ключей для клиентов clients.
func New(clients []config.Client) *Authenticator {
	return &Authenticator{clients: clients}
}

// Authenticate возвращает клиента, чей ключ передан в заголовке Authorization (Bearer-схема).
func (a *Authenticator) Authenticate(authorization string) (config.Client, bool) {
	key, found := strings.CutPrefix(authorization, "Bearer ")
	if !found || key == "" {
		return config.Client{}, false
	}
	// Цикл намеренно не прерывается на первом совпадении: время ответа не должно
	// зависеть от того, какой по счёту ключ подошёл.
	var match config.Client
	matched := false
	for _, c := range a.clients {
		if subtle.ConstantTimeCompare([]byte(key), []byte(c.Key)) == 1 {
			match, matched = c, true
		}
	}
	return match, matched
}
