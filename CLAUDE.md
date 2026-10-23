# Incident Copilot

Навигация. Всё остальное лежит по указателям.

- Журнал решений проекта (Q1–Q26): `IDEAS.md`
- Стандарты ревью: `CODING_STANDARDS.md`
- Термины проекта: `GLOSSARY.md`
- Необратимые решения: `docs/adr/`
- Спеки: `SPEC-scaffold.md` (слой 0, делается первым), `SPEC-gateway.md` (слой 1)
- Спеки и таблицы длинные: читай их инструментом Read кусками, не через `cat` в Bash.
- Разбор ревью issue #3: `docs/review-3-gateway-skeleton.md`; ретро: `docs/retro-3-gateway-skeleton.md`
- Команда проверки (запусти перед каждым PR): `make check` (= `./scripts/check.sh`: go vet, golangci-lint, go test -race). Хук и CI вызывают её же. Включить хук: `make hooks`. Тесты гейтвея поднимают настоящий `redis-server`: он должен быть в PATH (`brew install redis`). `docker compose up` требует переменную `GATEWAY_CLIENT_KEY`.
- Защита `main` (включена): прямой пуш запрещён, мердж только через PR с зелёной проверкой `check`; у владельца репозитория есть обход. Работай в ветке, открывай PR.
- Локальные модели: сервер `llama-swap` на `localhost:7382`, конфиг `~/models/llama-swap.yaml`. Профили: `research` и `agent` (Qwen3.5-4B, контекст 32k и 16k), `coder` (omnicoder-9B, 16k). Модели медленные, окно маленькое.

## Agent skills

### Issue tracker

Задачи ведутся в GitHub Issues (`gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Стандартный словарь: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: один `GLOSSARY.md` + `docs/adr/` в корне. See `docs/agents/domain.md`.
