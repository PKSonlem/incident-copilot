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
- Тикеты (`/to-tickets`): создавай их в порядке графа зависимостей, блокеры раньше зависимых. В каждом тикете в конце блок «Порядок»: `Blocked by: #N`, `Можно параллельно с: #N, #M` (если зависимости не пересекаются) и `Правит те же места, что: #N` (если параллельный запуск даст конфликт слияния). Тикет запускается только когда все его блокеры смерджены в `main`. Сводная таблица батчей лежит комментарием в родительской спеке.
- Не коммить в репозиторий журналы своей работы (разбор ревью, ретро, заметки по итерациям). Результат ревью и выводы идут в описание PR или комментарий к issue.
- Гейтвей (#2) собирается в накопительной ветке `release/gateway`: PR каждого тикета или батча открывай в неё, а не в `main`, и новую интеграционную ветку не создавай. Подтягивай `main` в неё, когда он уходит вперёд. В `main` ветка идёт одним PR, когда все тикеты влиты, `make check` зелёный и `docker compose up` поднимает стенд. Слова `Closes #N` закрывают issue только при мердже в `main`, поэтому тикет, влитый в `release/gateway`, закрывай вручную с комментарием.
- Защита `main` (включена): прямой пуш запрещён, мердж только через PR с зелёной проверкой `check`; у владельца репозитория есть обход. Работай в ветке, открывай PR.
- Локальные модели: сервер `llama-swap` на `localhost:7382`, конфиг `~/models/llama-swap.yaml`. Профили: `research` и `agent` (Qwen3.5-4B, контекст 32k и 16k), `coder` (omnicoder-9B, 16k). Модели медленные, окно маленькое.

## Agent skills

### Issue tracker

Задачи ведутся в GitHub Issues (`gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Стандартный словарь: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: один `GLOSSARY.md` + `docs/adr/` в корне. See `docs/agents/domain.md`.
