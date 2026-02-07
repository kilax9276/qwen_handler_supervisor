# QwenHandlerSupervisor — Multi-Container Orchestrator

QwenHandlerSupervisor — это **оркестратор на базе FastAPI**, который маршрутизирует запросы `/v1/solve` в пул upstream‑воркеров (контейнеров с браузером). Он добавляет:

- **Маршрутизацию по нескольким контейнерам** с проверкой busy/health и ручным исключением контейнеров через chat‑lock
- **Изоляцию профилей** (директория профиля браузера на запрос) с **локом на процесс**, чтобы исключить параллельное использование
- **Управление SOCKS‑прокси** (через `socks_id` или прямой URL‑override)
- **Реестр промптов** на файловой основе (стартовый промпт отправляется один раз при создании нового чата)
- **SQLite‑бэкенд для состояния и наблюдаемости** (jobs, attempts, chat sessions, usage reports)
- Опциональное **поконтейнерное I/O‑логирование** (JSONL, с редактированием секретов)

> В этом репозитории поставляется *полный multi-container режим*. Для запуска обязателен `CONFIG_PATH`.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Как это работает](#как-это-работает)
- [Upstream воркер (Qwen Camoufox Inference Worker)](#upstream-worker-qwen-camoufox-inference-worker)
- [Установка](#установка)
- [Настройка](#настройка)
- [Запуск](#запуск)
- [API](#api)
- [Примеры](#примеры)
- [Модель данных SQLite](#модель-данных-sqlite)
- [Логи и наблюдаемость](#логи-и-наблюдаемость)
- [Замечания по эксплуатации и ограничения](#замечания-по-эксплуатации-и-ограничения)
- [Troubleshooting](#troubleshooting)

---

## Быстрый старт

1) Создайте виртуальное окружение и установите зависимости:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pyyaml
```

2) Подготовьте `config.yaml` (см. [Настройка](#настройка)) и файлы промптов.

3) Запустите:

```bash
export CONFIG_PATH=./config.yaml
export SQLITE_PATH=./data/orchestrator.sqlite
uvicorn src.app.main:app --host 0.0.0.0 --port 9000
```

4) Проверьте health:

```bash
curl -s http://127.0.0.1:9000/health
```

---

## Как это работает

В общих чертах pipeline для `/v1/solve`:

1. **PromptRegistry** резолвит `prompt_id` → читает файл промпта (кэш по mtime).
2. **ProfileManager** резолвит `profile_id` → `profile_value` + SOCKS (`socks_id` → URL) + `allowed_containers`.
3. **ProfileLock** запрещает параллельное использование одного и того же `profile_id`.
4. **ContainerSelector** выбирает enabled, не busy контейнер, не исключённый chat‑lock’ами.
5. **ChatManager** переиспользует существующую chat session или создаёт новую:
   - при создании нового чата он отправляет `start_prompt` один раз и сохраняет `page_url`, содержащий `/c/<chat_id>`.
6. **UpstreamClient** вызывает upstream `/analyze` (или legacy `/analyze_text`) с параметрами:
   - `url = chat_session.page_url`
   - `profile = profile_value`
   - `socks = socks_url`

Всё состояние фиксируется в SQLite:
- `jobs` — 1 строка на `/v1/solve`
- `job_attempts` — попытки выполнения по контейнерам
- `chat_sessions` — постоянные chat URLs и счётчики использования
- `profiles` / `socks` — инвентарь, сидится из YAML

---

---

<a id="upstream-worker-qwen-camoufox-inference-worker"></a>

## Upstream воркер: Qwen Camoufox Inference Worker

Этот оркестратор **не управляет браузером напрямую**. Каждый контейнер из `config.yaml` должен поднимать upstream HTTP‑сервис (по одному на контейнер):

**Qwen Camoufox Inference Worker** — HTTP‑сервис, который управляет **ровно одним** экземпляром браузера Camoufox/Playwright внутри контейнера и через **веб‑интерфейс Qwen Chat** выполняет:

- **Анализ текста** (буфер обмена → вставка → отправка → ожидание ответа)
- **Анализ изображений** (копирование изображения в системный буфер обмена → Ctrl+V в поле ввода → ожидание вложения → отправка → ожидание ответа)

Сервис рассчитан на **один активный браузер и одну задачу одновременно** (жёсткая сериализация). Когда воркер занят, он должен отвечать **HTTP 423**; QwenHandlerSupervisor воспринимает это как временную «занятость» и пытается выполнить задачу в другом контейнере (если доступен).

Репозиторий (upstream компонент): https://github.com/kilax9276/qwen_inference_worker

### Какие эндпоинты воркера ожидает этот оркестратор

Оркестратор обращается к каждому воркеру по таким эндпоинтам (см. `src/app/upstream_client.py`):

- `GET /health`
- `GET /status`
- `POST /open` — открыть Qwen chat URL (опционально с `profile` + `socks`)
- `POST /analyze` — анализ текста или изображения (есть backward‑compatible fallback’и вроде `/analyze_text`)

> Конкретная реализация воркера может быть приватной или версионироваться отдельно; оркестратору важно соответствие эндпоинтам и **HTTP 423** как сигналу «busy».

## Установка

### Требования

- Рекомендуется Python **3.10+**
- `pip` / virtualenv
- Один или несколько upstream‑воркеров (контейнеров), реализующих **Upstream API контракт** (см. ниже)

### Зависимости

В `requirements.txt` зафиксированы основные зависимости:

- `fastapi`, `uvicorn`
- `httpx`
- `pydantic` (+ `pydantic-settings`)
- для тестов: `pytest`, `pytest-asyncio`

**Дополнительно обязательно для режима `CONFIG_PATH`:** `pyyaml` (загрузчик конфига импортирует `yaml` во время выполнения).

---

## Настройка

Конфигурация загружается из YAML файла, на который указывает `CONFIG_PATH`.

### Минимальный `config.yaml`

```yaml
allow_socks_override: true

containers:
  - id: camoufox-1
    base_url: http://127.0.0.1:8001
    enabled: true
    timeouts:
      connect_seconds: 10
      read_seconds: 120
    analyze_retries: 1

socks:
  - socks_id: s1
    url: socks5://user:pass@1.2.3.4:1080

profiles:
  - profile_id: p1
    profile_value: /data/profiles/profile-001
    socks_id: s1
    allowed_containers: [camoufox-1]
    max_uses: 100
    pending_replace: false

prompts:
  - prompt_id: default
    file: ./prompts/default.txt
    default_max_chat_uses: 50

container_io_log:
  enabled: false
```

### Схема конфига

Ключи верхнего уровня:

- `allow_socks_override` *(bool, default: true)*  
  Разрешает override прокси на запрос через `options.socks_override`.

- `containers` *(list)*  
  Каждый элемент:
  - `id` *(str)* — идентификатор контейнера (используется в маршрутизации и отчётах)
  - `base_url` *(str)* — upstream base URL (например `http://127.0.0.1:8001`)
  - `enabled` *(bool, default: true)* — если `false`, контейнер никогда не выбирается
  - `weight` *(int, default: 1)* — зарезервировано (сейчас выбор — round-robin по доступным)
  - `timeouts.connect_seconds` *(float, default: 10)*
  - `timeouts.read_seconds` *(float, default: 120)*
  - `analyze_retries` *(int, default: 1)* — ретраи на сетевые ошибки (с ограничением сверху)

- `socks` *(list)*  
  Каждый элемент:
  - `socks_id` *(str)*
  - `url` *(str)* — socks URL (может содержать креды)

- `profiles` *(list)*  
  Каждый элемент:
  - `profile_id` *(str)*
  - `profile_value` *(str)* — путь к директории профиля браузера (передаётся в upstream)
  - `socks_id` *(str, optional)* — дефолтный прокси для профиля
  - `allowed_containers` *(list[str])* — какие контейнеры могут использовать этот профиль
  - `max_uses` *(int, optional)* — мягкий лимит; при достижении профиль пропускается в авто‑выборе
  - `pending_replace` *(bool)* — если `true`, профиль пропускается в авто‑выборе

- `prompts` *(list)*  
  Каждый элемент:
  - `prompt_id` *(str)*
  - `file` *(str)* — путь к файлу; **относительные пути резолвятся относительно директории `config.yaml`**
  - `default_max_chat_uses` *(int, default: 50)* — лимит reuse чата

- `container_io_log` *(object)*  
  Опциональное I/O‑логирование по контейнерам:
  - `enabled` *(bool)*
  - `dir` *(str, default: ./logs/container-io)* — директория для `*.jsonl`
  - `max_bytes` *(int)*, `backup_count` *(int)* — ротация
  - `include_bodies` *(bool)* — логировать request/response bodies
  - `redact_secrets` *(bool)* — маскировать пароли прокси и ряд секрет‑полей
  - `max_field_chars` *(int)* — обрезать большие строки
  - `level` *(str)* — уровень логов для IO логов

### Файлы промптов

Каждый prompt‑файл содержит **start_prompt**. Когда создаётся новый чат, оркестратор отправляет этот текст один раз в upstream; upstream должен вернуть `page_url`, содержащий `/c/<chat_id>`. Этот URL дальше переиспользуется для сообщений.

---

## Запуск

### Переменные окружения

- `CONFIG_PATH` *(обязательно)* — путь к YAML конфигу
- `SQLITE_PATH` *(опционально, default: `./data/orchestrator.sqlite`)* — путь к SQLite файлу
- `ORCH_LOG_LEVEL` *(опционально, default: INFO)* — уровень логов оркестратора

### Uvicorn

```bash
export CONFIG_PATH=./config.yaml
export SQLITE_PATH=./data/orchestrator.sqlite

uvicorn src.app.main:app --host 0.0.0.0 --port 9000
```

### Пример для PM2

См. `ecosystem.config.js` — пример конфигурации PM2 для продового запуска (задаются абсолютные пути для `CONFIG_PATH` и `SQLITE_PATH`).

---

## API

Base URL: `http://<host>:9000`

### Health

`GET /health`

Ответ:

```json
{"ok": true}
```

### Статус контейнеров

`GET /v1/status?container_id=<id>`

Если `container_id` не указан — возвращается статус первого enabled контейнера.

`GET /v1/status/all` возвращает статусы всех enabled контейнеров + путь к БД.

### Solve

`POST /v1/solve`

#### Тело запроса

```json
{
  "request_id": "optional-id",
  "input": {
    "text": "string (optional)",
    "image_b64": "base64 (optional)",
    "image_ext": "png|jpg|... (обязательно когда задан image_b64)"
  },
  "options": {
    "prompt_id": "default",
    "profile_id": "p1",
    "socks_override": "s1 или socks5://user:pass@host:port",
    "force_new_chat": false,
    "max_chat_uses": 50,
    "chat_url": "https://chat.qwen.ai/c/<id>",
    "include_debug": false
  }
}
```

Правила:
- Нужно передать `input.text` и/или `input.image_b64`.
- Если передан `image_b64`, то обязателен `image_ext`.
- В типичных установках `options.profile_id` должен быть задан (авто‑выбор существует, но в основном для внутреннего использования).
- `options.chat_url` «прибивает» запрос к существующей chat session (она должна быть в SQLite).

#### Тело ответа

Успех:

```json
{
  "ok": true,
  "final": { "text": "..." },
  "meta": {
    "job_id": "...",
    "request_id": "...",
    "prompt_id_selected": "default",
    "container_ids_used": ["camoufox-1"],
    "profile_id": "p1",
    "socks_id": "s1",
    "page_url": "https://chat.qwen.ai/c/abc123",
    "started_at": "...",
    "finished_at": "..."
  }
}
```

Ошибка:

```json
{
  "ok": false,
  "error": { "code": "CONTAINER_BUSY", "message": "..." },
  "meta": { "job_id": "...", "request_id": "..." }
}
```

Типовые коды ошибок:
- `INVALID_REQUEST` (HTTP 400)
- `PROFILE_BUSY` (HTTP 503) — профиль занят (заблокирован другим in-flight запросом)
- `CONTAINER_BUSY` (HTTP 503) — нет доступных контейнеров / upstream вернул busy (HTTP 423)
- `UPSTREAM_ERROR` (HTTP 502) — upstream 5xx или transport ошибки после ретраев
- `INTERNAL_ERROR` (HTTP 500)

### Chat locks (ручное исключение контейнера)

Локи хранятся в SQLite (`chat_sessions.locked_by/locked_until`). Пока **хотя бы один чат** в контейнере залочен и TTL не истёк — контейнер исключается из маршрутизации.

Lock:

`POST /v1/chats/lock` *(alias: `/v1/chat/lock`)*

```json
{
  "chat_url": "https://chat.qwen.ai/c/abc123",
  "locked_by": "operator",
  "ttl_seconds": 600
}
```

Unlock:

`POST /v1/chats/unlock` *(alias: `/v1/chat/unlock`)*

```json
{
  "chat_url": "https://chat.qwen.ai/c/abc123",
  "locked_by": "operator"
}
```

### Admin-эндпоинты

> **Важно:** в исходном README проекта упоминаются пути `/v1/admin/*`, но **в текущем коде нет роутов вида `/v1/admin/...`**.
> В актуальной реализации вместо этого используйте:
>
> - `GET /v1/status/all` — увидеть включённые контейнеры и их upstream-статус
> - `config.yaml` — посмотреть контейнеры / socks / профили / промпты из конфигурации
> - SQLite (`socks`, `profiles`, `chat_sessions`, `jobs`, `job_attempts`) — для операционных данных

Если вам нужен admin API, можно добавить отдельный роутер, который будет отдавать:
- containers из `config.yaml`
- socks из таблицы `socks`
- profiles из таблицы `profiles`
- prompts из конфигурации (реестр PromptRegistry)

### Отчёты (Reports)

Все report endpoints требуют query params `from` и `to` в ISO8601 (naive timestamps трактуются как UTC).

- `GET /v1/reports/containers?from=...&to=...&limit=50&offset=0`
- `GET /v1/reports/profiles?from=...&to=...&limit=50&offset=0`
- `GET /v1/reports/prompts?from=...&to=...&limit=50&offset=0`

Пример:

```bash
curl -s "http://127.0.0.1:9000/v1/reports/containers?from=2026-02-01T00:00:00%2B00:00&to=2026-02-03T00:00:00%2B00:00"
curl -s "http://127.0.0.1:9000/v1/reports/profiles?from=2026-02-01T00:00:00%2B00:00&to=2026-02-03T00:00:00%2B00:00"
curl -s "http://127.0.0.1:9000/v1/reports/prompts?from=2026-02-01T00:00:00%2B00:00&to=2026-02-03T00:00:00%2B00:00"
```

---

## Примеры

### Текстовый solve

```bash
curl -s http://127.0.0.1:9000/v1/solve   -H 'Content-Type: application/json'   -d '{
    "input": { "text": "Explain JWT in one paragraph" },
    "options": { "prompt_id": "default", "profile_id": "p1" }
  }'
```

### Принудительно создать новый чат

```bash
curl -s http://127.0.0.1:9000/v1/solve   -H 'Content-Type: application/json'   -d '{
    "input": { "text": "Start fresh context" },
    "options": { "prompt_id": "default", "profile_id": "p1", "force_new_chat": true }
  }'
```

### Привязка к существующему chat URL

> `chat_url` должен существовать в SQLite (быть созданным предыдущим запросом).

```bash
curl -s http://127.0.0.1:9000/v1/solve   -H 'Content-Type: application/json'   -d '{
    "input": { "text": "Continue from previous context" },
    "options": {
      "prompt_id": "default",
      "profile_id": "p1",
      "chat_url": "https://chat.qwen.ai/c/abc123"
    }
  }'
```

### Image solve (base64)

```bash
curl -s http://127.0.0.1:9000/v1/solve   -H 'Content-Type: application/json'   -d '{
    "input": { "image_b64": "<BASE64>", "image_ext": "png" },
    "options": { "prompt_id": "default", "profile_id": "p1" }
  }'
```

### Python клиент (минимальный)

```python
import requests

payload = {
  "input": {"text": "Summarize OAuth2"},
  "options": {"prompt_id": "default", "profile_id": "p1"}
}

r = requests.post("http://127.0.0.1:9000/v1/solve", json=payload, timeout=300)
r.raise_for_status()
print(r.json()["final"]["text"])
```

---

## Модель данных SQLite

SQLite инициализируется автоматически при старте (WAL mode при доступности).

Основные таблицы:

- `socks(socks_id, url, created_at, updated_at)`
- `profiles(profile_id, profile_value, socks_id, allowed_containers_json, uses_count, max_uses, pending_replace, ...)`
- `chat_sessions(id, container_id, prompt_id, profile_id, socks_id, chat_id, page_url, uses_count, disabled, locked_by, locked_until, ...)`
- `jobs(job_id, request_id, prompt_id, selected_prompt_id, decision_mode, ..., status, result_text, error_code, ...)`
- `job_attempts(attempt_id, job_id, container_id, prompt_id, role, ..., status, result_text, error_code, ...)`

---

## Логи и наблюдаемость

### Логи оркестратора

Управляются через `ORCH_LOG_LEVEL` (или `LOG_LEVEL`). Оркестратор пишет структурированные JSON сообщения для ключевых событий (solve start/done, ошибки и т.д.).

### Container I/O logs (опционально)

Если включено в `config.yaml` в секции `container_io_log`, оркестратор пишет JSONL‑логи по контейнерам:

```
logs/container-io/camoufox-1.jsonl
logs/container-io/camoufox-2.jsonl
...
```

Каждая строка содержит timestamp, request/response, status codes, duration и best‑effort редактирование чувствительных значений (например, пароли прокси).

---

## Замечания по эксплуатации и ограничения

- **Лок профиля — локальный для процесса.** Если запускать `uvicorn` с несколькими воркерами, у каждого воркера будет своя карта локов. Для строгой безопасности запускайте один воркер или внедрите distributed lock.
- **Busy — best-effort.** Оркестратор проверяет `/status`, но контейнер может стать busy позже (например, во время отправки стартового промпта).
- **`allowed_containers` строго применяется.** Профиль может работать только на контейнерах, перечисленных в `allowed_containers`.
- **Относительные пути** в конфиге (файлы промптов, директория IO логов) резолвятся относительно директории, содержащей `config.yaml`.
- **`include_debug`** рассчитан на дев‑режим. Если вы используете это поле, проверьте формат вывода на вашей версии схем.

---

## Troubleshooting

### `RuntimeError: CONFIG_PATH is required`

Перед стартом приложения задайте `CONFIG_PATH` и убедитесь, что путь указывает на валидный YAML.

### `PyYAML is required for CONFIG_PATH mode`

Установите PyYAML:

```bash
pip install pyyaml
```

### `CONTAINER_BUSY` (HTTP 503)

- Все контейнеры busy или залочены
- Пинованный `chat_url` указывает на контейнер, который сейчас busy
- Upstream вернул HTTP 423

Что делать:
- `/v1/status/all` чтобы посмотреть busy state
- снять локи чатов для этого контейнера
- добавить больше контейнеров

### `PROFILE_BUSY` (HTTP 503)

Другой запрос уже использует тот же профиль. Используйте другой `profile_id` или дождитесь завершения текущего запроса.

#copyright "Kilax @kilax9276"
---