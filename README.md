# QwenHandlerSupervisor — Multi-Container Orchestrator

QwenHandlerSupervisor is a **FastAPI-based orchestrator** that routes `/v1/solve` requests to a pool of upstream “browser containers” (workers). It adds:

- **Multi-container routing** with health/busy checks and manual container exclusion via chat locks
- **Profile isolation** (per-request browser profile directory) with **process-local locking** to prevent concurrent use
- **SOCKS proxy management** (by `socks_id` or direct URL override)
- **Prompt registry** backed by files (start prompt sent once when a new chat is created)
- **SQLite-backed state and observability** (jobs, attempts, chat sessions, usage reports)
- Optional **per-container I/O logging** (JSONL, with secret redaction)

> This repository ships the *full multi-container* mode. `CONFIG_PATH` is required to start the app.

---

## Contents

- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Upstream worker (Qwen Camoufox Inference Worker)](#upstream-worker-qwen-camoufox-inference-worker)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running](#running)
- [API](#api)
- [Examples](#examples)
- [SQLite data model](#sqlite-data-model)
- [Logging & observability](#logging--observability)
- [Operational notes & caveats](#operational-notes--caveats)
- [Troubleshooting](#troubleshooting)

---

## Quick start

1) Create a virtual environment and install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pyyaml
```

2) Prepare `config.yaml` (see [Configuration](#configuration)) and prompt files.

3) Run:

```bash
export CONFIG_PATH=./config.yaml
export SQLITE_PATH=./data/orchestrator.sqlite
uvicorn src.app.main:app --host 0.0.0.0 --port 9000
```

4) Test health:

```bash
curl -s http://127.0.0.1:9000/health
```

---

## How it works

At a high level the request path for `/v1/solve` is:

1. **PromptRegistry** resolves `prompt_id` → reads the prompt file (cached by mtime).
2. **ProfileManager** resolves `profile_id` → browser `profile_value` + SOCKS (`socks_id` → URL) + `allowed_containers`.
3. **ProfileLock** prevents using the same `profile_id` concurrently.
4. **ContainerSelector** picks an enabled, non-busy container that is not excluded by chat locks.
5. **ChatManager** reuses an existing chat session or creates a new one:
   - when creating a new chat it sends `start_prompt` once and stores `page_url` containing `/c/<chat_id>`.
6. **UpstreamClient** calls upstream `/analyze` (or legacy `/analyze_text`) using:
   - `url = chat_session.page_url`
   - `profile = profile_value`
   - `socks = socks_url`

All state is recorded in SQLite:
- `jobs` — one row per `/v1/solve`
- `job_attempts` — per-container attempt
- `chat_sessions` — persistent chat URLs and usage counters
- `profiles` / `socks` — inventory seeded from YAML

---

---

<a id="upstream-worker-qwen-camoufox-inference-worker"></a>

## Upstream worker: Qwen Camoufox Inference Worker

This orchestrator **does not drive the browser itself**. Each configured container is expected to run an upstream HTTP service (one per container) named:

**Qwen Camoufox Inference Worker** — an HTTP service that controls **exactly one** Camoufox/Playwright browser instance inside a container and, via the **Qwen Chat web UI**, performs:

- **Text analysis** (clipboard → paste → send → wait for reply)
- **Image analysis** (copy image into the system clipboard → Ctrl+V into the input field → wait for attachment → send → wait for reply)

The worker is designed for **one active browser and one task at a time** (hard serialization). When the worker is busy, it should respond with **HTTP 423**; QwenHandlerSupervisor treats this as a transient “busy” signal and routes the job to another container (if available).

Repository (upstream component): https://github.com/kilax9276/qwen_inference_worker

### Worker endpoints expected by this orchestrator

The orchestrator talks to each worker using these endpoints (see `src/app/upstream_client.py`):

- `GET /health`
- `GET /status`
- `POST /open` — open a Qwen chat URL (optionally with `profile` + `socks`)
- `POST /analyze` — analyze text or image (supports backward-compatible fallbacks like `/analyze_text`)

> The exact worker implementation may be private or versioned separately; the orchestrator primarily relies on the endpoints and on **HTTP 423** to detect “busy”.

## Installation

### Requirements

- Python **3.10+** recommended
- `pip` / virtualenv
- One or more upstream worker services (containers) that implement the **Upstream API contract** (see below)

### Dependencies

The project pins core deps in `requirements.txt`:

- `fastapi`, `uvicorn`
- `httpx`
- `pydantic` (+ `pydantic-settings`)
- tests: `pytest`, `pytest-asyncio`

**Additionally required for `CONFIG_PATH` mode:** `pyyaml` (the config loader imports `yaml` at runtime).

---

## Configuration

Configuration is loaded from YAML file referenced by `CONFIG_PATH`.

`containers[].base_url` must point to the **Qwen Camoufox Inference Worker** HTTP service running inside that container.

### Minimal `config.yaml`

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

### Config schema

Top-level keys:

- `allow_socks_override` *(bool, default: true)*  
  Allows per-request proxy override via `options.socks_override`.

- `containers` *(list)*  
  Each item:
  - `id` *(str)* — container identifier (used in routing and reporting)
  - `base_url` *(str)* — upstream base URL (e.g. `http://127.0.0.1:8001`)
  - `enabled` *(bool, default: true)* — if `false`, container is never selected
  - `weight` *(int, default: 1)* — reserved (currently selection is round-robin over available)
  - `timeouts.connect_seconds` *(float, default: 10)*
  - `timeouts.read_seconds` *(float, default: 120)*
  - `analyze_retries` *(int, default: 1)* — retry count for transport errors (capped)

- `socks` *(list)*  
  Each item:
  - `socks_id` *(str)*
  - `url` *(str)* — socks URL (credentials may be embedded)

- `profiles` *(list)*  
  Each item:
  - `profile_id` *(str)*
  - `profile_value` *(str)* — path to a browser profile directory (passed to upstream)
  - `socks_id` *(str, optional)* — default proxy for that profile
  - `allowed_containers` *(list[str])* — containers that may use this profile
  - `max_uses` *(int, optional)* — soft cap; when reached, profile is skipped by auto-selection
  - `pending_replace` *(bool)* — if `true`, profile is skipped by auto-selection

- `prompts` *(list)*  
  Each item:
  - `prompt_id` *(str)*
  - `file` *(str)* — file path; **relative paths are resolved against the config.yaml directory**
  - `default_max_chat_uses` *(int, default: 50)* — chat reuse limit

- `container_io_log` *(object)*  
  Optional per-container I/O logging:
  - `enabled` *(bool)*
  - `dir` *(str, default: ./logs/container-io)* — directory for `*.jsonl` files
  - `max_bytes` *(int)*, `backup_count` *(int)* — rotation
  - `include_bodies` *(bool)* — log request/response bodies
  - `redact_secrets` *(bool)* — mask proxy credentials and common secret-like fields
  - `max_field_chars` *(int)* — truncate large string fields
  - `level` *(str)* — log level for IO logs

### Prompt files

Each prompt file contains **start_prompt** text. When a new chat is created, the orchestrator sends this text once to upstream; upstream should return `page_url` containing `/c/<chat_id>`. This URL is then reused for subsequent messages.

---

## Running

### Environment variables

- `CONFIG_PATH` *(required)* — path to YAML config
- `SQLITE_PATH` *(optional, default: `./data/orchestrator.sqlite`)* — SQLite file path
- `ORCH_LOG_LEVEL` *(optional, default: INFO)* — orchestrator log level

### Uvicorn

```bash
export CONFIG_PATH=./config.yaml
export SQLITE_PATH=./data/orchestrator.sqlite

uvicorn src.app.main:app --host 0.0.0.0 --port 9000
```

### PM2 example

See `ecosystem.config.js` for a production-style PM2 configuration (sets absolute paths for `CONFIG_PATH` and `SQLITE_PATH`).

---

## API

Base URL: `http://<host>:9000`

### Health

`GET /health`

Response:

```json
{"ok": true}
```

### Container status

`GET /v1/status?container_id=<id>`

If `container_id` is omitted, returns status of the first enabled container.

`GET /v1/status/all` returns status for all enabled containers + DB path.

### Solve

`POST /v1/solve`

#### Request body

```json
{
  "request_id": "optional-id",
  "input": {
    "text": "string (optional)",
    "image_b64": "base64 (optional)",
    "image_ext": "png|jpg|... (required when image_b64 is set)"
  },
  "options": {
    "prompt_id": "default",
    "profile_id": "p1",
    "socks_override": "s1 or socks5://user:pass@host:port",
    "force_new_chat": false,
    "max_chat_uses": 50,
    "chat_url": "https://chat.qwen.ai/c/<id>",
    "include_debug": false
  }
}
```

Rules:
- You must provide `input.text` and/or `input.image_b64`.
- If `image_b64` is provided, `image_ext` is required.
- In typical deployments `options.profile_id` is required (auto-selection exists but is mainly for internal use).
- `options.chat_url` pins the request to an existing stored chat session (must exist in SQLite).

#### Response body

Success:

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

Error:

```json
{
  "ok": false,
  "error": { "code": "CONTAINER_BUSY", "message": "..." },
  "meta": { "job_id": "...", "request_id": "..." }
}
```

Common error codes:
- `INVALID_REQUEST` (HTTP 400)
- `PROFILE_BUSY` (HTTP 503) — profile is locked by another in-flight request
- `CONTAINER_BUSY` (HTTP 503) — no available containers / upstream returned busy (HTTP 423)
- `UPSTREAM_ERROR` (HTTP 502) — upstream 5xx or transport failures after retries
- `INTERNAL_ERROR` (HTTP 500)

### Chat locks (manual container exclusion)

Locks are stored in SQLite (`chat_sessions.locked_by/locked_until`). While **any chat** in a container is locked and not expired, the container is excluded from routing.

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

### Reports

All report endpoints require `from` and `to` query params in ISO8601 (offset-naive timestamps are treated as UTC).

- `GET /v1/reports/containers?from=...&to=...&limit=50&offset=0`
- `GET /v1/reports/profiles?from=...&to=...&limit=50&offset=0`
- `GET /v1/reports/prompts?from=...&to=...&limit=50&offset=0`

Example:

```bash
curl -s "http://127.0.0.1:9000/v1/reports/containers?from=2026-02-01T00:00:00%2B00:00&to=2026-02-03T00:00:00%2B00:00"
curl -s "http://127.0.0.1:9000/v1/reports/profiles?from=2026-02-01T00:00:00%2B00:00&to=2026-02-03T00:00:00%2B00:00"
curl -s "http://127.0.0.1:9000/v1/reports/prompts?from=2026-02-01T00:00:00%2B00:00&to=2026-02-03T00:00:00%2B00:00"
```

---

## Examples

### Text solve

```bash
curl -s http://127.0.0.1:9000/v1/solve   -H 'Content-Type: application/json'   -d '{
    "input": { "text": "Explain JWT in one paragraph" },
    "options": { "prompt_id": "default", "profile_id": "p1" }
  }'
```

### Force a new chat

```bash
curl -s http://127.0.0.1:9000/v1/solve   -H 'Content-Type: application/json'   -d '{
    "input": { "text": "Start fresh context" },
    "options": { "prompt_id": "default", "profile_id": "p1", "force_new_chat": true }
  }'
```

### Pin to an existing chat URL

> `chat_url` must exist in SQLite (was created by a previous request).

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

### Python client (minimal)

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

## SQLite data model

SQLite is initialized automatically on startup (WAL mode when available).

Main tables:

- `socks(socks_id, url, created_at, updated_at)`
- `profiles(profile_id, profile_value, socks_id, allowed_containers_json, uses_count, max_uses, pending_replace, ...)`
- `chat_sessions(id, container_id, prompt_id, profile_id, socks_id, chat_id, page_url, uses_count, disabled, locked_by, locked_until, ...)`
- `jobs(job_id, request_id, prompt_id, selected_prompt_id, decision_mode, ..., status, result_text, error_code, ...)`
- `job_attempts(attempt_id, job_id, container_id, prompt_id, role, ..., status, result_text, error_code, ...)`

---

## Logging & observability

### Orchestrator logs

Controlled via `ORCH_LOG_LEVEL` (or `LOG_LEVEL`). The orchestrator emits structured JSON messages for key lifecycle events (solve start/done, failures, etc.).

### Container I/O logs (optional)

If enabled in `config.yaml` under `container_io_log`, the orchestrator writes JSONL logs per container:

```
logs/container-io/camoufox-1.jsonl
logs/container-io/camoufox-2.jsonl
...
```

Each line includes timestamps, request/response, status codes, duration, and best-effort redaction of sensitive values (e.g. proxy passwords).

---

## Operational notes & caveats

- **Profile locks are process-local.** If you run `uvicorn` with multiple workers, each worker has its own lock map. For strict safety, run with a single worker or implement a distributed lock.
- **Busy is best-effort.** The orchestrator checks upstream `/status`, but upstream can still become busy later (e.g. during start prompt).
- **`allowed_containers` is enforced.** A profile can only run on containers listed in its `allowed_containers`.
- **Relative file paths** in config (prompt files, IO log dir) are resolved against the directory containing `config.yaml`.
- **`include_debug`** is intended for development only. If you rely on it, validate its output against your current schema.

---

## Troubleshooting

### `RuntimeError: CONFIG_PATH is required`

Set `CONFIG_PATH` to a valid YAML file path before starting the app.

### `PyYAML is required for CONFIG_PATH mode`

Install PyYAML:

```bash
pip install pyyaml
```

### `CONTAINER_BUSY` (HTTP 503)

- All containers are busy or locked
- A pinned `chat_url` points to a container that is currently busy
- Upstream returned HTTP 423

Try:
- `/v1/status/all` to inspect busy state
- unlocking chats for that container
- adding more containers

### `PROFILE_BUSY` (HTTP 503)


Copyright © kilax9276 (Kolobov Aleksei)

Telegram: @kilax9276

Another request is currently using the same profile. Use different `profile_id` or wait for the in-flight request to finish.

#copyright "Kilax @kilax9276"
---