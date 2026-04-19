# Changelog

All notable changes to this project are documented in this file.

Versioning format:
- **MAJOR** — breaking API or behavior changes
- **MINOR** — new functionality without breaking the main workflow
- **PATCH** — fixes and small improvements

Current version: **0.3.1**

## [0.3.1] - 2026-04-19

### Added
- Added the `GET /v1/version` endpoint to return the current service version.

### Changed
- `GET /health` now returns the `version` field.
- `GET /v1/config/state` now also returns the `version` field.
- Project version updated to `0.3.1`.

## [0.3.0] - 2026-04-19

### Added
- Added a new **promptless/direct-chat** mode for `/v1/solve`.
- Added the global `chat_policy` section in `config.yaml`.
- Added chat fields `rest_until` and `logged_out`.
- Added endpoints for chat state management:
  - `POST /v1/chat/rest`
  - `POST /v1/chats/rest`
  - `POST /v1/chat/rest/clear`
  - `POST /v1/chats/rest/clear`
  - `POST /v1/chat/logged-out`
  - `POST /v1/chats/logged-out`
  - `POST /v1/chat/logged-out/clear`
  - `POST /v1/chats/logged-out/clear`

### Changed
- The scenario without `prompt_id` remains backward compatible and still uses `default`.
- The new promptless mode is enabled **explicitly** by passing `options.prompt_id: null` or `options.prompt_id: ""`.
- In direct mode, the chat session now correctly updates `page_url` and `chat_id` after the first user message.

### Behavior
- In promptless/direct mode, the first request receives a free direct chat from the `__direct__` pool if the chat has been idle long enough.
- If no suitable direct chat is available, a new direct chat is created.
- Further access to such a chat is intended to happen **only via direct `chat_url`**.
- Chats marked with active `rest_until` or `logged_out` flags are excluded from reuse and cannot be used even via direct `chat_url`.
- `rest_until` is cleared automatically after its TTL expires.
- `logged_out` is cleared manually only.

## [0.2.0] - 2026-04-19

### Added
- Added **hot reload** for `config.yaml` without restarting the process.
- Added endpoints:
  - `GET /v1/config/state`
  - `POST /v1/config/reload`

### Changed
- Runtime components are rebuilt on the fly when configuration changes are detected.
- If the YAML becomes invalid, the service continues running with the last valid configuration.
- Added synchronization of `profiles` and `socks` with the current YAML so removed entries do not remain available for auto-selection.

## [0.1.0] - 2026-04-19

### Added
- Added the `manual_only: true` profile flag in `config.yaml`.

### Behavior
- `manual_only` profiles are excluded from automatic selection.
- `manual_only` profiles are excluded from reuse through old `chat_sessions`.
- Such profiles are still available when `options.profile_id` is specified explicitly.
