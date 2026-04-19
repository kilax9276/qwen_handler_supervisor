# Release notes (see CHANGELOG.md for version history)

Current version: **0.3.1**

# Promptless direct-chat mode + chat rest/logged_out markers

Что добавлено:

- Новый режим **promptless/direct** для `/v1/solve`.
  - Старое поведение сохранено: если `prompt_id` не передан, используется `default`.
  - Новый режим включается **явно** через `options.prompt_id: null` или `options.prompt_id: ""`.
  - В этом режиме базовый prompt не используется.
  - Первый запрос получает свободный direct-chat из пула `__direct__`, если чат достаточно долго не использовался.
  - Если подходящего direct-чата нет, создаётся новый direct-chat.
  - Для продолжения именно этого direct-чата нужно передавать `chat_url`.

- Глобальные настройки в `config.yaml`:

```yaml
chat_policy:
  promptless_idle_seconds: 900
  default_rest_ttl_seconds: 900
  default_max_chat_uses: 50
```

- Новые признаки для chat_sessions:
  - `rest_until`
  - `logged_out`

- Новые endpoint'ы:
  - `POST /v1/chat/rest`
  - `POST /v1/chats/rest`
  - `POST /v1/chat/rest/clear`
  - `POST /v1/chats/rest/clear`
  - `POST /v1/chat/logged-out`
  - `POST /v1/chats/logged-out`
  - `POST /v1/chat/logged-out/clear`
  - `POST /v1/chats/logged-out/clear`

Поведение:

- `rest_until` и `logged_out` исключают чат из обычного reuse и из promptless авто-выбора.
- `rest_until` снимается автоматически после истечения TTL.
- `logged_out` снимается вручную.
- Даже при прямом `chat_url` такой чат не будет использован, пока признак активен.
- Для promptless/direct режима chat session теперь корректно обновляет `page_url/chat_id` после первого пользовательского сообщения.

Проверка:

- `pytest`: 8 passed
- `py_compile`: ok


- Added `GET /v1/version` and exposed `version` via `/health` and `/v1/config/state`.
