# Changelog

## v3.0.2

- Hid the admin shortcut from the public dashboard while keeping `/admin` available directly.
- Added configurable OpenAI, Grok, Gemini, and Claude icons for model groups.
- Made Responses API the preferred model probe with a Chat Completions compatibility fallback.
- Added rate multiplier extraction, short-lived billing fallback caching, SQLite history storage, and dashboard display.
- Displayed fluctuation as a distinct dashboard status while keeping latency visible.

## v3.0.1

- Bound each group history chart directly to its configured default display model.
- Prevented a missing default-model result from silently showing another model's history.
- Added the active model name to each history chart and automated GitHub releases with multi-architecture GHCR images.

## v3.0.0

- Replaced the Python runtime with a statically linked Go backend.
- Kept the existing HTTP API, config format, SQLite history, dashboard, and admin UI.
- Reimplemented OpenAI-compatible streaming checks, retries, Responses fallback, and fresh upstream connections in Go.
- Reimplemented QQ scheduled pushes, @ replies, duplicate-message protection, and group binding in Go.
- Switched Docker to a multi-stage Go build with an Alpine runtime image.

## v2.0.0

- Grouped the dashboard into one card per model group.
- Added configurable default display model per group.
- Added click-to-open group details with all monitored models.
- Added response latency, endpoint probe latency, availability, and recent check history.
- Added per-group check intervals and timeout handling.
- Added bounded retries and Responses API fallback for compatible gateways.
- Added optional QQ scheduled status pushes and @ mention replies.
- Removed runtime credentials from the Docker Compose file and documented `.env` configuration.
