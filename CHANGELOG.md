# Changelog

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
