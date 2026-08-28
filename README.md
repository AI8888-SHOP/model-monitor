# Model Monitor v3.0

OpenAI-compatible model availability monitor with a lightweight Go backend.
It keeps the grouped dashboard, per-group default display models, response
latency, availability history, bounded retries, Responses API fallback, and
optional QQ group status pushes and @ mention replies.

## Docker

1. Copy the example environment file and set a private admin password:

   ```bash
   cp .env.example .env
   ```

2. Set `ADMIN_PASSWORD`, and set `API_KEY` when the default API requires it.

3. Build and start the service:

   ```bash
   docker compose up -d --build
   ```

4. Open the dashboard at `http://127.0.0.1:8020` and the admin page at
   `http://127.0.0.1:8020/admin`.

Runtime configuration and SQLite history are stored in `./data`. Do not commit
that directory or `.env`.

## GHCR

```bash
docker pull ghcr.io/ai8888-shop/model-monitor:v3.0
```

The image also has `3.0.0` and `latest` tags. The GHCR package must be set to
public in its package settings before anonymous pulls are allowed.

## Development

```bash
go test ./...
go vet ./...
```
