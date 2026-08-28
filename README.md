# Model Monitor v2.0

OpenAI-compatible model availability monitor with a dark dashboard, grouped
model cards, per-group default display models, bounded retries, latency
classification, and optional QQ group status replies.

## Features

- One dashboard card per model group.
- Configure the default model shown on each group card.
- Click a group card to inspect every model in that group.
- Show response latency, endpoint probe latency, availability, and recent checks.
- Per-group check intervals and timeout limits.
- Optional QQ scheduled status pushes and @ mention replies.

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

Runtime configuration and history are stored in `./data`. Do not commit that
directory or `.env`.

## Test

```bash
python3 -m unittest -v test_monitor.py
python3 -m py_compile monitor.py test_monitor.py
```
