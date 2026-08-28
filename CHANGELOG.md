# Changelog

## v2.0.0

- Grouped the dashboard into one card per model group.
- Added configurable default display model per group.
- Added click-to-open group details with all monitored models.
- Added response latency, endpoint probe latency, availability, and recent check history.
- Added per-group check intervals and timeout handling.
- Added bounded retries and Responses API fallback for compatible gateways.
- Added optional QQ scheduled status pushes and @ mention replies.
- Removed runtime credentials from the Docker Compose file and documented `.env` configuration.
