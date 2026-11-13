# Local Docker Compose startup

This configuration runs the FastAPI orchestrator, PostgreSQL 16, Redis 7, a single-node Kafka 3.9 broker, and the outbox relay. The single Kafka broker is for local development and evaluation only; it is not highly available and does not provide production durability guarantees.

## Start locally

From the repository root:

```sh
cp .env.example .env
# Edit .env if needed; the example passwords are for local use only.
docker compose up --build
```

The API is available at `http://localhost:8080`; liveness is `GET /health/live`, readiness is `GET /health/ready`, and OpenAPI docs are at `/docs`. The Compose healthcheck waits for PostgreSQL and Redis. The relay waits for PostgreSQL and Kafka. The schema is installed automatically only when PostgreSQL initializes a new empty data volume.

Stop the services with `docker compose down`. Persistent local database, Redis, and Kafka data remains in Docker volumes. To intentionally reset all local state, use `docker compose down -v` (this permanently deletes those volumes).

## Configuration and security

- The `.env.example` values are explicitly local-development examples. Do not expose this Compose stack publicly with those values.
- Replace all passwords and use strong, unique secrets before any shared environment. Keep PostgreSQL URL passwords URL-encoded; the sample uses URL-safe passwords.
- Store production secrets in your hosting platform's secret manager, not in Git or a committed `.env` file.
- The API requires both `WMS_DATABASE_URL` and `WMS_SYSTEM_DATABASE_URL`; the latter is used for the outbox relay's system pool. Compose supplies the same DSNs to the API and relay.
- Production requires separately managed PostgreSQL, Redis, and Kafka services, TLS/authentication, backup/restore procedures, resource limits, and a multi-broker Kafka cluster. Do not treat this local stack as production-ready.
- The PostgreSQL initialization script applies `sql/001_schema.sql` to a new volume. For schema upgrades to an existing volume, use a reviewed migration process rather than deleting the volume.

Useful commands:

```sh
docker compose ps
docker compose logs -f api relay
docker compose exec api python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8080/health/ready').read().decode())"
```
