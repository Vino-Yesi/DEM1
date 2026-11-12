# Hybrid / 3PL WMS 2026

Enterprise reference implementation for concurrent B2C piece picking, B2B pallet operations, and multi-tenant 3PL isolation. The package contains the PostgreSQL DDL, FastAPI orchestrator, deterministic exception engine, billing/VAS engine, Mermaid blueprints, master specification, and validation harnesses.

## Deliverables

| Area | Primary files |
|---|---|
| Database DDL | `sql/001_schema.sql` |
| Backend service | `services/orchestrator/*.py` |
| SQL statement catalogue | `services/orchestrator/sql.py` |
| Architecture specification | `docs/00-master-architecture.md` |
| Deep-dive specifications | `docs/01-data-and-concurrency.md` through `docs/08-adr.md` |
| Mermaid diagrams | `diagrams/system-architecture.mmd`, `diagrams/hybrid-order-fulfillment.mmd`, `docs/diagrams.md` |
| Tests and validation | `services/orchestrator/tests/`, `tests/`, `tests/sql_validation/`, `tests/diagram_validation/` |

## Runtime prerequisites

- PostgreSQL 16+ with `pgcrypto` and `btree_gist`
- Python 3.13+
- Redis 7+ for ephemeral worker task pushes
- Kafka 3+ for committed domain events
- Node.js 22.12+ recommended for Mermaid 12.3.0 parse validation; Node 20.20.2 also parses successfully with npm EBADENGINE warnings

## Service startup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r services/orchestrator/requirements.txt
pip install -r requirements-dev.txt

export WMS_DATABASE_URL='postgresql://wms_app@127.0.0.1:5432/wms?options=-csearch_path%3Dwms,public'
export WMS_SYSTEM_DATABASE_URL='postgresql://wms_migration@127.0.0.1:5432/wms?options=-csearch_path%3Dwms,public'
export WMS_REDIS_URL='redis://127.0.0.1:6379/0'
export WMS_KAFKA_BOOTSTRAP_SERVERS='kafka-1:9092,kafka-2:9092,kafka-3:9092'

psql "$WMS_SYSTEM_DATABASE_URL" -f sql/001_schema.sql

cd services
uvicorn orchestrator.main:app --host 0.0.0.0 --port 8080
# separate process:
python -m orchestrator.run_relay
```

`WMS_START_OUTBOX_RELAY=true` also runs the relay inside the API process; the separate process is the production topology. See `.env.example` for the full configuration surface.

## Validation

```bash
make validate
```

The target compiles Python, runs Ruff F/E9 checks, runs unit and static-contract tests, executes the DDL under PGlite, prepares every SQL string used by the orchestrator against that schema, and parses every Mermaid source with Mermaid 12.3.0. Individual targets are `make compile`, `make lint`, `make test`, `make sql`, and `make mermaid`.

PostgreSQL 16 is the deployment target. Sandbox validation uses PGlite's PostgreSQL 18.6 engine because no native PG16 server is installed; run the DDL against the target PG16 cluster in the deployment pipeline.

## Start reading

1. `docs/00-master-architecture.md`
2. `docs/diagrams.md`
3. `docs/01-data-and-concurrency.md`
4. `docs/02-routing-and-labour.md`
5. `docs/03-digital-twin-exceptions.md`
6. `docs/04-billing-and-vas.md`
7. `docs/05-api-and-events.md`
8. `docs/07-validation.md`
