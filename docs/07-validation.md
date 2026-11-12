# Validation Evidence

## 1. Validation environment

| Component | Version / role |
|---|---|
| Python | 3.13.16; orchestrator dependencies installed in a validation virtual environment |
| Node.js | 20.20.2; Mermaid 12.3.0 parse succeeded with npm EBADENGINE warnings because Mermaid declares Node >=22.12.0 |
| PostgreSQL engine | PGlite 0.5.16 / PostgreSQL 18.6 (wasm) for DDL execution and SQL preparation |
| SQL parser | `pglast` for PostgreSQL DDL parse |
| Lint | Ruff F/E9 rules |
| Mermaid parser | `mermaid.parse` with JSDOM globals |

PostgreSQL 16+ is the deployment target. No native PG16 server exists in the workspace sandbox; deployment pipelines must execute `sql/001_schema.sql` against the target PG16+ cluster as a release gate.

## 2. Commands

Install runtime and development dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r services/orchestrator/requirements.txt
pip install -r requirements-dev.txt
```

Run the complete workspace validation target:

```bash
make validate
```

`make validate` executes:

```text
python -m compileall -q services/orchestrator
ruff check --select F,E9 services/orchestrator
cd services && python -m unittest discover -s orchestrator/tests -v
python -m unittest discover -s tests -v
cd tests/sql_validation && npm install --no-audit --no-fund && node validate_sql.mjs
cd tests/diagram_validation && npm install --no-audit --no-fund && node parse.mjs
```

Additional parse/import checks:

```bash
python - <<'PY'
from pathlib import Path
from pglast import parse_sql
print("PGLAST PARSED STATEMENTS", len(parse_sql(Path("sql/001_schema.sql").read_text())))
PY

cd services
PYTHONPATH=. python - <<'PY'
import importlib
mods = [
    "orchestrator.config", "orchestrator.db", "orchestrator.errors",
    "orchestrator.schemas", "orchestrator.utils", "orchestrator.auth",
    "orchestrator.idempotency", "orchestrator.outbox", "orchestrator.sql",
    "orchestrator.affinity", "orchestrator.pricing", "orchestrator.catalog",
    "orchestrator.order_repo", "orchestrator.wave_repo", "orchestrator.allocation",
    "orchestrator.task_dispatcher", "orchestrator.b2b", "orchestrator.billing",
    "orchestrator.exceptions", "orchestrator.routing", "orchestrator.main",
    "orchestrator.run_relay",
]
for name in mods:
    importlib.import_module(name)
from orchestrator.main import app
print("IMPORT OK", len(mods), "modules")
print("FASTAPI ROUTES", len(app.routes))
PY
```

## 3. Observed results

| Check | Result |
|---|---|
| Python compile | `COMPILE_OK` |
| Ruff F/E9 | `All checks passed!` |
| Pure unit tests | `Ran 12 tests ... OK` |
| Static contract tests | `Ran 6 tests ... OK` |
| `pglast` DDL parse | `PGLAST PARSED STATEMENTS 135` |
| PGlite DDL execution | `DDL EXECUTION OK` |
| Schema object count | `TABLES 75 POLICIES 75` |
| SQL contract | `PREPARED 106/106 SQL statements` |
| Module imports | `IMPORT OK 22 modules` |
| FastAPI routes | `FASTAPI ROUTES 15` |
| Mermaid parse | `PARSED 6/6 Mermaid sources` |

The 106 prepared SQL statements include all `sql.py` constants and every inline DML/query string collected from the orchestrator. The 6 Mermaid sources are:

1. `diagrams/system-architecture.mmd`
2. `diagrams/hybrid-order-fulfillment.mmd`
3. system diagram fenced block in `docs/00-master-architecture.md`
4. workflow diagram fenced block in `docs/00-master-architecture.md`
5. system diagram fenced block in `docs/diagrams.md`
6. workflow diagram fenced block in `docs/diagrams.md`

Each reported `flowchart-v2`.

## 4. Static contract coverage

`tests/test_static_contract.py` asserts:

- every Python source under `services/orchestrator` parses;
- every SQL call with a literal parameter dictionary supplies exactly the placeholders required by its SQL constant or inline SQL;
- more than 60 SQL constants and more than 70 checked call sites exist;
- DDL contains required tenant/SKU/location/inventory fields, `stock_status`, `UNIQUE NULLS NOT DISTINCT`, forced RLS, concurrency-contract comment, `FOR UPDATE ... SKIP LOCKED`, version increment, and RLS policy function;
- at least 40 `CREATE INDEX` statements and at least 4 literal `CREATE POLICY` statements exist; the remaining policies are created by `apply_tenant_rls`;
- both `.mmd` blueprints, `README.md`, and `docs/00-master-architecture.md` exist;
- every `__all__` module has a corresponding `.py` file;
- no `TODO`, `FIXME`, `XXX`, or commented `pass` markers remain in service code.

## 5. Unit test coverage

`services/orchestrator/tests/test_pure_logic.py` covers:

- 3D travel cost with zone and vertical penalties;
- Jaccard similarity including empty-set behaviour;
- lot-aware stop planning that rejects insufficient single lots and selects a sufficient lot;
- deterministic wave choice and cutoff bucket handling;
- currency exponent and HALF_UP rounding;
- tiered-rate selection;
- pallet-position storage;
- cubic-foot storage tier discount;
- B2C piece-pick tier discount;
- B2B pallet pick with cross-dock fee;
- VAS charge with setup fee, tier discount, and complexity multiplier;
- simple VAS charge.

## 6. Remaining deployment gate

Native PostgreSQL 16 execution is not part of the sandbox evidence. Before production release, run:

```bash
psql "$WMS_SYSTEM_DATABASE_URL" -f sql/001_schema.sql
```

then repeat the SQL-contract preparation against that cluster and verify extension availability, role grants, partition maintenance jobs, connection-pool limits, RLS behaviour under `wms_app`, and outbox relay throughput. The service, schema, and validation harnesses are otherwise complete and internally consistent.
