PYTHON ?= python3
NPM ?= npm
RUFF ?= ruff

.PHONY: validate compile lint test unit static sql mermaid clean

validate: compile lint test sql mermaid

compile:
	$(PYTHON) -m compileall -q services/orchestrator

lint:
	$(RUFF) check --select F,E9 services/orchestrator

test: unit static

unit:
	cd services && $(PYTHON) -m unittest discover -s orchestrator/tests -v

static:
	$(PYTHON) -m unittest discover -s tests -v

sql:
	cd tests/sql_validation && $(NPM) install --no-audit --no-fund && node validate_sql.mjs

mermaid:
	cd tests/diagram_validation && $(NPM) install --no-audit --no-fund && node parse.mjs

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .ruff_cache
