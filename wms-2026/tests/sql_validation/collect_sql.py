#!/usr/bin/env python3
"""Collect every SQL string used by the orchestrator for PostgreSQL validation.

The script emits JSON on stdout. It rewrites psycopg named and positional
placeholders to PostgreSQL PREPARE parameters and unescapes psycopg's literal
``%%`` to ``%``. It deliberately collects only strings whose first token is a
DML/query keyword, so prose and exception messages are excluded.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATOR = ROOT / "services" / "orchestrator"
PLACEHOLDER = re.compile(r"%\((\w+)\)s|(?<!%)%s(?!%)")
ALLOWED_FIRST_TOKENS = {"SELECT", "INSERT", "UPDATE", "DELETE", "WITH"}


def convert(raw: str) -> tuple[str, list[str]]:
    sqltext = raw.replace("%%", "%")
    mapping: dict[str, int] = {}

    def replace(match: re.Match[str]) -> str:
        key = match.group(1) or f"pos{len(mapping) + 1}"
        if key not in mapping:
            mapping[key] = len(mapping) + 1
        return f"${mapping[key]}"

    return PLACEHOLDER.sub(replace, sqltext), list(mapping.keys())


def main() -> None:
    items: list[dict[str, object]] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()
    for path in sorted(ORCHESTRATOR.rglob("*.py")):
        if path.name == "__init__.py" or "tests" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            raw = node.value
            stripped = raw.lstrip()
            if not stripped:
                continue
            first = stripped.split(None, 1)[0].upper().rstrip(";,:")
            if first not in ALLOWED_FIRST_TOKENS:
                continue
            converted, params = convert(raw)
            key = (converted.strip(), tuple(params))
            if key in seen:
                continue
            seen.add(key)
            items.append(
                {
                    "source": f"{path.relative_to(ROOT)}:{node.lineno}",
                    "sql": converted,
                    "params": params,
                }
            )
    print(json.dumps(items, indent=2))


if __name__ == "__main__":
    main()
