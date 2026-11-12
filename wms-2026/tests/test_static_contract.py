from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATOR = ROOT / "services" / "orchestrator"
DDL = ROOT / "sql" / "001_schema.sql"
DIAGRAMS = ROOT / "diagrams"
PLACEHOLDER = re.compile(r"%\((\w+)\)s|(?<!%)%s(?!%)")
ALLOWED_FIRST_TOKENS = {"SELECT", "INSERT", "UPDATE", "DELETE", "WITH"}
SQL_CALL_NAMES = {"fetch_one", "fetch_all", "execute"}


def sql_constants() -> dict[str, str]:
    tree = ast.parse((ORCHESTRATOR / "sql.py").read_text(encoding="utf-8"))
    constants: dict[str, str] = {}
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            constants[node.targets[0].id] = node.value.value
    return constants


def imported_sql_aliases(tree: ast.AST, constants: dict[str, str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "sql"
            and node.level == 1
        ):
            for alias in node.names:
                if alias.name in constants:
                    aliases[alias.asname or alias.name] = alias.name
    return aliases


def call_sql_text(
    node: ast.Call,
    constants: dict[str, str],
    aliases: dict[str, str],
) -> str | None:
    if len(node.args) < 2:
        return None
    function = node.func
    name = function.id if isinstance(function, ast.Name) else function.attr
    if name not in SQL_CALL_NAMES:
        return None
    arg = node.args[1]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        stripped = arg.value.lstrip()
        first = stripped.split(None, 1)[0].upper().rstrip(";,:") if stripped else ""
        return arg.value if first in ALLOWED_FIRST_TOKENS else None
    if (
        isinstance(arg, ast.Attribute)
        and isinstance(arg.value, ast.Name)
        and arg.value.id == "sql"
        and arg.attr in constants
    ):
        return constants[arg.attr]
    if isinstance(arg, ast.Name) and aliases.get(arg.id) in constants:
        return constants[aliases[arg.id]]
    return None


def dict_shape(node: ast.Dict) -> tuple[set[str], int]:
    keys: set[str] = set()
    spreads = 0
    for key in node.keys:
        if key is None:
            spreads += 1
        elif isinstance(key, ast.Constant) and isinstance(key.value, str):
            keys.add(key.value)
    return keys, spreads


class StaticContractTests(unittest.TestCase):
    def test_python_files_parse(self) -> None:
        for path in sorted(ORCHESTRATOR.rglob("*.py")):
            with self.subTest(path=str(path.relative_to(ROOT))):
                ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def test_sql_placeholders_match_call_parameters(self) -> None:
        constants = sql_constants()
        self.assertGreater(len(constants), 60)
        problems: list[str] = []
        checked = 0
        for path in sorted(ORCHESTRATOR.rglob("*.py")):
            if path.name == "sql.py" or "tests" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            aliases = imported_sql_aliases(tree, constants)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                sqltext = call_sql_text(node, constants, aliases)
                if sqltext is None:
                    continue
                params = set(PLACEHOLDER.findall(sqltext.replace("%%", "%")))
                dict_node = next(
                    (arg for arg in node.args[2:] if isinstance(arg, ast.Dict)),
                    None,
                )
                if dict_node is None:
                    continue
                checked += 1
                keys, spreads = dict_shape(dict_node)
                missing = params - keys
                extra = keys - params
                if missing and spreads == 0:
                    problems.append(
                        f"{path.relative_to(ROOT)}:{node.lineno} missing {sorted(missing)}"
                    )
                if extra and spreads == 0:
                    problems.append(
                        f"{path.relative_to(ROOT)}:{node.lineno} extra {sorted(extra)}"
                    )
        self.assertGreater(checked, 70)
        self.assertEqual([], problems)

    def test_ddl_required_contract(self) -> None:
        ddl = DDL.read_text(encoding="utf-8")
        required = [
            "CREATE TABLE IF NOT EXISTS tenants",
            "CREATE TABLE IF NOT EXISTS skus",
            "CREATE TABLE IF NOT EXISTS locations",
            "CREATE TABLE IF NOT EXISTS inventory_balances",
            "warehouse_id",
            "zone",
            "aisle",
            "bay",
            "level",
            "position",
            "max_volume_cm3",
            "max_weight_kg",
            "current_volume_cm3",
            "current_weight_kg",
            "status",
            "CREATE TYPE stock_status AS ENUM ('available','allocated','damaged','qc_hold'",
            "UNIQUE NULLS NOT DISTINCT",
            "ENABLE ROW LEVEL SECURITY",
            "FORCE ROW LEVEL SECURITY",
            "CONCURRENCY CONTRACT",
            "FOR UPDATE OF h, b SKIP LOCKED",
            "FOR UPDATE SKIP LOCKED",
            "version = version + 1",
            "CREATE OR REPLACE FUNCTION apply_tenant_rls",
            "p_tenant_isolation",
        ]
        for token in required:
            with self.subTest(token=token):
                self.assertIn(token, ddl)
        self.assertGreaterEqual(ddl.count("CREATE INDEX"), 40)
        self.assertGreaterEqual(ddl.count("CREATE POLICY"), 4)

    def test_mermaid_and_docs_are_present(self) -> None:
        for name in ("system-architecture.mmd", "hybrid-order-fulfillment.mmd"):
            path = DIAGRAMS / name
            self.assertTrue(path.is_file())
            self.assertTrue(
                path.read_text(encoding="utf-8").lstrip().startswith("flowchart")
            )
        self.assertTrue((ROOT / "README.md").is_file())
        self.assertTrue((ROOT / "docs" / "00-master-architecture.md").is_file())
        self.assertTrue((ROOT / "docs" / "diagrams.md").is_file())
        self.assertTrue((ROOT / "docs" / "07-validation.md").is_file())

    def test_package_exports_exist(self) -> None:
        tree = ast.parse((ORCHESTRATOR / "__init__.py").read_text(encoding="utf-8"))
        exports: list[str] = []
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(target, ast.Name) and target.id == "__all__"
                    for target in node.targets
                )
                and isinstance(node.value, ast.List)
            ):
                exports = [
                    item.value
                    for item in node.value.elts
                    if isinstance(item, ast.Constant) and isinstance(item.value, str)
                ]
        self.assertGreaterEqual(len(exports), 20)
        for module in exports:
            with self.subTest(module=module):
                self.assertTrue((ORCHESTRATOR / f"{module}.py").is_file())

    def test_no_unfinished_code_markers(self) -> None:
        forbidden = re.compile(r"\bTODO\b|\bFIXME\b|\bXXX\b|pass\s+#")
        offenders: list[str] = []
        for path in sorted(ORCHESTRATOR.rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if forbidden.search(text):
                offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
