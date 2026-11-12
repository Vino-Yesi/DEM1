import { PGlite } from "@electric-sql/pglite";
import { btree_gist } from "@electric-sql/pglite/contrib/btree_gist";
import { pgcrypto } from "@electric-sql/pglite/contrib/pgcrypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const ddlPath = path.join(root, "sql", "001_schema.sql");
const collectorPath = path.join(here, "collect_sql.py");
const ddl = fs.readFileSync(ddlPath, "utf8");
const statements = JSON.parse(
  execFileSync("python3", [collectorPath], { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }),
);

const db = new PGlite({ extensions: { btree_gist, pgcrypto } });
let errors = 0;
try {
  await db.exec("CREATE EXTENSION IF NOT EXISTS pgcrypto; CREATE EXTENSION IF NOT EXISTS btree_gist;");
  await db.exec(ddl);
  console.log("DDL EXECUTION OK");

  const tableCount = await db.query(
    "SELECT count(*)::int AS n FROM pg_tables WHERE schemaname = 'wms'",
  );
  const policyCount = await db.query(
    "SELECT count(*)::int AS n FROM pg_policies WHERE schemaname = 'wms'",
  );
  console.log(`TABLES ${tableCount.rows[0].n} POLICIES ${policyCount.rows[0].n}`);

  let prepared = 0;
  for (let i = 0; i < statements.length; i += 1) {
    const item = statements[i];
    try {
      await db.exec(`PREPARE q_${i + 1} AS ${item.sql}`);
      prepared += 1;
    } catch (error) {
      errors += 1;
      console.error(`PREPARE FAILED ${item.source}: ${error.message}`);
      console.error(item.sql.split("\n").slice(0, 6).join("\n"));
    }
  }
  console.log(`PREPARED ${prepared}/${statements.length} SQL statements`);
} catch (error) {
  errors += 1;
  console.error("FATAL", error.message || error);
} finally {
  await db.close();
}
if (errors > 0) process.exitCode = 1;
