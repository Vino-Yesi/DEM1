#!/bin/sh
set -eu

# The official Postgres entrypoint sources non-executable .sh init scripts.
# Create login roles before applying the schema so its role setup remains safe
# to re-run while the durable application and migration roles can authenticate.
psql --set=ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=app_password="$WMS_APP_PASSWORD" \
  --set=migration_password="$WMS_MIGRATION_PASSWORD" <<'SQL'
CREATE ROLE wms_app LOGIN PASSWORD :'app_password';
CREATE ROLE wms_migration LOGIN BYPASSRLS PASSWORD :'migration_password';
SQL

psql --set=ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --file=/docker/wms/001_schema.sql
