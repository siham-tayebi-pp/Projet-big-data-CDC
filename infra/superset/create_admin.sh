#!/bin/bash
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname Admin \
  --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@local.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" 2>/dev/null || true

superset db upgrade
superset init

if [ -f /app/docker/docker-init.d/databases.yaml ]; then
  superset import-datasources -p /app/docker/docker-init.d/databases.yaml || true
fi
