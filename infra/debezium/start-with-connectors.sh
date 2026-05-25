#!/bin/bash
# Ce script démarre Kafka Connect, attend qu'il soit prêt,
# puis enregistre automatiquement tous les connecteurs JSON
# trouvés dans le dossier /kafka/connectors/

set -euo pipefail

CONNECT_HOST="${DEBEZIUM_CONNECT_HOST:-localhost}"
CONNECT_PORT="${DEBEZIUM_CONNECT_PORT:-8083}"
CONFIG_DIR="${DEBEZIUM_CONNECTOR_CONFIG_DIR:-/kafka/connectors}"
MAX_ATTEMPTS="${DEBEZIUM_CONNECT_WAIT_ATTEMPTS:-60}"
SLEEP_SECONDS="${DEBEZIUM_CONNECT_WAIT_INTERVAL:-5}"

# Démarre Kafka Connect en arrière-plan
/docker-entrypoint.sh start &
CONNECT_PID=$!
trap 'kill -TERM ${CONNECT_PID} >/dev/null 2>&1 || true' TERM INT

# Attend que l'API REST de Connect soit disponible
available=false
for ((attempt=1; attempt<=MAX_ATTEMPTS; attempt++)); do
  if curl -sSf "http://${CONNECT_HOST}:${CONNECT_PORT}/connectors" >/dev/null 2>&1; then
    available=true
    break
  fi
  echo "Attente Kafka Connect (${attempt}/${MAX_ATTEMPTS})..."
  sleep "${SLEEP_SECONDS}"
done

if ! $available; then
  echo "Kafka Connect non disponible." >&2
else
  # Enregistre chaque fichier .json comme connecteur
  if compgen -G "${CONFIG_DIR}/*.json" >/dev/null 2>&1; then
    for config_path in "${CONFIG_DIR}"/*.json; do
      connector_name="$(basename "${config_path}" .json)"
      echo "Enregistrement du connecteur : ${connector_name}"
      http_code=$(curl -sS -o /tmp/resp -w "%{http_code}" \
        -X PUT "http://${CONNECT_HOST}:${CONNECT_PORT}/connectors/${connector_name}/config" \
        -H "Content-Type: application/json" \
        --data "@${config_path}" || true)
      echo "HTTP $http_code pour $connector_name"
      cat /tmp/resp
    done
  fi
fi

wait ${CONNECT_PID}
