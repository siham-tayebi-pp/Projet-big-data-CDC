#!/bin/bash
# Ce script s'exécute une seule fois après le démarrage de MinIO.
# Il crée les 3 buckets dont on a besoin :
#  - iceberg/      → stockage des tables Iceberg (Bronze, Silver, Gold)
#  - checkpoints/  → checkpoints Spark Structured Streaming
#  - warehouse/    → alias pour le warehouse Hive Metastore

set -e

echo "Attente de MinIO..."
until /usr/bin/mc alias set local http://minio:9000 \
      "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
  sleep 2
done
echo "MinIO prêt."

for bucket in iceberg checkpoints warehouse; do
  if ! /usr/bin/mc ls "local/$bucket" > /dev/null 2>&1; then
    /usr/bin/mc mb "local/$bucket"
    echo "Bucket créé : $bucket"
  else
    echo "Bucket existant : $bucket"
  fi
  /usr/bin/mc anonymous set public "local/$bucket" > /dev/null 2>&1 || true
done

echo "MinIO setup terminé."
