#!/bin/bash
set -x

DB_DRIVER="${DB_DRIVER:-postgres}"
SKIP_SCHEMA_INIT="${IS_RESUME:-false}"

initialize_hive() {
  # Initialise le schéma de la base de métadonnées HMS dans PostgreSQL
  "$HIVE_HOME/bin/schematool" -dbType "$DB_DRIVER" -initOrUpgradeSchema
  if [ $? -eq 0 ]; then
    echo "Schéma HMS initialisé."
  else
    echo "Échec initialisation HMS." && exit 1
  fi
}

export HIVE_CONF_DIR=$HIVE_HOME/conf
export HADOOP_CLASSPATH="/opt/hive/lib/hadoop-aws-3.3.4.jar:\
/opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar:\
/opt/hive/lib/iceberg-hive-runtime-1.9.2.jar:$HADOOP_CLASSPATH"

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G \
  -Dfs.s3a.endpoint=http://minio:9000 \
  -Dfs.s3a.access.key=minio \
  -Dfs.s3a.secret.key=minio123 \
  -Dfs.s3a.path.style.access=true \
  -Dfs.s3a.connection.ssl.enabled=false \
  -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

if [[ "${SKIP_SCHEMA_INIT}" == "false" ]]; then
  initialize_hive
fi

export METASTORE_PORT=${METASTORE_PORT:-9083}
exec "$HIVE_HOME/bin/start-metastore"
