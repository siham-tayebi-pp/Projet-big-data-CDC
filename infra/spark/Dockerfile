FROM bitnami/spark:3.5.1

USER 0

# Dépendances Python utiles pour les jobs
RUN pip install --no-cache-dir pyarrow pandas psycopg2-binary

# Télécharge les JARs au moment du BUILD (pas au runtime)
# Cela rend le démarrage du container beaucoup plus rapide.
RUN set -eux; \
    JARS="${SPARK_HOME}/jars"; \
    # ── Connecteur Kafka pour Spark Streaming ──────────────
    curl -fSL -o "${JARS}/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar"; \
    curl -fSL -o "${JARS}/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar"; \
    curl -fSL -o "${JARS}/kafka-clients-3.5.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar"; \
    curl -fSL -o "${JARS}/commons-pool2-2.12.0.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"; \
    # ── Iceberg pour Spark 3.5 ─────────────────────────────
    curl -fSL -o "${JARS}/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"; \
    # ── S3A pour accéder à MinIO ───────────────────────────
    curl -fSL -o "${JARS}/hadoop-aws-3.3.4.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"; \
    curl -fSL -o "${JARS}/aws-java-sdk-bundle-1.12.367.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar"; \
    # ── Runtime Hive pour le catalog ──────────────────────
    curl -fSL -o "${JARS}/iceberg-hive-runtime-1.5.2.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.5.2/iceberg-hive-runtime-1.5.2.jar"

COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
RUN chown -R 1001:1001 ${SPARK_HOME}/conf ${SPARK_HOME}/jars

ENV PYTHONPATH="/opt/spark/jobs:${PYTHONPATH:-}"
USER 1001
