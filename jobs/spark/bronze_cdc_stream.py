#!/usr/bin/env python3
"""
BRONZE CDC STREAM — Couche Bronze du Lakehouse
===============================================
Ce job tourne en CONTINU (streaming).
Il lit les topics Kafka Debezium et stocke les événements bruts
dans des tables Iceberg Bronze.

Flux :
  Kafka (cdc.public.*) → parse JSON Debezium → Iceberg Bronze

Un message Debezium ressemble à :
{
  "payload": {
    "before": null,           ← null pour INSERT
    "after": {"id":1, ...},   ← données après modification
    "op": "c",                ← c=INSERT, u=UPDATE, d=DELETE, r=READ
    "ts_ms": 1700000000,
    "source": {"db":"retail", "table":"customers", "lsn":123}
  }
}

Tables créées :
  iceberg.bronze.customers_cdc
  iceberg.bronze.orders_cdc
  iceberg.bronze.products_cdc
  iceberg.bronze.order_items_cdc
  iceberg.bronze.payments_cdc
  iceberg.bronze.returns_cdc
"""

import argparse
import logging
from pyspark.sql import SparkSession, functions as F, types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bronze")


# Schéma de l'enveloppe Debezium JSON
DEBEZIUM_SCHEMA = T.StructType([
    T.StructField("payload", T.StructType([
        T.StructField("before", T.StringType(), True),
        T.StructField("after",  T.StringType(), True),
        T.StructField("op",     T.StringType(), False),
        T.StructField("ts_ms",  T.LongType(),   True),
        T.StructField("source", T.StructType([
            T.StructField("db",    T.StringType(), True),
            T.StructField("table", T.StringType(), True),
            T.StructField("lsn",   T.LongType(),   True),
        ]), True),
    ]), True),
])


def get_spark(app: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table: str) -> None:
    """Crée la table Bronze Iceberg si elle n'existe pas encore."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            cdc_op          STRING,
            cdc_ts_ms       BIGINT,
            cdc_ts          TIMESTAMP,
            cdc_db          STRING,
            cdc_table       STRING,
            cdc_lsn         BIGINT,
            before_json     STRING,
            after_json      STRING,
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            kafka_ts        TIMESTAMP,
            processed_at    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(cdc_ts), cdc_op)
        LOCATION 's3a://iceberg/warehouse/bronze/{table.split(".")[-1]}'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topics",
        default="cdc.public.customers,cdc.public.products,cdc.public.orders,"
                "cdc.public.order_items,cdc.public.payments,cdc.public.returns")
    parser.add_argument("--checkpoint", default="s3a://checkpoints/streaming")
    parser.add_argument("--trigger",    default="30 seconds")
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    args = parser.parse_args()

    spark = get_spark("cdc-bronze")
    spark.sparkContext.setLogLevel("WARN")
    kafka_bootstrap = spark.conf.get("spark.cdc.kafka.bootstrap", "kafka:9092")

    log.info("Bronze Stream démarré | topics=%s", args.topics)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    # Lecture du stream Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", args.topics)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    # Parse l'enveloppe Debezium JSON
    parsed = (
        raw
        .withColumn("env", F.from_json(F.col("value").cast("string"), DEBEZIUM_SCHEMA))
        .withColumn("p", F.col("env.payload"))
        .filter(F.col("p").isNotNull() & F.col("p.op").isNotNull())
        .select(
            F.when(F.col("p.op")=="c","INSERT")
             .when(F.col("p.op")=="u","UPDATE")
             .when(F.col("p.op")=="d","DELETE")
             .when(F.col("p.op")=="r","READ")
             .alias("cdc_op"),
            F.col("p.ts_ms").alias("cdc_ts_ms"),
            (F.col("p.ts_ms")/1000).cast("timestamp").alias("cdc_ts"),
            F.col("p.source.db").alias("cdc_db"),
            F.col("p.source.table").alias("cdc_table"),
            F.col("p.source.lsn").alias("cdc_lsn"),
            F.to_json(F.col("p.before")).alias("before_json"),
            F.to_json(F.col("p.after")).alias("after_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    def write_batch(batch_df, epoch_id):
        if batch_df.isEmpty():
            return
        for row in batch_df.select("cdc_table").distinct().collect():
            t = row["cdc_table"]
            iceberg_t = f"iceberg.bronze.{t}_cdc"
            df = batch_df.filter(F.col("cdc_table") == t)
            try:
                ensure_table(spark, iceberg_t)
            except Exception:
                pass
            df.writeTo(iceberg_t).append()
            log.info("Bronze | epoch=%d | table=%s | rows=%d", epoch_id, iceberg_t, df.count())

    (
        parsed.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{args.checkpoint}/bronze")
        .trigger(processingTime=args.trigger)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
