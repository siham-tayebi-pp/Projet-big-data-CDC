#!/usr/bin/env python3
"""
SILVER CDC PROCESSOR — Couche Silver
=====================================
Lit les tables Bronze et reconstruit l'état actuel de chaque entité.

Logique CDC :
  INSERT (op='c') → MERGE : INSERT si absent
  UPDATE (op='u') → MERGE : UPDATE si présent
  DELETE (op='d') → soft-delete : _is_deleted=true (on ne supprime pas)
  READ   (op='r') → traité comme INSERT (snapshot initial Debezium)

La clé est le MERGE INTO d'Iceberg :
  MERGE INTO silver.customers AS t
  USING nouvelles_données     AS s
    ON t.customer_id = s.customer_id
  WHEN MATCHED     → UPDATE SET ...
  WHEN NOT MATCHED → INSERT ...
"""

import argparse
import json
import logging
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver")

# Schémas métier de chaque table Silver
SCHEMAS = {
    "customers": T.StructType([
        T.StructField("customer_id",       T.IntegerType(),   False),
        T.StructField("name",              T.StringType(),    True),
        T.StructField("email",             T.StringType(),    True),
        T.StructField("city",              T.StringType(),    True),
        T.StructField("country",           T.StringType(),    True),
        T.StructField("segment",           T.StringType(),    True),
        T.StructField("registration_date", T.DateType(),      True),
        T.StructField("is_active",         T.BooleanType(),   True),
        T.StructField("created_at",        T.TimestampType(), True),
        T.StructField("updated_at",        T.TimestampType(), True),
    ]),
    "products": T.StructType([
        T.StructField("product_id", T.IntegerType(),  False),
        T.StructField("name",       T.StringType(),   True),
        T.StructField("category",   T.StringType(),   True),
        T.StructField("brand",      T.StringType(),   True),
        T.StructField("price",      T.DoubleType(),   True),
        T.StructField("stock_qty",  T.IntegerType(),  True),
        T.StructField("is_active",  T.BooleanType(),  True),
        T.StructField("created_at", T.TimestampType(),True),
        T.StructField("updated_at", T.TimestampType(),True),
    ]),
    "orders": T.StructType([
        T.StructField("order_id",     T.IntegerType(),  False),
        T.StructField("customer_id",  T.IntegerType(),  True),
        T.StructField("order_date",   T.TimestampType(),True),
        T.StructField("status",       T.StringType(),   True),
        T.StructField("channel",      T.StringType(),   True),
        T.StructField("total_amount", T.DoubleType(),   True),
        T.StructField("created_at",   T.TimestampType(),True),
        T.StructField("updated_at",   T.TimestampType(),True),
    ]),
    "order_items": T.StructType([
        T.StructField("item_id",    T.IntegerType(), False),
        T.StructField("order_id",   T.IntegerType(), True),
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("quantity",   T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(),  True),
        T.StructField("subtotal",   T.DoubleType(),  True),
        T.StructField("created_at", T.TimestampType(),True),
    ]),
    "payments": T.StructType([
        T.StructField("payment_id",   T.IntegerType(),  False),
        T.StructField("order_id",     T.IntegerType(),  True),
        T.StructField("payment_date", T.TimestampType(),True),
        T.StructField("method",       T.StringType(),   True),
        T.StructField("amount",       T.DoubleType(),   True),
        T.StructField("status",       T.StringType(),   True),
        T.StructField("created_at",   T.TimestampType(),True),
        T.StructField("updated_at",   T.TimestampType(),True),
    ]),
    "returns": T.StructType([
        T.StructField("return_id",     T.IntegerType(),  False),
        T.StructField("order_id",      T.IntegerType(),  True),
        T.StructField("return_date",   T.TimestampType(),True),
        T.StructField("reason",        T.StringType(),   True),
        T.StructField("status",        T.StringType(),   True),
        T.StructField("refund_amount", T.DoubleType(),   True),
        T.StructField("created_at",    T.TimestampType(),True),
        T.StructField("updated_at",    T.TimestampType(),True),
    ]),
}

PK = {
    "customers":  "customer_id",
    "products":   "product_id",
    "orders":     "order_id",
    "order_items":"item_id",
    "payments":   "payment_id",
    "returns":    "return_id",
}


def get_spark():
    return (
        SparkSession.builder.appName("cdc-silver")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def ensure_silver(spark, name):
    """Crée la table Silver avec les colonnes CDC supplémentaires."""
    schema = SCHEMAS[name]
    biz_cols = ", ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in schema.fields)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS iceberg.silver.{name} (
            {biz_cols},
            _cdc_op            STRING,
            _cdc_ts            TIMESTAMP,
            _is_deleted        BOOLEAN,
            _deleted_at        TIMESTAMP,
            _silver_updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(_cdc_ts))
        LOCATION 's3a://iceberg/warehouse/silver/{name}'
        TBLPROPERTIES ('write.upsert.enabled'='true')
    """)


def process(spark, name):
    bronze = f"iceberg.bronze.{name}_cdc"
    silver = f"iceberg.silver.{name}"
    pk     = PK[name]
    schema = SCHEMAS[name]
    biz    = [f.name for f in schema.fields]

    try:
        spark.table(bronze).limit(1).collect()
    except Exception as e:
        log.warning("Bronze %s non disponible : %s", bronze, e)
        return

    ensure_silver(spark, name)

    biz_schema = T.StructType([f for f in schema.fields])

    df = (
        spark.table(bronze)
        .withColumn("row_data",
            F.when(F.col("cdc_op").isin("INSERT","UPDATE","READ"),
                   F.from_json(F.col("after_json"), biz_schema))
            .otherwise(F.from_json(F.col("before_json"), biz_schema))
        )
        .filter(F.col("row_data").isNotNull())
        .select(
            *[F.col(f"row_data.{c}").alias(c) for c in biz],
            F.col("cdc_op").alias("_cdc_op"),
            F.col("cdc_ts").alias("_cdc_ts"),
            F.when(F.col("cdc_op")=="DELETE", F.lit(True)).otherwise(F.lit(False)).alias("_is_deleted"),
            F.when(F.col("cdc_op")=="DELETE", F.current_timestamp()).alias("_deleted_at"),
            F.current_timestamp().alias("_silver_updated_at"),
        )
    )

    # Dédoublonnage : garde la version la plus récente par PK
    w = Window.partitionBy(pk).orderBy(F.col("_cdc_ts").desc())
    latest = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")

    n = latest.count()
    if n == 0:
        log.info("Silver %s : aucune donnée à traiter", name)
        return

    latest.createOrReplaceTempView("_updates")

    # Construction dynamique du MERGE
    set_cols  = ", ".join(f"t.{c}=s.{c}" for c in biz if c != pk)
    set_cols += ", t._cdc_op=s._cdc_op, t._cdc_ts=s._cdc_ts, t._is_deleted=s._is_deleted, t._deleted_at=s._deleted_at, t._silver_updated_at=s._silver_updated_at"
    all_cols  = ", ".join(biz + ["_cdc_op","_cdc_ts","_is_deleted","_deleted_at","_silver_updated_at"])
    all_vals  = ", ".join(f"s.{c}" for c in biz + ["_cdc_op","_cdc_ts","_is_deleted","_deleted_at","_silver_updated_at"])

    spark.sql(f"""
        MERGE INTO {silver} AS t
        USING _updates AS s ON t.{pk} = s.{pk}
        WHEN MATCHED     THEN UPDATE SET {set_cols}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({all_vals})
    """)
    log.info("Silver MERGE | %s | %d lignes", name, n)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="all")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables = list(SCHEMAS.keys()) if args.table == "all" else [args.table]
    log.info("Silver : tables=%s", tables)

    for t in tables:
        try:
            process(spark, t)
        except Exception as e:
            log.error("Erreur Silver %s : %s", t, e, exc_info=True)

    spark.stop()


if __name__ == "__main__":
    main()
