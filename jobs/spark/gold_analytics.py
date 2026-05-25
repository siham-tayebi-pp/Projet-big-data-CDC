#!/usr/bin/env python3
"""
GOLD ANALYTICS — Couche Gold du Lakehouse
==========================================
Construit les tables analytiques finales à partir de Silver.
Ces tables sont optimisées pour Trino et Superset.

Tables produites :
  iceberg.gold.customers        → clients actifs
  iceberg.gold.orders           → commandes enrichies
  iceberg.gold.sales_facts      → faits de ventes (KPI principal)
  iceberg.gold.payments_summary → résumé paiements
  iceberg.gold.returns_summary  → résumé retours
  iceberg.gold.cdc_audit        → métriques CDC (nb events, latence)
"""

import argparse
import logging
from functools import reduce
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gold")


def get_spark():
    return (
        SparkSession.builder.appName("cdc-gold")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def write(spark, df, name, partition=None):
    """Écrase la table Gold (snapshot complet à chaque run)."""
    t = f"iceberg.gold.{name}"
    w = df.writeTo(t).using("iceberg") \
          .tableProperty("write.format.default", "parquet") \
          .tableProperty("write.parquet.compression-codec", "snappy")
    if partition:
        w = w.partitionedBy(F.col(partition))
    w.createOrReplace()
    log.info("Gold écrit : %s (%d lignes)", t, df.count())


def build_customers(spark):
    df = spark.sql("""
        SELECT customer_id, name, email, city, country, segment,
               registration_date, is_active, _cdc_ts, _silver_updated_at
        FROM iceberg.silver.customers
        WHERE _is_deleted = false
    """)
    write(spark, df, "customers")


def build_orders(spark):
    df = spark.sql("""
        SELECT
            o.order_id, o.customer_id,
            c.name        AS customer_name,
            c.segment     AS customer_segment,
            c.city        AS customer_city,
            o.order_date,
            DATE(o.order_date)   AS order_day,
            MONTH(o.order_date)  AS order_month,
            YEAR(o.order_date)   AS order_year,
            o.status, o.channel, o.total_amount,
            CASE WHEN o.status='cancelled' THEN 1 ELSE 0 END AS is_cancelled,
            o._cdc_ts, o._silver_updated_at
        FROM iceberg.silver.orders o
        LEFT JOIN iceberg.silver.customers c
            ON o.customer_id = c.customer_id AND c._is_deleted=false
        WHERE o._is_deleted = false
    """)
    write(spark, df, "orders", "order_year")


def build_sales_facts(spark):
    """Table de faits principale — une ligne = une ligne de commande."""
    df = spark.sql("""
        SELECT
            oi.item_id, oi.order_id, oi.product_id, o.customer_id,
            p.name       AS product_name,
            p.category   AS product_category,
            p.brand      AS product_brand,
            c.name       AS customer_name,
            c.segment    AS customer_segment,
            c.city       AS customer_city,
            o.order_date,
            DATE(o.order_date)  AS order_day,
            MONTH(o.order_date) AS order_month,
            YEAR(o.order_date)  AS order_year,
            o.channel, o.status,
            oi.quantity, oi.unit_price,
            oi.quantity * oi.unit_price AS line_revenue,
            CASE WHEN o.status='cancelled' THEN 1 ELSE 0 END AS is_cancelled
        FROM iceberg.silver.order_items oi
        JOIN iceberg.silver.orders o
            ON oi.order_id=o.order_id AND o._is_deleted=false
        LEFT JOIN iceberg.silver.products p
            ON oi.product_id=p.product_id AND p._is_deleted=false
        LEFT JOIN iceberg.silver.customers c
            ON o.customer_id=c.customer_id AND c._is_deleted=false
        WHERE oi._is_deleted=false
    """)
    write(spark, df, "sales_facts", "order_year")


def build_payments_summary(spark):
    df = spark.sql("""
        SELECT
            DATE(py.payment_date)  AS payment_day,
            MONTH(py.payment_date) AS payment_month,
            YEAR(py.payment_date)  AS payment_year,
            py.method, py.status,
            c.segment AS customer_segment,
            COUNT(DISTINCT py.payment_id) AS nb_payments,
            SUM(py.amount)   AS total_amount,
            AVG(py.amount)   AS avg_amount
        FROM iceberg.silver.payments py
        LEFT JOIN iceberg.silver.orders o ON py.order_id=o.order_id
        LEFT JOIN iceberg.silver.customers c ON o.customer_id=c.customer_id
        WHERE py._is_deleted=false
        GROUP BY 1,2,3,4,5,6
    """)
    write(spark, df, "payments_summary", "payment_year")


def build_returns_summary(spark):
    df = spark.sql("""
        SELECT
            DATE(r.return_date)  AS return_day,
            MONTH(r.return_date) AS return_month,
            YEAR(r.return_date)  AS return_year,
            r.reason, r.status,
            c.segment AS customer_segment,
            COUNT(DISTINCT r.return_id) AS nb_returns,
            SUM(r.refund_amount) AS total_refund
        FROM iceberg.silver.returns r
        LEFT JOIN iceberg.silver.orders o ON r.order_id=o.order_id
        LEFT JOIN iceberg.silver.customers c ON o.customer_id=c.customer_id
        WHERE r._is_deleted=false
        GROUP BY 1,2,3,4,5,6
    """)
    write(spark, df, "returns_summary")


def build_cdc_audit(spark):
    """KPI : nb événements CDC par table, latence source→Kafka."""
    bronze_tables = ["customers","products","orders","order_items","payments","returns"]
    dfs = []
    for t in bronze_tables:
        try:
            df = spark.sql(f"""
                SELECT '{t}' AS source_table, cdc_op AS operation,
                       DATE(cdc_ts) AS event_day,
                       COUNT(*) AS event_count,
                       AVG(UNIX_TIMESTAMP(kafka_ts) - (cdc_ts_ms/1000)) AS avg_latency_seconds,
                       MAX(cdc_ts) AS last_event_ts
                FROM iceberg.bronze.{t}_cdc
                GROUP BY 1,2,3
            """)
            dfs.append(df)
        except Exception as e:
            log.warning("bronze.%s_cdc non dispo : %s", t, e)

    if dfs:
        write(spark, reduce(lambda a, b: a.union(b), dfs), "cdc_audit")


BUILDERS = {
    "customers":        build_customers,
    "orders":           build_orders,
    "sales_facts":      build_sales_facts,
    "payments_summary": build_payments_summary,
    "returns_summary":  build_returns_summary,
    "cdc_audit":        build_cdc_audit,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="all")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    tables = list(BUILDERS.keys()) if args.table == "all" else [args.table]
    log.info("Gold : tables=%s", tables)

    for t in tables:
        try:
            BUILDERS[t](spark)
        except Exception as e:
            log.error("Erreur Gold %s : %s", t, e, exc_info=True)

    spark.stop()


if __name__ == "__main__":
    main()
