# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce_lakehouse.governance.pipeline_audit_log (
# MAGIC     audit_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     pipeline_layer STRING,
# MAGIC     source_table STRING,
# MAGIC     target_table STRING,
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     status STRING,
# MAGIC     records_read BIGINT,
# MAGIC     records_written BIGINT,
# MAGIC     records_failed BIGINT,
# MAGIC     error_message STRING,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from datetime import datetime
import uuid
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)

# =======================================================
# 1. Recreate/ensure pipeline audit log table exists
# =======================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS ecommerce_lakehouse.governance.pipeline_audit_log (
    audit_id STRING,
    pipeline_name STRING,
    pipeline_layer STRING,
    source_table STRING,
    target_table STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    records_read BIGINT,
    records_written BIGINT,
    records_failed BIGINT,
    error_message STRING,
    created_at TIMESTAMP
)
USING DELTA
""")


# =======================================================
# 2. Audit log function with explicit schema
# =======================================================

def write_pipeline_audit_log(
    pipeline_name,
    pipeline_layer,
    source_table,
    target_table,
    start_time,
    end_time,
    status,
    records_read,
    records_written,
    records_failed,
    error_message=None
):
    """
    Insert one audit record into governance.pipeline_audit_log.
    Explicit schema is used to avoid Spark type inference errors.
    """

    audit_id = str(uuid.uuid4())

    audit_schema = StructType([
        StructField("audit_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("pipeline_layer", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("records_failed", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    audit_data = [
        (
            audit_id,
            pipeline_name,
            pipeline_layer,
            source_table,
            target_table,
            start_time,
            end_time,
            status,
            int(records_read) if records_read is not None else None,
            int(records_written) if records_written is not None else None,
            int(records_failed) if records_failed is not None else None,
            error_message,
            datetime.now()
        )
    ]

    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)

    audit_df.write.mode("append").format("delta").saveAsTable(
        "ecommerce_lakehouse.governance.pipeline_audit_log"
    )

    print(f"Audit inserted: {pipeline_name} | {pipeline_layer} | {status}")


# =======================================================
# 3. Pipeline audit configuration
# =======================================================

pipeline_audit_config = [
    # -------------------------------
    # Silver flat tables
    # -------------------------------
    {
        "pipeline_name": "silver_product_catalog_load",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.product_catalog",
        "target_table": "ecommerce_lakehouse.silver.product_catalog",
        "quarantine_table_name": "product_catalog"
    },
    {
        "pipeline_name": "silver_inventory_snapshot_load",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.inventory_snapshot",
        "target_table": "ecommerce_lakehouse.silver.inventory_snapshot",
        "quarantine_table_name": "inventory_snapshot"
    },
    {
        "pipeline_name": "silver_vendor_master_load",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.vendor_master",
        "target_table": "ecommerce_lakehouse.silver.vendor_master",
        "quarantine_table_name": "vendor_master"
    },

    # -------------------------------
    # Silver CDC tables
    # -------------------------------
    {
        "pipeline_name": "silver_customers_cdc_load",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.customers",
        "target_table": "ecommerce_lakehouse.silver.customers",
        "quarantine_table_name": "customers"
    },
    {
        "pipeline_name": "silver_orders_cdc_load",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.orders",
        "target_table": "ecommerce_lakehouse.silver.orders",
        "quarantine_table_name": "orders"
    },
    {
        "pipeline_name": "silver_payments_cdc_load",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.payments",
        "target_table": "ecommerce_lakehouse.silver.payments",
        "quarantine_table_name": "payments"
    },
    {
        "pipeline_name": "silver_order_items_cdc_load",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.order_items",
        "target_table": "ecommerce_lakehouse.silver.order_items",
        "quarantine_table_name": "order_items"
    },

    # -------------------------------
    # Silver log tables
    # -------------------------------
    {
        "pipeline_name": "silver_clickstream_logs_parse",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.clickstream_logs",
        "target_table": "ecommerce_lakehouse.silver.clickstream_logs",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "silver_app_events_logs_parse",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.app_events_logs",
        "target_table": "ecommerce_lakehouse.silver.app_events_logs",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "silver_payment_gateway_logs_parse",
        "pipeline_layer": "silver",
        "source_table": "ecommerce_lakehouse.bronze.payment_gateway_logs",
        "target_table": "ecommerce_lakehouse.silver.payment_gateway_logs",
        "quarantine_table_name": None
    },

    # -------------------------------
    # Gold dimensions
    # -------------------------------
    {
        "pipeline_name": "gold_dim_product_scd1",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.silver.product_catalog",
        "target_table": "ecommerce_lakehouse.gold.dim_product",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_dim_customer_scd2",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.silver.customers",
        "target_table": "ecommerce_lakehouse.gold.dim_customer",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_dim_vendor_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.silver.vendor_master",
        "target_table": "ecommerce_lakehouse.gold.dim_vendor",
        "quarantine_table_name": None
    },

    # -------------------------------
    # Gold facts
    # -------------------------------
    {
        "pipeline_name": "gold_fact_orders_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.silver.orders",
        "target_table": "ecommerce_lakehouse.gold.fact_orders",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_fact_order_items_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.silver.order_items",
        "target_table": "ecommerce_lakehouse.gold.fact_order_items",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_fact_payments_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.silver.payments",
        "target_table": "ecommerce_lakehouse.gold.fact_payments",
        "quarantine_table_name": None
    },

    # -------------------------------
    # Gold summaries
    # -------------------------------
    {
        "pipeline_name": "gold_daily_sales_summary_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.gold.fact_orders",
        "target_table": "ecommerce_lakehouse.gold.daily_sales_summary",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_payment_failure_summary_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.gold.fact_payments",
        "target_table": "ecommerce_lakehouse.gold.payment_failure_summary",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_product_performance_summary_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.gold.fact_order_items",
        "target_table": "ecommerce_lakehouse.gold.product_performance_summary",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_customer_360_summary_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.gold.fact_orders",
        "target_table": "ecommerce_lakehouse.gold.customer_360_summary",
        "quarantine_table_name": None
    },
    {
        "pipeline_name": "gold_inventory_alert_summary_build",
        "pipeline_layer": "gold",
        "source_table": "ecommerce_lakehouse.silver.inventory_snapshot",
        "target_table": "ecommerce_lakehouse.gold.inventory_alert_summary",
        "quarantine_table_name": None
    }
]


# =======================================================
# 4. Dynamic audit logging loop
# =======================================================

for pipeline in pipeline_audit_config:
    start_time = datetime.now()

    try:
        source_table = pipeline["source_table"]
        target_table = pipeline["target_table"]
        quarantine_table_name = pipeline["quarantine_table_name"]

        # Count input and output records
        records_read = spark.table(source_table).count()
        records_written = spark.table(target_table).count()

        # Count failed/quarantine records only for Silver DQ pipelines
        if quarantine_table_name is not None:
            records_failed = spark.sql(f"""
                SELECT COUNT(*) AS failed_count
                FROM ecommerce_lakehouse.governance.quarantine_records
                WHERE table_name = '{quarantine_table_name}'
                  AND pipeline_stage = 'bronze_to_silver'
            """).collect()[0]["failed_count"]
        else:
            records_failed = 0

        end_time = datetime.now()

        write_pipeline_audit_log(
            pipeline_name=pipeline["pipeline_name"],
            pipeline_layer=pipeline["pipeline_layer"],
            source_table=source_table,
            target_table=target_table,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
            error_message=None
        )

    except Exception as e:
        end_time = datetime.now()

        write_pipeline_audit_log(
            pipeline_name=pipeline["pipeline_name"],
            pipeline_layer=pipeline["pipeline_layer"],
            source_table=pipeline["source_table"],
            target_table=pipeline["target_table"],
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            records_read=None,
            records_written=None,
            records_failed=None,
            error_message=str(e)
        )

        print(f"FAILED: {pipeline['pipeline_name']} | Error: {str(e)}")


print("Dynamic pipeline audit logging completed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_layer,
# MAGIC     status,
# MAGIC     COUNT(*) AS run_count,
# MAGIC     SUM(records_read) AS total_records_read,
# MAGIC     SUM(records_written) AS total_records_written,
# MAGIC     SUM(records_failed) AS total_records_failed
# MAGIC FROM ecommerce_lakehouse.governance.pipeline_audit_log
# MAGIC GROUP BY pipeline_layer, status
# MAGIC ORDER BY pipeline_layer, status;