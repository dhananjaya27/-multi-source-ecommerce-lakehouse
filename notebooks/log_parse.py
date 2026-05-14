# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.bronze.clickstream_logs
# MAGIC LIMIT 10;

# COMMAND ----------

from pyspark.sql.functions import (
    col, regexp_extract, to_timestamp, current_timestamp
)

# -------------------------------------------------------
# 1. Read Bronze clickstream logs
# -------------------------------------------------------

bronze_clickstream_df = spark.table(
    "ecommerce_lakehouse.bronze.clickstream_logs"
)

# -------------------------------------------------------
# 2. Parse raw log line from value column
# -------------------------------------------------------

silver_clickstream_df = (
    bronze_clickstream_df
    .withColumn(
        "event_time",
        to_timestamp(
            regexp_extract(
                col("value"),
                r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
                1
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "log_level",
        regexp_extract(col("value"), r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+(\w+)", 1)
    )
    .withColumn(
        "user_id",
        regexp_extract(col("value"), r"user_id=(\d+)", 1).cast("long")
    )
    .withColumn(
        "session_id",
        regexp_extract(col("value"), r"session_id=([^\s]+)", 1)
    )
    .withColumn(
        "event_name",
        regexp_extract(col("value"), r"event=([^\s]+)", 1)
    )
    .withColumn(
        "product_id",
        regexp_extract(col("value"), r"product_id=(\d+)", 1).cast("long")
    )
    .withColumn(
        "page",
        regexp_extract(col("value"), r"page=([^\s]+)", 1)
    )
    .withColumn(
        "device",
        regexp_extract(col("value"), r"device=([^\s]+)", 1)
    )
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "event_time",
        "log_level",
        "user_id",
        "session_id",
        "event_name",
        "product_id",
        "page",
        "device",
        "value",
        "source_file_name",
        "ingestion_timestamp",
        "source_system",
        "load_date",
        "processed_timestamp"
    )
)

# -------------------------------------------------------
# 3. Write parsed logs to Silver
# -------------------------------------------------------

(
    silver_clickstream_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_lakehouse.silver.clickstream_logs")
)

print("silver.clickstream_logs created successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.clickstream_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT value
# MAGIC FROM ecommerce_lakehouse.bronze.app_events_logs
# MAGIC LIMIT 5;

# COMMAND ----------

from pyspark.sql.functions import expr, current_timestamp

bronze_app_events_df = spark.table(
    "ecommerce_lakehouse.bronze.app_events_logs"
)

silver_app_events_df = (
    bronze_app_events_df
    .withColumn(
        "event_time",
        expr("""
            to_timestamp(
                regexp_extract(value, '^(\\\\d{4}-\\\\d{2}-\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2})', 1),
                'yyyy-MM-dd HH:mm:ss'
            )
        """)
    )
    .withColumn(
        "log_level",
        expr("regexp_extract(value, '^\\\\d{4}-\\\\d{2}-\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2}\\\\s+(\\\\w+)', 1)")
    )
    .withColumn("service", expr("regexp_extract(value, 'service=([^\\\\s]+)', 1)"))
    .withColumn("event_name", expr("regexp_extract(value, 'event=([^\\\\s]+)', 1)"))

    # Use try_cast because these fields may be missing in some log lines
    .withColumn("order_id", expr("try_cast(regexp_extract(value, 'order_id=(\\\\d+)', 1) AS BIGINT)"))
    .withColumn("customer_id", expr("try_cast(regexp_extract(value, 'customer_id=(\\\\d+)', 1) AS BIGINT)"))
    .withColumn("payment_id", expr("try_cast(regexp_extract(value, 'payment_id=(\\\\d+)', 1) AS BIGINT)"))
    .withColumn("amount", expr("try_cast(regexp_extract(value, 'amount=([0-9.]+)', 1) AS DECIMAL(10,2))"))
    .withColumn("product_id", expr("try_cast(regexp_extract(value, 'product_id=(\\\\d+)', 1) AS BIGINT)"))
    .withColumn("stock_quantity", expr("try_cast(regexp_extract(value, 'stock_quantity=(\\\\d+)', 1) AS BIGINT)"))
    .withColumn("reorder_level", expr("try_cast(regexp_extract(value, 'reorder_level=(\\\\d+)', 1) AS BIGINT)"))

    .withColumn("transaction_id", expr("regexp_extract(value, 'transaction_id=([^\\\\s]+)', 1)"))
    .withColumn("carrier", expr("regexp_extract(value, 'carrier=([^\\\\s]+)', 1)"))
    .withColumn("tracking_id", expr("regexp_extract(value, 'tracking_id=([^\\\\s]+)', 1)"))
    .withColumn("status", expr("regexp_extract(value, 'status=([^\\\\s]+)', 1)"))
    .withColumn("processed_timestamp", current_timestamp())

    .select(
        "event_time",
        "log_level",
        "service",
        "event_name",
        "order_id",
        "customer_id",
        "payment_id",
        "amount",
        "transaction_id",
        "product_id",
        "stock_quantity",
        "reorder_level",
        "carrier",
        "tracking_id",
        "status",
        "value",
        "source_file_name",
        "ingestion_timestamp",
        "source_system",
        "load_date",
        "processed_timestamp"
    )
)

(
    silver_app_events_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_lakehouse.silver.app_events_logs")
)

print("silver.app_events_logs created successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT value
# MAGIC FROM ecommerce_lakehouse.bronze.payment_gateway_logs
# MAGIC LIMIT 5;

# COMMAND ----------

from pyspark.sql.functions import expr, current_timestamp

# -------------------------------------------------------
# 1. Read Bronze payment gateway logs
# -------------------------------------------------------

bronze_payment_gateway_df = spark.table(
    "ecommerce_lakehouse.bronze.payment_gateway_logs"
)

# -------------------------------------------------------
# 2. Parse raw log line from value column
# -------------------------------------------------------

silver_payment_gateway_df = (
    bronze_payment_gateway_df
    .withColumn(
        "event_time",
        expr("""
            to_timestamp(
                regexp_extract(value, '^(\\\\d{4}-\\\\d{2}-\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2})', 1),
                'yyyy-MM-dd HH:mm:ss'
            )
        """)
    )
    .withColumn(
        "log_level",
        expr("regexp_extract(value, '^\\\\d{4}-\\\\d{2}-\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2}\\\\s+(\\\\w+)', 1)")
    )
    .withColumn("gateway", expr("regexp_extract(value, 'gateway=([^\\\\s]+)', 1)"))
    .withColumn("event_name", expr("regexp_extract(value, 'event=([^\\\\s]+)', 1)"))

    # Use try_cast because some fields may be missing in some log lines
    .withColumn("order_id", expr("try_cast(regexp_extract(value, 'order_id=(\\\\d+)', 1) AS BIGINT)"))
    .withColumn("payment_id", expr("try_cast(regexp_extract(value, 'payment_id=(\\\\d+)', 1) AS BIGINT)"))
    .withColumn("amount", expr("try_cast(regexp_extract(value, 'amount=([0-9.]+)', 1) AS DECIMAL(10,2))"))

    .withColumn("status", expr("regexp_extract(value, 'status=([^\\\\s]+)', 1)"))
    .withColumn("failure_reason", expr("regexp_extract(value, 'failure_reason=([^\\\\s]+)', 1)"))
    .withColumn("transaction_id", expr("regexp_extract(value, 'transaction_id=([^\\\\s]+)', 1)"))
    .withColumn("refund_id", expr("regexp_extract(value, 'refund_id=([^\\\\s]+)', 1)"))

    .withColumn("processed_timestamp", current_timestamp())

    .select(
        "event_time",
        "log_level",
        "gateway",
        "event_name",
        "order_id",
        "payment_id",
        "amount",
        "status",
        "failure_reason",
        "transaction_id",
        "refund_id",
        "value",
        "source_file_name",
        "ingestion_timestamp",
        "source_system",
        "load_date",
        "processed_timestamp"
    )
)

# -------------------------------------------------------
# 3. Write parsed payment gateway logs to Silver
# -------------------------------------------------------

(
    silver_payment_gateway_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_lakehouse.silver.payment_gateway_logs")
)

print("silver.payment_gateway_logs created successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS payment_gateway_logs_count
# MAGIC FROM ecommerce_lakehouse.silver.payment_gateway_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.payment_gateway_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS product_catalog_count FROM ecommerce_lakehouse.silver.product_catalog;
# MAGIC SELECT COUNT(*) AS inventory_snapshot_count FROM ecommerce_lakehouse.silver.inventory_snapshot;
# MAGIC SELECT COUNT(*) AS customers_count FROM ecommerce_lakehouse.silver.customers;
# MAGIC SELECT COUNT(*) AS orders_count FROM ecommerce_lakehouse.silver.orders;
# MAGIC SELECT COUNT(*) AS payments_count FROM ecommerce_lakehouse.silver.payments;
# MAGIC SELECT COUNT(*) AS order_items_count FROM ecommerce_lakehouse.silver.order_items;
# MAGIC SELECT COUNT(*) AS clickstream_logs_count FROM ecommerce_lakehouse.silver.clickstream_logs;
# MAGIC SELECT COUNT(*) AS app_events_logs_count FROM ecommerce_lakehouse.silver.app_events_logs;
# MAGIC SELECT COUNT(*) AS payment_gateway_logs_count FROM ecommerce_lakehouse.silver.payment_gateway_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.product_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.inventory_snapshot;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.customers;
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.orders;
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.payments;
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.clickstream_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.clickstream_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.payment_gateway_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.governance.quarantine_records;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS product_catalog_count 
# MAGIC FROM ecommerce_lakehouse.silver.product_catalog;
# MAGIC
# MAGIC SELECT COUNT(*) AS inventory_snapshot_count 
# MAGIC FROM ecommerce_lakehouse.silver.inventory_snapshot;
# MAGIC
# MAGIC SELECT COUNT(*) AS customers_count 
# MAGIC FROM ecommerce_lakehouse.silver.customers;
# MAGIC
# MAGIC SELECT COUNT(*) AS orders_count 
# MAGIC FROM ecommerce_lakehouse.silver.orders;
# MAGIC
# MAGIC SELECT COUNT(*) AS payments_count 
# MAGIC FROM ecommerce_lakehouse.silver.payments;
# MAGIC
# MAGIC SELECT COUNT(*) AS order_items_count 
# MAGIC FROM ecommerce_lakehouse.silver.order_items;
# MAGIC
# MAGIC SELECT COUNT(*) AS clickstream_logs_count 
# MAGIC FROM ecommerce_lakehouse.silver.clickstream_logs;
# MAGIC
# MAGIC SELECT COUNT(*) AS app_events_logs_count 
# MAGIC FROM ecommerce_lakehouse.silver.app_events_logs;
# MAGIC
# MAGIC SELECT COUNT(*) AS payment_gateway_logs_count 
# MAGIC FROM ecommerce_lakehouse.silver.payment_gateway_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   COUNT(*) AS quarantine_count
# MAGIC FROM ecommerce_lakehouse.governance.quarantine_records
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name;