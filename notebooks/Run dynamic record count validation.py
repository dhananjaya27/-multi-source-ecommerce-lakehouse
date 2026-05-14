# Databricks notebook source
# MAGIC %md
# MAGIC Run dynamic record count validation

# COMMAND ----------

from datetime import datetime
import uuid
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

# =======================================================
# 1. Function to write validation result
# =======================================================

def write_record_count_validation(
    validation_name,
    source_table,
    target_table,
    source_count,
    target_count,
    failed_count,
    difference_count,
    validation_status,
    validation_message
):
    validation_schema = StructType([
        StructField("validation_id", StringType(), True),
        StructField("validation_name", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("source_count", LongType(), True),
        StructField("target_count", LongType(), True),
        StructField("failed_count", LongType(), True),
        StructField("difference_count", LongType(), True),
        StructField("validation_status", StringType(), True),
        StructField("validation_message", StringType(), True),
        StructField("validation_timestamp", TimestampType(), True)
    ])

    validation_data = [(
        str(uuid.uuid4()),
        validation_name,
        source_table,
        target_table,
        int(source_count) if source_count is not None else None,
        int(target_count) if target_count is not None else None,
        int(failed_count) if failed_count is not None else None,
        int(difference_count) if difference_count is not None else None,
        validation_status,
        validation_message,
        datetime.now()
    )]

    validation_df = spark.createDataFrame(
        validation_data,
        schema=validation_schema
    )

    validation_df.write.mode("append").format("delta").saveAsTable(
        "ecommerce_lakehouse.governance.record_count_validation"
    )


# =======================================================
# 2. Validation configuration
# =======================================================

validation_config = [
    {
        "validation_name": "bronze_to_silver_product_catalog",
        "source_table": "ecommerce_lakehouse.bronze.product_catalog",
        "target_table": "ecommerce_lakehouse.silver.product_catalog",
        "quarantine_table_name": "product_catalog",
        "validation_type": "source_equals_target_plus_failed"
    },
    {
        "validation_name": "bronze_to_silver_inventory_snapshot",
        "source_table": "ecommerce_lakehouse.bronze.inventory_snapshot",
        "target_table": "ecommerce_lakehouse.silver.inventory_snapshot",
        "quarantine_table_name": "inventory_snapshot",
        "validation_type": "source_equals_target_plus_failed"
    },
    {
        "validation_name": "bronze_to_silver_vendor_master",
        "source_table": "ecommerce_lakehouse.bronze.vendor_master",
        "target_table": "ecommerce_lakehouse.silver.vendor_master",
        "quarantine_table_name": "vendor_master",
        "validation_type": "source_equals_target_plus_failed"
    },
    {
        "validation_name": "bronze_to_silver_customers",
        "source_table": "ecommerce_lakehouse.bronze.customers",
        "target_table": "ecommerce_lakehouse.silver.customers",
        "quarantine_table_name": "customers",
        "validation_type": "source_equals_target_plus_failed"
    },
    {
        "validation_name": "bronze_to_silver_orders",
        "source_table": "ecommerce_lakehouse.bronze.orders",
        "target_table": "ecommerce_lakehouse.silver.orders",
        "quarantine_table_name": "orders",
        "validation_type": "source_equals_target_plus_failed"
    },
    {
        "validation_name": "bronze_to_silver_payments",
        "source_table": "ecommerce_lakehouse.bronze.payments",
        "target_table": "ecommerce_lakehouse.silver.payments",
        "quarantine_table_name": "payments",
        "validation_type": "source_equals_target_plus_failed"
    },
    {
        "validation_name": "bronze_to_silver_order_items",
        "source_table": "ecommerce_lakehouse.bronze.order_items",
        "target_table": "ecommerce_lakehouse.silver.order_items",
        "quarantine_table_name": "order_items",
        "validation_type": "source_equals_target_plus_failed"
    },
    {
        "validation_name": "silver_to_gold_fact_orders",
        "source_table": "ecommerce_lakehouse.silver.orders",
        "target_table": "ecommerce_lakehouse.gold.fact_orders",
        "quarantine_table_name": None,
        "validation_type": "source_equals_target"
    },
    {
        "validation_name": "silver_to_gold_fact_payments",
        "source_table": "ecommerce_lakehouse.silver.payments",
        "target_table": "ecommerce_lakehouse.gold.fact_payments",
        "quarantine_table_name": None,
        "validation_type": "source_equals_target"
    },
    {
        "validation_name": "silver_to_gold_fact_order_items",
        "source_table": "ecommerce_lakehouse.silver.order_items",
        "target_table": "ecommerce_lakehouse.gold.fact_order_items",
        "quarantine_table_name": None,
        "validation_type": "source_equals_target"
    }
]


# =======================================================
# 3. Run validations dynamically
# =======================================================

for validation in validation_config:
    validation_name = validation["validation_name"]
    source_table = validation["source_table"]
    target_table = validation["target_table"]
    quarantine_table_name = validation["quarantine_table_name"]
    validation_type = validation["validation_type"]

    try:
        source_count = spark.table(source_table).count()
        target_count = spark.table(target_table).count()

        if quarantine_table_name is not None:
            failed_count = spark.sql(f"""
                SELECT COUNT(*) AS failed_count
                FROM ecommerce_lakehouse.governance.quarantine_records
                WHERE table_name = '{quarantine_table_name}'
                  AND pipeline_stage = 'bronze_to_silver'
            """).collect()[0]["failed_count"]
        else:
            failed_count = 0

        if validation_type == "source_equals_target_plus_failed":
            difference_count = source_count - target_count - failed_count

            if difference_count == 0:
                validation_status = "PASS"
                validation_message = "Source count matches target count plus failed count."
            else:
                validation_status = "FAIL"
                validation_message = "Source count does not match target count plus failed count."

        elif validation_type == "source_equals_target":
            difference_count = source_count - target_count

            if difference_count == 0:
                validation_status = "PASS"
                validation_message = "Source count matches target count."
            else:
                validation_status = "FAIL"
                validation_message = "Source count does not match target count."

        else:
            difference_count = None
            validation_status = "FAILED"
            validation_message = f"Unsupported validation type: {validation_type}"

        write_record_count_validation(
            validation_name=validation_name,
            source_table=source_table,
            target_table=target_table,
            source_count=source_count,
            target_count=target_count,
            failed_count=failed_count,
            difference_count=difference_count,
            validation_status=validation_status,
            validation_message=validation_message
        )

        print(f"{validation_name}: {validation_status}")

    except Exception as e:
        write_record_count_validation(
            validation_name=validation_name,
            source_table=source_table,
            target_table=target_table,
            source_count=None,
            target_count=None,
            failed_count=None,
            difference_count=None,
            validation_status="FAILED",
            validation_message=str(e)
        )

        print(f"{validation_name}: FAILED - {str(e)}")


print("Record count validation completed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.governance.record_count_validation
# MAGIC ORDER BY validation_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     validation_status,
# MAGIC     COUNT(*) AS validation_count
# MAGIC FROM ecommerce_lakehouse.governance.record_count_validation
# MAGIC GROUP BY validation_status;