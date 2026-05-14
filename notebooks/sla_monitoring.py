# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce_lakehouse.governance.sla_monitoring (
# MAGIC     sla_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     pipeline_layer STRING,
# MAGIC     expected_duration_minutes DOUBLE,
# MAGIC     actual_duration_minutes DOUBLE,
# MAGIC     sla_status STRING,
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     checked_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from datetime import datetime
import uuid
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

# =======================================================
# 1. Create SLA monitoring table if not exists
# =======================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS ecommerce_lakehouse.governance.sla_monitoring (
    sla_id STRING,
    pipeline_name STRING,
    pipeline_layer STRING,
    expected_duration_minutes DOUBLE,
    actual_duration_minutes DOUBLE,
    sla_status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    checked_at TIMESTAMP
)
USING DELTA
""")


# =======================================================
# 2. Optional: remove old SLA records for clean rerun
# =======================================================
# This avoids duplicate SLA rows every time we rerun this notebook.

spark.sql("""
DELETE FROM ecommerce_lakehouse.governance.sla_monitoring
""")


# =======================================================
# 3. Function to write one SLA result
# =======================================================

def write_sla_monitoring(
    pipeline_name,
    pipeline_layer,
    expected_duration_minutes,
    actual_duration_minutes,
    sla_status,
    start_time,
    end_time
):
    """
    Insert one SLA monitoring record into governance.sla_monitoring.
    """

    sla_schema = StructType([
        StructField("sla_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("pipeline_layer", StringType(), True),
        StructField("expected_duration_minutes", DoubleType(), True),
        StructField("actual_duration_minutes", DoubleType(), True),
        StructField("sla_status", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("checked_at", TimestampType(), True)
    ])

    sla_data = [(
        str(uuid.uuid4()),
        pipeline_name,
        pipeline_layer,
        float(expected_duration_minutes),
        float(actual_duration_minutes),
        sla_status,
        start_time,
        end_time,
        datetime.now()
    )]

    sla_df = spark.createDataFrame(sla_data, schema=sla_schema)

    sla_df.write.mode("append").format("delta").saveAsTable(
        "ecommerce_lakehouse.governance.sla_monitoring"
    )


# =======================================================
# 4. SLA configuration
# =======================================================
# Expected duration by layer.
# You can change these values later based on production SLA.

sla_config = {
    "bronze": 15.0,   # bronze pipelines should finish within 15 minutes
    "silver": 20.0,   # silver pipelines should finish within 20 minutes
    "gold": 30.0      # gold pipelines should finish within 30 minutes
}


# =======================================================
# 5. Read latest successful pipeline audit records
# =======================================================
# This reads pipeline start/end times from pipeline_audit_log.

pipeline_runs_df = spark.sql("""
    SELECT
        pipeline_name,
        pipeline_layer,
        start_time,
        end_time
    FROM ecommerce_lakehouse.governance.pipeline_audit_log
    WHERE status = 'SUCCESS'
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL
""")

pipeline_runs = pipeline_runs_df.collect()


# =======================================================
# 6. Calculate SLA status dynamically
# =======================================================

for run in pipeline_runs:
    pipeline_name = run["pipeline_name"]
    pipeline_layer = run["pipeline_layer"]
    start_time = run["start_time"]
    end_time = run["end_time"]

    expected_duration = sla_config.get(pipeline_layer, 30.0)

    actual_duration = (end_time - start_time).total_seconds() / 60

    if actual_duration <= expected_duration:
        sla_status = "MET"
    else:
        sla_status = "BREACHED"

    write_sla_monitoring(
        pipeline_name=pipeline_name,
        pipeline_layer=pipeline_layer,
        expected_duration_minutes=expected_duration,
        actual_duration_minutes=actual_duration,
        sla_status=sla_status,
        start_time=start_time,
        end_time=end_time
    )

    print(
        f"{pipeline_name} | Layer: {pipeline_layer} | "
        f"SLA: {sla_status} | "
        f"Actual: {actual_duration:.2f} mins | "
        f"Expected: {expected_duration} mins"
    )


print("SLA monitoring completed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_layer,
# MAGIC     sla_status,
# MAGIC     COUNT(*) AS pipeline_count,
# MAGIC     ROUND(AVG(actual_duration_minutes), 2) AS avg_actual_duration_minutes,
# MAGIC     MAX(actual_duration_minutes) AS max_actual_duration_minutes
# MAGIC FROM ecommerce_lakehouse.governance.sla_monitoring
# MAGIC GROUP BY pipeline_layer, sla_status
# MAGIC ORDER BY pipeline_layer, sla_status;