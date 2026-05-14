# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce_lakehouse.governance.data_quality_rules (
# MAGIC     table_name STRING,
# MAGIC     column_name STRING,
# MAGIC     rule_type STRING,
# MAGIC     rule_value STRING,
# MAGIC     error_message STRING,
# MAGIC     severity STRING,
# MAGIC     enabled BOOLEAN,
# MAGIC     created_at TIMESTAMP
# MAGIC );

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

dq_rules_path = "abfss://ecommerce-lakehouse@ecommercesanulakehouse.dfs.core.windows.net/raw/config/data_quality_rules.csv"

dq_rules_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load(dq_rules_path)
    .withColumn("enabled", col("enabled").cast("boolean"))
    .withColumn("created_at", current_timestamp())
)

dq_rules_df.write.mode("overwrite").format("delta").saveAsTable(
    "ecommerce_lakehouse.governance.data_quality_rules"
)

display(dq_rules_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ecommerce_lakehouse.governance.data_quality_rules;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ecommerce_lakehouse.governance.data_quality_rules (
# MAGIC     table_name STRING,
# MAGIC     column_name STRING,
# MAGIC     rule_type STRING,
# MAGIC     rule_value STRING,
# MAGIC     error_message STRING,
# MAGIC     severity STRING,
# MAGIC     enabled BOOLEAN,
# MAGIC     created_at TIMESTAMP
# MAGIC );

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

dq_rules_path = "abfss://ecommerce-lakehouse@ecommercesanulakehouse.dfs.core.windows.net/raw/config/data_quality_rules.csv"

# Read CSV
dq_rules_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load(dq_rules_path)
)

# Clean column names: remove spaces from beginning/end
for old_col in dq_rules_df.columns:
    dq_rules_df = dq_rules_df.withColumnRenamed(old_col, old_col.strip())

# Select columns in correct order and add created_at
dq_rules_df = (
    dq_rules_df
    .select(
        "table_name",
        "column_name",
        "rule_type",
        "rule_value",
        "error_message",
        "severity",
        "enabled"
    )
    .withColumn("enabled", col("enabled").cast("boolean"))
    .withColumn("created_at", current_timestamp())
)

# Write to governance table
(
    dq_rules_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_lakehouse.governance.data_quality_rules")
)

display(dq_rules_df)