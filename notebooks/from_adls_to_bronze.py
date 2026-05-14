# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# Project values
storage_account = "ecommercesanulakehouse"
container = "ecommerce-lakehouse"
catalog = "ecommerce_lakehouse"

raw_base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw/"

metadata_rows = []

for folder in dbutils.fs.ls(raw_base_path):
    source_name = folder.name.replace("/", "")

    # Skip system folders like _checkpoints
    if source_name.startswith("_"):
        continue

    files = dbutils.fs.ls(folder.path)

    # Skip empty folders
    if len(files) == 0:
        continue

    first_file_name = files[0].name.lower()

    # Detect file format
    if first_file_name.endswith(".csv"):
        file_format = "csv"
    elif first_file_name.endswith(".json"):
        file_format = "json"
    elif first_file_name.endswith(".log"):
        file_format = "text"
    else:
        print(f"Skipping unsupported file type for source: {source_name}")
        continue

    # Detect source type
    source_type = "file"

    if source_name.endswith("_api"):
        source_type = "api"

    if source_name in ["customers", "orders", "order_items", "payments"]:
        source_type = "cdc"

    raw_path = folder.path

    bronze_table = f"{catalog}.bronze.{source_name}"

    schema_path = (
        f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        f"raw/_checkpoints/bronze/{source_name}/schema/"
    )

    checkpoint_path = (
        f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        f"raw/_checkpoints/bronze/{source_name}/checkpoint/"
    )

    load_type = "full_load"

    if source_type == "cdc":
        load_type = "cdc"

    metadata_rows.append(
        (
            source_name,
            source_type,
            file_format,
            raw_path,
            bronze_table,
            schema_path,
            checkpoint_path,
            source_name,
            load_type,
            True,
        )
    )

metadata_columns = [
    "source_name",
    "source_type",
    "file_format",
    "raw_path",
    "bronze_table",
    "schema_path",
    "checkpoint_path",
    "source_system",
    "load_type",
    "enabled",
]

metadata_df = spark.createDataFrame(metadata_rows, metadata_columns)

metadata_df = metadata_df.withColumn("created_at", current_timestamp())

metadata_df.write.mode("overwrite").saveAsTable(
    "ecommerce_lakehouse.governance.source_metadata"
)

print("source_metadata table refreshed successfully.")

display(metadata_df.orderBy("source_name"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   source_name,
# MAGIC   source_type,
# MAGIC   file_format,
# MAGIC   bronze_table,
# MAGIC   enabled
# MAGIC FROM ecommerce_lakehouse.governance.source_metadata
# MAGIC ORDER BY source_name;

# COMMAND ----------

cdc_sources_df = spark.sql("""
    SELECT *
    FROM ecommerce_lakehouse.governance.source_metadata
    WHERE enabled = true
      AND source_type = 'cdc'
    ORDER BY source_name
""")

cdc_sources = cdc_sources_df.collect()

for source_config in cdc_sources:
    load_bronze_from_metadata(source_config)

print("CDC Bronze sources loaded successfully.")