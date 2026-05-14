# Databricks notebook source
from pyspark.sql.functions import current_timestamp, current_date, lit, col


def load_bronze_from_metadata(source_config):
    """
    Load one source into Bronze using source_metadata table configuration.
    """

    source_name = source_config["source_name"]
    file_format = source_config["file_format"]
    raw_path = source_config["raw_path"]
    bronze_table = source_config["bronze_table"]
    schema_path = source_config["schema_path"]
    checkpoint_path = source_config["checkpoint_path"]
    source_system = source_config["source_system"]

    print(f"Starting Bronze load for source: {source_name}")
    print(f"Raw path: {raw_path}")
    print(f"Bronze table: {bronze_table}")

    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_path)
    )

    if file_format == "csv":
        df_reader = df_reader.option("header", "true")
        df = df_reader.load(raw_path)

    elif file_format == "json":
        inferred_schema = (
            spark.read
            .option("multiLine", "true")
            .json(raw_path)
            .schema
        )

        df = (
            df_reader
            .option("multiLine", "true")
            .schema(inferred_schema)
            .load(raw_path)
        )

    elif file_format == "text":
        df = df_reader.load(raw_path)

    else:
        print(f"Unsupported file format: {file_format} for source: {source_name}")
        return

    bronze_df = (
        df
        .withColumn("source_file_name", col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit(source_system))
        .withColumn("load_date", current_date())
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )

    query.awaitTermination()

    print(f"Completed Bronze load for source: {source_name}")


metadata_df = spark.sql("""
    SELECT *
    FROM ecommerce_lakehouse.governance.source_metadata
    WHERE enabled = true
    ORDER BY source_name
""")

enabled_sources = metadata_df.collect()

for source_config in enabled_sources:
    load_bronze_from_metadata(source_config)

print("All enabled Bronze sources processed successfully.")

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.bronze.customers;

# COMMAND ----------

cdc_sources_df = spark.sql("""
    SELECT *
    FROM ecommerce_lakehouse.governance.source_metadata
    WHERE enabled = true
      AND source_type = 'cdc'
    ORDER BY source_name
""")

display(cdc_sources_df)

# COMMAND ----------

cdc_sources = cdc_sources_df.collect()

for source_config in cdc_sources:
    load_bronze_from_metadata(source_config)

print("CDC Bronze sources loaded successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ecommerce_lakehouse.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.bronze.customers;