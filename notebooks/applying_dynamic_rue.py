# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce_lakehouse.governance.quarantine_records (
# MAGIC     source_name STRING,
# MAGIC     table_name STRING,
# MAGIC     record_data STRING,
# MAGIC     error_reason STRING,
# MAGIC     source_file_name STRING,
# MAGIC     error_timestamp TIMESTAMP,
# MAGIC     pipeline_stage STRING
# MAGIC );

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, to_json, struct
from functools import reduce


# -------------------------------------------------------
# 1. Build Spark condition from one DQ rule
# -------------------------------------------------------

def build_rule_condition(rule):
    """
    Convert one DQ rule row into a Spark condition.
    """

    column_name = rule["column_name"]
    rule_type = rule["rule_type"]
    rule_value = rule["rule_value"]

    if rule_type == "not_null":
        return col(column_name).isNotNull()

    elif rule_type == "greater_than":
        return col(column_name) > float(rule_value)

    elif rule_type == "greater_than_equal":
        return col(column_name) >= float(rule_value)

    elif rule_type == "less_than":
        return col(column_name) < float(rule_value)

    elif rule_type == "less_than_equal":
        return col(column_name) <= float(rule_value)

    elif rule_type == "less_than_equal_column":
        return col(column_name) <= col(rule_value)

    elif rule_type == "allowed_values":
        allowed_values = [value.strip() for value in rule_value.split(",")]
        return col(column_name).isin(allowed_values)

    else:
        raise ValueError(f"Unsupported rule type: {rule_type}")


# -------------------------------------------------------
# 2. Apply DQ rules dynamically
# -------------------------------------------------------

def apply_dynamic_dq_rules(df, table_name):
    """
    Read DQ rules for one table and split records into valid and invalid.
    """

    rules_df = spark.sql(f"""
        SELECT *
        FROM ecommerce_lakehouse.governance.data_quality_rules
        WHERE table_name = '{table_name}'
          AND enabled = true
    """)

    rules = rules_df.collect()

    if len(rules) == 0:
        print(f"No DQ rules found for table: {table_name}")
        return df, df.limit(0)

    conditions = []

    for rule in rules:
        column_name = rule["column_name"]
        rule_type = rule["rule_type"]
        rule_value = rule["rule_value"]

        # Check source column exists
        if column_name not in df.columns:
            raise ValueError(
                f"Column '{column_name}' from DQ rules does not exist in table '{table_name}'. "
                f"Available columns: {df.columns}"
            )

        # For column-to-column rule, check comparison column exists
        if rule_type == "less_than_equal_column" and rule_value not in df.columns:
            raise ValueError(
                f"Comparison column '{rule_value}' from DQ rules does not exist in table '{table_name}'. "
                f"Available columns: {df.columns}"
            )

        condition = build_rule_condition(rule)
        conditions.append(condition)

    final_condition = reduce(lambda a, b: a & b, conditions)

    valid_df = df.filter(final_condition)
    invalid_df = df.filter(~final_condition)

    return valid_df, invalid_df


# -------------------------------------------------------
# 3. Prepare Silver input with table-specific type casting
# -------------------------------------------------------

def prepare_silver_input(df, table_name):
    """
    Convert columns into proper data types before applying DQ.
    """

    if table_name == "product_catalog":
        df = (
            df
            .withColumn("product_id", col("product_id").cast("int"))
            .withColumn("mrp", col("mrp").cast("decimal(10,2)"))
            .withColumn("selling_price", col("selling_price").cast("decimal(10,2)"))
            .withColumn("is_active", col("is_active").cast("boolean"))
        )

    elif table_name == "inventory_snapshot":
        df = (
            df
            .withColumn("product_id", col("product_id").cast("int"))
            .withColumn("stock_quantity", col("stock_quantity").cast("int"))
            .withColumn("reorder_level", col("reorder_level").cast("int"))
        )

    df = df.withColumn("processed_timestamp", current_timestamp())

    return df


# -------------------------------------------------------
# 4. Process one flat Bronze table to Silver + Quarantine
# -------------------------------------------------------

def process_flat_silver_table(table_name):
    """
    Process one flat Bronze table:
    Bronze -> type casting -> DQ rules -> Silver + Quarantine
    """

    bronze_table = f"ecommerce_lakehouse.bronze.{table_name}"
    silver_table = f"ecommerce_lakehouse.silver.{table_name}"

    print("=" * 80)
    print(f"Processing table: {table_name}")
    print(f"Bronze table: {bronze_table}")
    print(f"Silver table: {silver_table}")

    bronze_df = spark.table(bronze_table)

    silver_input_df = prepare_silver_input(bronze_df, table_name)

    valid_df, invalid_df = apply_dynamic_dq_rules(
        df=silver_input_df,
        table_name=table_name
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"Valid records: {valid_count}")
    print(f"Invalid records: {invalid_count}")

    # Write valid records to Silver
    valid_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)

    # Remove old quarantine records for same table to avoid duplicates during rerun
    spark.sql(f"""
        DELETE FROM ecommerce_lakehouse.governance.quarantine_records
        WHERE table_name = '{table_name}'
          AND pipeline_stage = 'bronze_to_silver'
    """)

    # Write invalid records to quarantine
    quarantine_df = (
        invalid_df
        .withColumn("source_name", lit(table_name))
        .withColumn("table_name", lit(table_name))
        .withColumn("record_data", to_json(struct("*")))
        .withColumn("error_reason", lit("One or more DQ rules failed"))
        .withColumn("error_timestamp", current_timestamp())
        .withColumn("pipeline_stage", lit("bronze_to_silver"))
        .select(
            "source_name",
            "table_name",
            "record_data",
            "error_reason",
            "source_file_name",
            "error_timestamp",
            "pipeline_stage"
        )
    )

    quarantine_df.write.mode("append").format("delta").saveAsTable(
        "ecommerce_lakehouse.governance.quarantine_records"
    )

    print(f"Completed Silver processing for: {table_name}")


# -------------------------------------------------------
# 5. Process only flat tables for now
# -------------------------------------------------------

flat_tables_to_process = [
    "product_catalog",
    "inventory_snapshot"
]

for table_name in flat_tables_to_process:
    process_flat_silver_table(table_name)

print("Flat Silver tables processed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ecommerce_lakehouse.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.governance.quarantine_records;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM ecommerce_lakehouse.silver.product_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM ecommerce_lakehouse.silver.inventory_snapshot;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType
from decimal import Decimal

test_bad_data = [
    (None, "Bad Product", "Test", "BrandX", "V001", Decimal("1000.00"), Decimal("1200.00"), True, "test_file.csv")
]

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("mrp", DecimalType(10, 2), True),
    StructField("selling_price", DecimalType(10, 2), True),
    StructField("is_active", BooleanType(), True),
    StructField("source_file_name", StringType(), True)
])

test_df = spark.createDataFrame(test_bad_data, schema)
test_df = test_df.withColumn("processed_timestamp", current_timestamp())

valid_test_df, invalid_test_df = apply_dynamic_dq_rules(
    df=test_df,
    table_name="product_catalog"
)

print("Valid test records:", valid_test_df.count())
print("Invalid test records:", invalid_test_df.count())

display(invalid_test_df)

# COMMAND ----------

