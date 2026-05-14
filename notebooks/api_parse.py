# Databricks notebook source
vendor_df = spark.table("ecommerce_lakehouse.bronze.vendor_master")
vendor_df.printSchema()
display(vendor_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, to_json, struct
from functools import reduce


# =======================================================
# 1. Add/refresh DQ rules for vendor_master
# =======================================================

spark.sql("""
DELETE FROM ecommerce_lakehouse.governance.data_quality_rules
WHERE table_name = 'vendor_master'
""")

spark.sql("""
INSERT INTO ecommerce_lakehouse.governance.data_quality_rules
VALUES
('vendor_master', 'vendor_id', 'not_null', '', 'vendor_id cannot be null', 'critical', true, current_timestamp()),
('vendor_master', 'vendor_name', 'not_null', '', 'vendor_name cannot be null', 'critical', true, current_timestamp()),
('vendor_master', 'vendor_contact_email', 'not_null', '', 'vendor_contact_email cannot be null', 'warning', true, current_timestamp()),
('vendor_master', 'vendor_contact_email', 'regex', '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$', 'invalid vendor email format', 'warning', true, current_timestamp())
""")


# =======================================================
# 2. Dynamic DQ helper functions
# =======================================================

def build_rule_condition(rule):
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

    elif rule_type == "regex":
        return col(column_name).rlike(rule_value)

    else:
        raise ValueError(f"Unsupported rule type: {rule_type}")


def apply_dynamic_dq_rules(df, table_name):
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

        if column_name not in df.columns:
            raise ValueError(
                f"Column '{column_name}' from DQ rules does not exist in DataFrame for table '{table_name}'. "
                f"Available columns: {df.columns}"
            )

        if rule_type == "less_than_equal_column" and rule_value not in df.columns:
            raise ValueError(
                f"Comparison column '{rule_value}' does not exist in DataFrame for table '{table_name}'. "
                f"Available columns: {df.columns}"
            )

        conditions.append(build_rule_condition(rule))

    final_condition = reduce(lambda a, b: a & b, conditions)

    valid_df = df.filter(final_condition)
    invalid_df = df.filter(~final_condition)

    return valid_df, invalid_df


# =======================================================
# 3. Read Bronze vendor_master
# =======================================================

bronze_vendor_df = spark.table("ecommerce_lakehouse.bronze.vendor_master")


# =======================================================
# 4. Prepare Silver vendor data
# =======================================================

silver_vendor_input_df = (
    bronze_vendor_df
    .withColumn("is_active", col("is_active").cast("boolean"))
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "vendor_id",
        "vendor_name",
        "vendor_city",
        "vendor_state",
        "vendor_contact_email",
        "is_active",
        "_rescued_data",
        "source_file_name",
        "ingestion_timestamp",
        "source_system",
        "load_date",
        "processed_timestamp"
    )
)


# =======================================================
# 5. Apply metadata-driven DQ rules
# =======================================================

valid_vendor_df, invalid_vendor_df = apply_dynamic_dq_rules(
    df=silver_vendor_input_df,
    table_name="vendor_master"
)

valid_count = valid_vendor_df.count()
invalid_count = invalid_vendor_df.count()

print(f"Valid vendor records: {valid_count}")
print(f"Invalid vendor records: {invalid_count}")


# =======================================================
# 6. Write valid records to Silver
# =======================================================

(
    valid_vendor_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_lakehouse.silver.vendor_master")
)

print("Valid vendor records written to ecommerce_lakehouse.silver.vendor_master")


# =======================================================
# 7. Remove old vendor quarantine records
# =======================================================

spark.sql("""
DELETE FROM ecommerce_lakehouse.governance.quarantine_records
WHERE table_name = 'vendor_master'
  AND pipeline_stage = 'bronze_to_silver'
""")


# =======================================================
# 8. Write invalid records to quarantine
# =======================================================

quarantine_vendor_df = (
    invalid_vendor_df
    .withColumn("source_name", lit("vendor_master"))
    .withColumn("table_name", lit("vendor_master"))
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

(
    quarantine_vendor_df.write
    .mode("append")
    .format("delta")
    .saveAsTable("ecommerce_lakehouse.governance.quarantine_records")
)

print("Invalid vendor records written to quarantine table")
print("Vendor Silver processing completed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.vendor_master;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS vendor_count
# MAGIC FROM ecommerce_lakehouse.silver.vendor_master;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.governance.quarantine_records
# MAGIC WHERE table_name = 'vendor_master';