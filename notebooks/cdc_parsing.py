# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE TABLE ecommerce_lakehouse.bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.bronze.orders
# MAGIC LIMIT 5;

# COMMAND ----------

orders_df = spark.table("ecommerce_lakehouse.bronze.orders")
orders_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when, to_timestamp

bronze_orders_df = spark.table("ecommerce_lakehouse.bronze.orders")

silver_orders_input_df = (
    bronze_orders_df
    .select(
        col("payload.after.order_id").cast("long").alias("order_id"),
        col("payload.after.customer_id").cast("long").alias("customer_id"),
        col("payload.after.order_date").alias("order_date_raw"),
        col("payload.after.order_status").alias("order_status"),
        col("payload.after.total_amount").cast("decimal(10,2)").alias("total_amount"),
        col("payload.after.city").alias("city"),
        col("payload.after.state").alias("state"),
        col("payload.after.updated_at").alias("updated_at_raw"),
        col("payload.op").alias("cdc_operation"),
        col("source_file_name"),
        col("ingestion_timestamp"),
        col("source_system"),
        col("load_date")
    )
    .withColumn(
        "is_deleted",
        when(col("cdc_operation") == "d", True).otherwise(False)
    )
    .withColumn("processed_timestamp", current_timestamp())
)

display(silver_orders_input_df)

# COMMAND ----------

# MAGIC %run /copied/path/here

# COMMAND ----------

# MAGIC %run ./applying_dynamic_rue

# COMMAND ----------

# MAGIC %run /Users/aditya.mishra143.150@gmail.com/applying_dynamic_rue

# COMMAND ----------

print(apply_dynamic_dq_rules)

# COMMAND ----------

valid_orders_df, invalid_orders_df = apply_dynamic_dq_rules(
    df=silver_orders_input_df,
    table_name="orders"
)

print("Valid orders:", valid_orders_df.count())
print("Invalid orders:", invalid_orders_df.count())

# COMMAND ----------

valid_orders_df.write.mode("overwrite").format("delta").saveAsTable(
    "ecommerce_lakehouse.silver.orders"
)

# COMMAND ----------

payments_df = spark.table("ecommerce_lakehouse.bronze.payments")
payments_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when, lit, to_json, struct

# -------------------------------------------------------
# 1. Read Bronze payments CDC table
# -------------------------------------------------------

bronze_payments_df = spark.table("ecommerce_lakehouse.bronze.payments")


# -------------------------------------------------------
# 2. Parse Debezium CDC payload
# -------------------------------------------------------

silver_payments_input_df = (
    bronze_payments_df
    .select(
        col("payload.after.payment_id").cast("long").alias("payment_id"),
        col("payload.after.order_id").cast("long").alias("order_id"),
        col("payload.after.payment_amount").cast("decimal(10,2)").alias("amount"),
        col("payload.after.payment_status").alias("payment_status"),
        col("payload.after.payment_method").alias("payment_method"),
        col("payload.after.transaction_id").alias("transaction_id"),
        col("payload.after.payment_date").alias("payment_date_raw"),
        col("payload.after.updated_at").alias("updated_at_raw"),
        col("payload.op").alias("cdc_operation"),
        col("source_file_name"),
        col("ingestion_timestamp"),
        col("source_system"),
        col("load_date")
    )
    .withColumn(
        "is_deleted",
        when(col("cdc_operation") == "d", True).otherwise(False)
    )
    .withColumn("processed_timestamp", current_timestamp())
)


# -------------------------------------------------------
# 3. Apply metadata-driven DQ rules
# -------------------------------------------------------

valid_payments_df, invalid_payments_df = apply_dynamic_dq_rules(
    df=silver_payments_input_df,
    table_name="payments"
)

valid_count = valid_payments_df.count()
invalid_count = invalid_payments_df.count()

print(f"Valid payments: {valid_count}")
print(f"Invalid payments: {invalid_count}")


# -------------------------------------------------------
# 4. Write valid records to Silver
# -------------------------------------------------------

valid_payments_df.write.mode("overwrite").format("delta").saveAsTable(
    "ecommerce_lakehouse.silver.payments"
)

print("Valid payment records written to ecommerce_lakehouse.silver.payments")


# -------------------------------------------------------
# 5. Remove old quarantine records for payments
# -------------------------------------------------------

spark.sql("""
    DELETE FROM ecommerce_lakehouse.governance.quarantine_records
    WHERE table_name = 'payments'
      AND pipeline_stage = 'bronze_to_silver'
""")


# -------------------------------------------------------
# 6. Write invalid records to quarantine
# -------------------------------------------------------

quarantine_payments_df = (
    invalid_payments_df
    .withColumn("source_name", lit("payments"))
    .withColumn("table_name", lit("payments"))
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

quarantine_payments_df.write.mode("append").format("delta").saveAsTable(
    "ecommerce_lakehouse.governance.quarantine_records"
)

print("Invalid payment records written to quarantine table")
print("Payments Silver processing completed successfully.")

# COMMAND ----------

valid_payments_df, invalid_payments_df = apply_dynamic_dq_rules(
    df=silver_payments_input_df,
    table_name="payments"
)

print("Valid payments:", valid_payments_df.count())
print("Invalid payments:", invalid_payments_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ecommerce_lakehouse.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.silver.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.governance.quarantine_records
# MAGIC WHERE table_name = 'payments';

# COMMAND ----------

customers_df = spark.table("ecommerce_lakehouse.bronze.customers")
customers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when

bronze_customers_df = spark.table("ecommerce_lakehouse.bronze.customers")

customers_parsed_df = bronze_customers_df.select(
    col("payload.after.customer_id").cast("long").alias("customer_id"),
    col("payload.after.first_name").cast("string").alias("first_name"),
    col("payload.after.last_name").cast("string").alias("last_name"),
    col("payload.after.email").cast("string").alias("email"),
    col("payload.after.phone").cast("string").alias("phone"),
    col("payload.after.city").cast("string").alias("city"),
    col("payload.after.state").cast("string").alias("state"),
    col("payload.after.country").cast("string").alias("country"),
    col("payload.after.created_at").cast("long").alias("created_at_raw"),
    col("payload.after.updated_at").cast("long").alias("updated_at_raw"),

    col("payload.op").cast("string").alias("cdc_operation"),

    when(col("payload.op") == "d", True)
    .otherwise(False)
    .alias("is_deleted"),

    col("source_file_name"),
    col("ingestion_timestamp"),
    col("source_system"),
    col("load_date"),

    current_timestamp().alias("processed_timestamp")
)

display(customers_parsed_df)

# COMMAND ----------

valid_customers_df, invalid_customers_df = apply_dynamic_dq_rules(
    customers_parsed_df,
    "customers"
)

print("Valid customers:", valid_customers_df.count())
print("Invalid customers:", invalid_customers_df.count())

display(valid_customers_df)
display(invalid_customers_df)

# COMMAND ----------

valid_customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce_lakehouse.silver.customers")

# COMMAND ----------

from pyspark.sql.functions import lit, to_json, struct, current_timestamp, col

invalid_customers_quarantine_df = (
    invalid_customers_df
    .select(
        lit("customers").alias("source_name"),
        lit("customers").alias("table_name"),
        to_json(struct("*")).alias("record_data"),
        lit("Failed customer DQ rules").alias("error_reason"),
        col("source_file_name"),
        current_timestamp().alias("error_timestamp"),
        lit("silver_customers_dq").alias("pipeline_stage")
    )
)

invalid_customers_quarantine_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("ecommerce_lakehouse.governance.quarantine_records")

# COMMAND ----------

# ============================================================
# SILVER ORDER_ITEMS PIPELINE
# bronze.order_items -> DQ -> silver.order_items + quarantine
# ============================================================

from pyspark.sql.functions import col, current_timestamp, when, lit, to_json, struct
from pyspark.sql import Row

# ------------------------------------------------------------
# 1. Read Bronze order_items
# ------------------------------------------------------------

bronze_order_items_df = spark.table("ecommerce_lakehouse.bronze.order_items")

print("Bronze order_items schema:")
bronze_order_items_df.printSchema()

display(bronze_order_items_df.limit(10))


# ------------------------------------------------------------
# 2. Parse Debezium CDC payload.after into flat Silver structure
# ------------------------------------------------------------

order_items_parsed_df = (
    bronze_order_items_df
    .select(
        col("payload.after.order_item_id").cast("long").alias("order_item_id"),
        col("payload.after.order_id").cast("long").alias("order_id"),
        col("payload.after.product_id").cast("long").alias("product_id"),
        col("payload.after.quantity").cast("int").alias("quantity"),
        col("payload.after.unit_price").cast("double").alias("unit_price"),

        col("payload.op").cast("string").alias("cdc_operation"),

        when(col("payload.op") == "d", True)
        .otherwise(False)
        .alias("is_deleted"),

        col("source_file_name"),
        col("ingestion_timestamp"),
        col("source_system"),
        col("load_date"),

        current_timestamp().alias("processed_timestamp")
    )
)

print("Parsed order_items schema:")
order_items_parsed_df.printSchema()

display(order_items_parsed_df)


# ------------------------------------------------------------
# 3. Check existing DQ rules for order_items
# ------------------------------------------------------------

existing_order_items_rules_df = spark.sql("""
    SELECT *
    FROM ecommerce_lakehouse.governance.data_quality_rules
    WHERE table_name = 'order_items'
""")

existing_rule_count = existing_order_items_rules_df.count()

print("Existing order_items DQ rules:", existing_rule_count)

display(existing_order_items_rules_df)


# ------------------------------------------------------------
# 4. Add order_items DQ rules only if they do not exist
# ------------------------------------------------------------

if existing_rule_count == 0:

    order_items_rules = [
        Row(
            rule_id="order_items_order_item_id_not_null",
            table_name="order_items",
            column_name="order_item_id",
            rule_type="not_null",
            rule_value=None,
            is_enabled=True
        ),
        Row(
            rule_id="order_items_order_id_not_null",
            table_name="order_items",
            column_name="order_id",
            rule_type="not_null",
            rule_value=None,
            is_enabled=True
        ),
        Row(
            rule_id="order_items_product_id_not_null",
            table_name="order_items",
            column_name="product_id",
            rule_type="not_null",
            rule_value=None,
            is_enabled=True
        ),
        Row(
            rule_id="order_items_quantity_greater_than",
            table_name="order_items",
            column_name="quantity",
            rule_type="greater_than",
            rule_value="0",
            is_enabled=True
        ),
        Row(
            rule_id="order_items_unit_price_greater_than_equal",
            table_name="order_items",
            column_name="unit_price",
            rule_type="greater_than_equal",
            rule_value="0",
            is_enabled=True
        )
    ]

    order_items_rules_df = spark.createDataFrame(order_items_rules)

    order_items_rules_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("ecommerce_lakehouse.governance.data_quality_rules")

    print("Order_items DQ rules added successfully.")

else:
    print("Order_items DQ rules already exist. Skipping rule insert.")


# ------------------------------------------------------------
# 5. Verify DQ rules
# ------------------------------------------------------------

display(
    spark.sql("""
        SELECT *
        FROM ecommerce_lakehouse.governance.data_quality_rules
        WHERE table_name = 'order_items'
        ORDER BY rule_id
    """)
)


# ------------------------------------------------------------
# 6. Apply metadata-driven DQ rules
# ------------------------------------------------------------
# Important:
# apply_dynamic_dq_rules() should already be available from your utility notebook.
# If not, run:
# %run /Users/aditya.mishra143.150@gmail.com/applying_dynamic_rue

valid_order_items_df, invalid_order_items_df = apply_dynamic_dq_rules(
    order_items_parsed_df,
    "order_items"
)

valid_count = valid_order_items_df.count()
invalid_count = invalid_order_items_df.count()

print("Valid order_items:", valid_count)
print("Invalid order_items:", invalid_count)

display(valid_order_items_df)
display(invalid_order_items_df)


# ------------------------------------------------------------
# 7. Write valid records to silver.order_items
# ------------------------------------------------------------

valid_order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce_lakehouse.silver.order_items")

print("Valid order_items written to ecommerce_lakehouse.silver.order_items")


# ------------------------------------------------------------
# 8. Verify silver.order_items
# ------------------------------------------------------------

display(spark.table("ecommerce_lakehouse.silver.order_items"))

spark.sql("""
    SELECT 
        COUNT(*) AS total_order_items,
        COUNT(DISTINCT order_item_id) AS distinct_order_items,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(quantity * unit_price), 2) AS total_item_amount
    FROM ecommerce_lakehouse.silver.order_items
""").show()


# ------------------------------------------------------------
# 9. Write invalid records to quarantine
# ------------------------------------------------------------

invalid_order_items_quarantine_df = (
    invalid_order_items_df
    .select(
        lit("order_items").alias("source_name"),
        lit("order_items").alias("table_name"),
        to_json(struct("*")).alias("record_data"),
        lit("Failed order_items DQ rules").alias("error_reason"),
        col("source_file_name"),
        current_timestamp().alias("error_timestamp"),
        lit("silver_order_items_dq").alias("pipeline_stage")
    )
)

invalid_order_items_quarantine_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("ecommerce_lakehouse.governance.quarantine_records")

print("Invalid order_items written to quarantine table.")


# ------------------------------------------------------------
# 10. Verify quarantine records for order_items
# ------------------------------------------------------------

display(
    spark.sql("""
        SELECT *
        FROM ecommerce_lakehouse.governance.quarantine_records
        WHERE table_name = 'order_items'
        ORDER BY error_timestamp DESC
    """)
)


# ------------------------------------------------------------
# 11. Final CDC Silver status check
# ------------------------------------------------------------

print("CDC Silver tables completed:")

spark.sql("""
    SELECT 'orders' AS table_name, COUNT(*) AS record_count
    FROM ecommerce_lakehouse.silver.orders

    UNION ALL

    SELECT 'payments' AS table_name, COUNT(*) AS record_count
    FROM ecommerce_lakehouse.silver.payments

    UNION ALL

    SELECT 'customers' AS table_name, COUNT(*) AS record_count
    FROM ecommerce_lakehouse.silver.customers

    UNION ALL

    SELECT 'order_items' AS table_name, COUNT(*) AS record_count
    FROM ecommerce_lakehouse.silver.order_items
""").show()

# COMMAND ----------

spark.table("ecommerce_lakehouse.governance.data_quality_rules").printSchema()

display(
    spark.sql("""
        SELECT *
        FROM ecommerce_lakehouse.governance.data_quality_rules
        LIMIT 5
    """)
)

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, to_json, struct,
    when, from_unixtime
)
from functools import reduce


# =======================================================
# 1. Add/refresh DQ rules for customers
# =======================================================

spark.sql("""
DELETE FROM ecommerce_lakehouse.governance.data_quality_rules
WHERE table_name = 'customers'
""")

spark.sql("""
INSERT INTO ecommerce_lakehouse.governance.data_quality_rules
VALUES
('customers', 'customer_id', 'not_null', '', 'customer_id cannot be null', 'critical', true, current_timestamp()),
('customers', 'first_name', 'not_null', '', 'first_name cannot be null', 'critical', true, current_timestamp()),
('customers', 'last_name', 'not_null', '', 'last_name cannot be null', 'critical', true, current_timestamp()),
('customers', 'email', 'not_null', '', 'email cannot be null', 'critical', true, current_timestamp()),
('customers', 'email', 'regex', '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$', 'invalid email format', 'warning', true, current_timestamp())
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
# 3. Read Bronze customers CDC table
# =======================================================

bronze_customers_df = spark.table("ecommerce_lakehouse.bronze.customers")


# =======================================================
# 4. Parse Debezium CDC payload.after
# =======================================================

silver_customers_input_df = (
    bronze_customers_df
    .select(
        col("payload.after.customer_id").cast("long").alias("customer_id"),
        col("payload.after.first_name").alias("first_name"),
        col("payload.after.last_name").alias("last_name"),
        col("payload.after.email").alias("email"),
        col("payload.after.phone").alias("phone"),
        col("payload.after.city").alias("city"),
        col("payload.after.state").alias("state"),
        col("payload.after.country").alias("country"),
        col("payload.after.created_at").alias("created_at_raw"),
        col("payload.after.updated_at").alias("updated_at_raw"),
        col("payload.op").alias("cdc_operation"),
        col("payload.ts_ms").alias("cdc_event_ts_ms"),
        col("source_file_name"),
        col("ingestion_timestamp"),
        col("source_system"),
        col("load_date")
    )
    .withColumn(
        "created_at",
        from_unixtime(col("created_at_raw") / 1000).cast("timestamp")
    )
    .withColumn(
        "updated_at",
        from_unixtime(col("updated_at_raw") / 1000).cast("timestamp")
    )
    .withColumn(
        "cdc_event_timestamp",
        from_unixtime(col("cdc_event_ts_ms") / 1000).cast("timestamp")
    )
    .withColumn(
        "is_deleted",
        when(col("cdc_operation") == "d", True).otherwise(False)
    )
    .withColumn("processed_timestamp", current_timestamp())
)


# =======================================================
# 5. Apply metadata-driven DQ rules
# =======================================================

valid_customers_df, invalid_customers_df = apply_dynamic_dq_rules(
    df=silver_customers_input_df,
    table_name="customers"
)

valid_count = valid_customers_df.count()
invalid_count = invalid_customers_df.count()

print(f"Valid customers: {valid_count}")
print(f"Invalid customers: {invalid_count}")


# =======================================================
# 6. Write valid customers to Silver
# =======================================================

(
    valid_customers_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_lakehouse.silver.customers")
)

print("Valid customer records written to ecommerce_lakehouse.silver.customers")


# =======================================================
# 7. Remove old customer quarantine records
# =======================================================

spark.sql("""
DELETE FROM ecommerce_lakehouse.governance.quarantine_records
WHERE table_name = 'customers'
  AND pipeline_stage = 'bronze_to_silver'
""")


# =======================================================
# 8. Write invalid customers to quarantine
# =======================================================

quarantine_customers_df = (
    invalid_customers_df
    .withColumn("source_name", lit("customers"))
    .withColumn("table_name", lit("customers"))
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
    quarantine_customers_df.write
    .mode("append")
    .format("delta")
    .saveAsTable("ecommerce_lakehouse.governance.quarantine_records")
)

print("Invalid customer records written to quarantine table")
print("Customers Silver processing completed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS customer_count
# MAGIC FROM ecommerce_lakehouse.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.governance.quarantine_records
# MAGIC WHERE table_name = 'customers';

# COMMAND ----------

order_items_df = spark.table("ecommerce_lakehouse.bronze.order_items")
order_items_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, to_json, struct,
    when, from_unixtime
)
from functools import reduce


# =======================================================
# 1. Add/refresh DQ rules for order_items
# =======================================================

spark.sql("""
DELETE FROM ecommerce_lakehouse.governance.data_quality_rules
WHERE table_name = 'order_items'
""")

spark.sql("""
INSERT INTO ecommerce_lakehouse.governance.data_quality_rules
VALUES
('order_items', 'order_item_id', 'not_null', '', 'order_item_id cannot be null', 'critical', true, current_timestamp()),
('order_items', 'order_id', 'not_null', '', 'order_id cannot be null', 'critical', true, current_timestamp()),
('order_items', 'product_id', 'not_null', '', 'product_id cannot be null', 'critical', true, current_timestamp()),
('order_items', 'quantity', 'greater_than', '0', 'quantity should be greater than 0', 'critical', true, current_timestamp()),
('order_items', 'unit_price', 'greater_than', '0', 'unit_price should be greater than 0', 'critical', true, current_timestamp()),
('order_items', 'item_total', 'greater_than_equal', '0', 'item_total should not be negative', 'critical', true, current_timestamp()),
('order_items', 'discount_amount', 'greater_than_equal', '0', 'discount_amount should not be negative', 'warning', true, current_timestamp())
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
# 3. Read Bronze order_items CDC table
# =======================================================

bronze_order_items_df = spark.table("ecommerce_lakehouse.bronze.order_items")


# =======================================================
# 4. Parse Debezium CDC payload.after
# =======================================================

silver_order_items_input_df = (
    bronze_order_items_df
    .select(
        col("payload.after.order_item_id").cast("long").alias("order_item_id"),
        col("payload.after.order_id").cast("long").alias("order_id"),
        col("payload.after.product_id").cast("long").alias("product_id"),
        col("payload.after.quantity").cast("long").alias("quantity"),
        col("payload.after.unit_price").cast("decimal(10,2)").alias("unit_price"),
        col("payload.after.item_total").cast("decimal(10,2)").alias("item_total"),
        col("payload.after.discount_amount").cast("decimal(10,2)").alias("discount_amount"),
        col("payload.after.created_at").alias("created_at_raw"),
        col("payload.op").alias("cdc_operation"),
        col("payload.ts_ms").alias("cdc_event_ts_ms"),
        col("source_file_name"),
        col("ingestion_timestamp"),
        col("source_system"),
        col("load_date")
    )
    .withColumn(
        "created_at",
        from_unixtime(col("created_at_raw") / 1000).cast("timestamp")
    )
    .withColumn(
        "cdc_event_timestamp",
        from_unixtime(col("cdc_event_ts_ms") / 1000).cast("timestamp")
    )
    .withColumn(
        "is_deleted",
        when(col("cdc_operation") == "d", True).otherwise(False)
    )
    .withColumn("processed_timestamp", current_timestamp())
)


# =======================================================
# 5. Apply metadata-driven DQ rules
# =======================================================

valid_order_items_df, invalid_order_items_df = apply_dynamic_dq_rules(
    df=silver_order_items_input_df,
    table_name="order_items"
)

valid_count = valid_order_items_df.count()
invalid_count = invalid_order_items_df.count()

print(f"Valid order_items: {valid_count}")
print(f"Invalid order_items: {invalid_count}")


# =======================================================
# 6. Write valid order_items to Silver
# =======================================================

(
    valid_order_items_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_lakehouse.silver.order_items")
)

print("Valid order_items records written to ecommerce_lakehouse.silver.order_items")


# =======================================================
# 7. Remove old order_items quarantine records
# =======================================================

spark.sql("""
DELETE FROM ecommerce_lakehouse.governance.quarantine_records
WHERE table_name = 'order_items'
  AND pipeline_stage = 'bronze_to_silver'
""")


# =======================================================
# 8. Write invalid order_items to quarantine
# =======================================================

quarantine_order_items_df = (
    invalid_order_items_df
    .withColumn("source_name", lit("order_items"))
    .withColumn("table_name", lit("order_items"))
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
    quarantine_order_items_df.write
    .mode("append")
    .format("delta")
    .saveAsTable("ecommerce_lakehouse.governance.quarantine_records")
)

print("Invalid order_items records written to quarantine table")
print("Order_items Silver processing completed successfully.")