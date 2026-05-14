# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES IN ecommerce_lakehouse.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ecommerce_lakehouse.gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_orders;

# COMMAND ----------

# MAGIC %sql OPTIMIZE ecommerce_lakehouse.gold.fact_order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_payments;

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY ecommerce_lakehouse.gold.fact_orders;

# COMMAND ----------

# MAGIC
# MAGIC %sql OPTIMIZE ecommerce_lakehouse.gold.fact_orders ZORDER BY (customer_id, order_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_payments
# MAGIC ZORDER BY (order_id, payment_date);
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_order_items
# MAGIC ZORDER BY (product_id, order_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_orders
# MAGIC ZORDER BY (customer_id, order_date);
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_payments
# MAGIC ZORDER BY (order_id, payment_date);
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_order_items
# MAGIC ZORDER BY (product_id, order_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ecommerce_lakehouse.gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM ecommerce_lakehouse.gold.fact_orders RETAIN 168 HOURS;
# MAGIC VACUUM ecommerce_lakehouse.gold.fact_order_items RETAIN 168 HOURS;
# MAGIC VACUUM ecommerce_lakehouse.gold.fact_payments RETAIN 168 HOURS;
# MAGIC VACUUM ecommerce_lakehouse.gold.dim_product RETAIN 168 HOURS;
# MAGIC VACUUM ecommerce_lakehouse.gold.dim_customer RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ecommerce_lakehouse.gold.fact_orders;

# COMMAND ----------

