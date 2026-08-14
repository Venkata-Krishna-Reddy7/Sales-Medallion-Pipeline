# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Star Schema
# MAGIC
# MAGIC Models the cleaned Silver data into a star schema: 4 dimension tables
# MAGIC (Customers, Products, Location, Date) and 2 fact tables (Sales,
# MAGIC Transactions). Everything is written as Delta tables in the `gold`
# MAGIC catalog so the analytics notebook (`04_gold_business_analytics`) can
# MAGIC query them directly.

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_silver = spark.read.table("workspace.silver.silver_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customers

# COMMAND ----------

dim_customers = df_silver.select(
    "customer_id",
    "customer_name",
    "age",
    "gender",
    "email",
    "Mobile_Number"
).dropDuplicates(["customer_id"]) \
 .withColumn("dim_customer_id", monotonically_increasing_id() + 1)

dim_customers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_products

# COMMAND ----------

dim_products = df_silver.select(
    "product_id",
    "product_name",
    "product_category"
).dropDuplicates(["product_id"]) \
 .withColumn("dim_product_id", monotonically_increasing_id() + 1)

dim_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_location

# COMMAND ----------

dim_location = df_silver.select(
    "customer_id",
    "House_No",
    "Street_Name",
    "City",
    "State",
    "Country"
).dropDuplicates(["customer_id"]) \
 .withColumn("dim_location_id", monotonically_increasing_id() + 1)

dim_location.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.dim_location")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date

# COMMAND ----------

dim_date = df_silver.select(
    "order_date",
    "year",
    "month",
    "day",
    "day_of_week",
    "quarter",
    "is_weekend"
).dropDuplicates(["order_date"]) \
 .withColumn("dim_date_id", monotonically_increasing_id() + 1)

dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_sales

# COMMAND ----------

fact_sales = df_silver.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "price",
    "order_status"
).withColumn("fact_sales_id", monotonically_increasing_id() + 1)

fact_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_transactions

# COMMAND ----------

fact_transactions = df_silver.select(
    "transaction_id",
    "order_id",
    "customer_id",
    "transaction_date",
    "transaction_amount",
    "payment_method",
    "transaction_status",
    "transaction_type"
).withColumn("fact_transaction_id", monotonically_increasing_id() + 1)

fact_transactions.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.fact_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Gold tables

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.gold").display()

# COMMAND ----------

spark.read.table("workspace.gold.dim_customers").display()
spark.read.table("workspace.gold.dim_products").display()
spark.read.table("workspace.gold.dim_location").display()
spark.read.table("workspace.gold.dim_date").display()
spark.read.table("workspace.gold.fact_sales").display()
spark.read.table("workspace.gold.fact_transactions").display()
