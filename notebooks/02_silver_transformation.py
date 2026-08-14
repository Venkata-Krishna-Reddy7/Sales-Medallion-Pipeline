# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning & Transformation
# MAGIC
# MAGIC Takes the raw Bronze table and produces an analytics-ready, validated
# MAGIC dataset. This is where most of the actual data engineering work happens:
# MAGIC type casting, deduplication, null handling, status validation, address
# MAGIC parsing, country-aware phone formatting, email validation, and date
# MAGIC feature engineering.

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, count, when, avg, coalesce, current_date,
    date_format, min, max, sum, expr, year, month, upper,
    dayofmonth, quarter, split, trim, regexp_replace, concat
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast columns to correct data types
# MAGIC
# MAGIC Uses `try_cast` / `try_to_date` instead of hard casts so a single bad
# MAGIC value (e.g. a malformed date) becomes `null` rather than failing the
# MAGIC whole pipeline.

# COMMAND ----------

df_silver = spark.read.table("workspace.bronze.bronze_sales") \
            .withColumn("price", expr('try_cast(price as float)')) \
            .withColumn("transaction_amount", expr('try_cast(transaction_amount as float)')) \
            .withColumn("quantity", expr('try_cast(quantity as int)')) \
            .withColumn("order_date", expr("try_to_date(order_date, 'yyyy-MM-dd')")) \
            .withColumn("transaction_date", expr("try_to_date(transaction_date, 'yyyy-MM-dd')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicate records

# COMMAND ----------

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fill missing values with meaningful defaults

# COMMAND ----------

df_silver = df_silver.fillna({
    "product_id": "Unknown",
    "product_name": "Unknown",
    "product_category": "Unknown",
    "product_id_order": "Unknown",
    "order_status": "Unknown",
    "payment_method": "Unknown",
    "transaction_status": "Unknown",
    "product_id_txn": "Unknown"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop rows where critical columns are null
# MAGIC
# MAGIC Rows with no date, quantity, price, or transaction amount can't be
# MAGIC trusted for any downstream metric, so they're dropped rather than
# MAGIC defaulted.

# COMMAND ----------

df_silver = df_silver.dropna(subset=["order_date", "quantity", "price", "transaction_date", "transaction_amount"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate order & transaction statuses
# MAGIC
# MAGIC Anything outside the accepted set of values gets normalized to
# MAGIC `"Unknown"` rather than silently kept, so bad upstream data can't leak
# MAGIC into Gold-layer aggregations.

# COMMAND ----------

valid_statuses = ["Delivered", "Cancelled", "Processing", "Shipped", "Returned"]
txn_statuses = ["Success", "Refunded"]

df_silver = df_silver.withColumn("order_status", when(col("order_status").isin(valid_statuses), col("order_status"))
                                .otherwise("Unknown")) \
                     .withColumn("transaction_status", when(col("transaction_status").isin(txn_statuses), col("transaction_status"))
                                .otherwise("Unknown"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter invalid rows & flag refunds

# COMMAND ----------

df_silver = df_silver.filter((col("price") > 0) & (col("quantity") > 0))
df_silver = df_silver.withColumn("transaction_type", when(col("transaction_amount") < 0, "Refund")
                                .otherwise("Sale"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the address column into structured fields

# COMMAND ----------

df_silver = df_silver.withColumn("address_parts", split(col("address"), ",")) \
                     .withColumn("House_No", trim(col("address_parts").getItem(0))) \
                     .withColumn("Street_Name", trim(col("address_parts").getItem(1))) \
                     .withColumn("city", trim(col("address_parts").getItem(2))) \
                     .withColumn("State", trim(col("address_parts").getItem(3))) \
                     .withColumn("Country", trim(col("address_parts").getItem(4))) \
                     .drop("address_parts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Format phone numbers per country
# MAGIC
# MAGIC Strips non-digit characters, then applies a country-specific dialing
# MAGIC code and expected digit length for India, USA/Canada, UK, Germany,
# MAGIC France, Australia, and Brazil.

# COMMAND ----------

df_silver = df_silver.withColumn("Mobile_Number", regexp_replace(col("phone"), r"[^\d]", "")) \
    .withColumn("Mobile_Number",
         when(col("Country") == "India", concat(lit("(+91) "), expr('right(Mobile_Number, 10)')))
        .when(col("Country").isin("USA", "Canada"), concat(lit("(+1) "), expr('right(Mobile_Number, 10)')))
        .when(col("Country") == "UK", concat(lit("(+44) "), expr('right(Mobile_Number, 10)')))
        .when(col("Country") == "Germany", concat(lit("(+49) "), expr('right(Mobile_Number, 11)')))
        .when(col("Country") == "France", concat(lit("(+33) "), expr('right(Mobile_Number, 9)')))
        .when(col("Country") == "Australia", concat(lit("(+61) "), expr('right(Mobile_Number, 9)')))
        .when(col("Country") == "Brazil", concat(lit("(+55) "), expr('right(Mobile_Number, 11)')))
        .otherwise(col("Mobile_Number"))
    ) \
    .drop("phone")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize customer names

# COMMAND ----------

df_silver = df_silver.withColumn("Customer_Name", upper(col("customer_name")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate email addresses via regex

# COMMAND ----------

df_silver = df_silver.filter(col("email").rlike(r"^[A-Za-z0-9]+@[A-Za-z]+\.[A-Za-z]{2,}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Engineer date features
# MAGIC
# MAGIC Adds year, month, day, day-of-week, quarter, and a weekend flag off of
# MAGIC `order_date` — these get reused directly by `dim_date` in the Gold layer.

# COMMAND ----------

df_silver = df_silver.withColumn("year", year(col("order_date"))) \
                     .withColumn("month", month(col("order_date"))) \
                     .withColumn("day", dayofmonth(col("order_date"))) \
                     .withColumn("day_of_week", date_format(col("order_date"), "EEEE")) \
                     .withColumn("quarter", quarter(col("order_date"))) \
                     .withColumn("is_weekend", when(date_format(col("order_date"), "EEEE").isin("Saturday", "Sunday"), True).otherwise(False)) \
                     .withColumn("price", col("price").cast("int")) \
                     .withColumn("transaction_amount", col("transaction_amount").cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to the Silver Delta table

# COMMAND ----------

df_silver.write.format("delta") \
               .mode("overwrite") \
               .saveAsTable("workspace.silver.silver_sales")

df_silver.display()
