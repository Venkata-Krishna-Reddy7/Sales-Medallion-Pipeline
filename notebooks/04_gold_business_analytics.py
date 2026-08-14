# Databricks notebook source
# MAGIC %md
# MAGIC # Business Analytics
# MAGIC
# MAGIC Answers a set of business questions directly off the Gold star schema.
# MAGIC Each query joins the fact table(s) to the relevant dimension(s), so
# MAGIC these can also serve as a template for a BI tool's semantic layer.

# COMMAND ----------

from pyspark.sql.functions import col, sum, date_format, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

fact_sales = spark.read.table("workspace.gold.fact_sales")
fact_transactions = spark.read.table("workspace.gold.fact_transactions")
dim_customers = spark.read.table("workspace.gold.dim_customers")
dim_products = spark.read.table("workspace.gold.dim_products")
dim_location = spark.read.table("workspace.gold.dim_location")
dim_date = spark.read.table("workspace.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which country generates the most revenue?

# COMMAND ----------

fact_sales.join(dim_location, on="customer_id", how="inner") \
    .groupBy("Country") \
    .agg(sum(col("price") * col("quantity")).alias("Total_Revenue")) \
    .orderBy(col("Total_Revenue").desc()) \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Who are the top 5 customers by total spending?

# COMMAND ----------

fact_sales.join(dim_customers, "customer_id") \
    .join(dim_location, "customer_id") \
    .groupBy("customer_id", "customer_name", "country", "age") \
    .agg(sum(col("price") * col("quantity")).alias("Total_Spending")) \
    .orderBy(col("Total_Spending").desc()) \
    .limit(5) \
    .select("customer_id", "customer_name", "Total_Spending", "country", "age") \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### How did sales trend month by month in 2024?

# COMMAND ----------

fact_sales.join(dim_date, "order_date") \
          .filter(col("year") == 2024) \
          .withColumn("month_name", date_format(col("order_date"), "MMM")) \
          .groupBy("month", "month_name") \
          .agg(sum(col("price") * col("quantity")).alias("monthly_revenue")) \
          .orderBy("month") \
          .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which product category drives the most revenue?

# COMMAND ----------

fact_sales.join(dim_products, "product_id") \
          .groupBy("product_category") \
          .agg(sum(col("price") * col("quantity")).alias("prod_category_revenue")) \
          .orderBy(col("prod_category_revenue").desc()) \
          .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full monthly sales trend, all years

# COMMAND ----------

fact_sales.join(dim_date, "order_date") \
          .withColumn("month_name", date_format(col("order_date"), "MMM")) \
          .groupBy("year", "month_name") \
          .agg(sum(col("price") * col("quantity")).alias("monthly_revenue")) \
          .orderBy("year", "month_name") \
          .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Do weekdays or weekends bring in more sales?

# COMMAND ----------

fact_sales.join(dim_date, "order_date") \
          .withColumn("is_weekend", when(col("is_weekend") == True, "Weekend").otherwise("weekdays")) \
          .groupBy("is_weekend") \
          .agg(sum(col("price") * col("quantity")).alias("total_revenue")) \
          .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the most sold product in each country?
# MAGIC
# MAGIC Uses a window function ranked by total units sold, partitioned per
# MAGIC country, and keeps only the top-ranked product per partition.

# COMMAND ----------

window_spec = Window.partitionBy("Country").orderBy(col("total_sold").desc())

df = fact_sales.join(dim_products, "product_id") \
               .join(dim_location, "customer_id") \
               .filter(col("product_id") != "Unknown") \
               .groupBy("Country", "product_id", "product_name") \
               .agg(sum(col("quantity")).alias("total_sold")) \
               .withColumn("rank", row_number().over(window_spec)) \
               .filter(col("rank") == 1) \
               .select("Country", "product_id", "product_name", "total_sold")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which quarter had the highest revenue share in 2024?

# COMMAND ----------

fact_sales.join(dim_date, "order_date") \
          .filter(col("year") == 2024) \
          .groupBy("quarter") \
          .agg(sum(col("price") * col("quantity")).alias("quarter_revenue")) \
          .orderBy("quarter") \
          .display()
