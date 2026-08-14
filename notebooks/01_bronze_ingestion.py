# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC Reads the raw sales CSV as-is and lands it into a Delta table with zero
# MAGIC transformations. This preserves the original data exactly as received,
# MAGIC so the Bronze table always acts as the single source of truth and lets
# MAGIC any downstream layer be rebuilt from scratch if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read raw file

# COMMAND ----------

raw_df = spark.read.format("csv") \
         .option("header", "true") \
         .option("inferSchema", "true") \
         .load("/Volumes/workspace/default/my_volume/sales_dataset.csv")

raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row count & column count sanity check

# COMMAND ----------

print((raw_df.count(), len(raw_df.columns)))
spark.sql("DROP TABLE IF EXISTS workspace.default.silver_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema validation

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null value profiling per column
# MAGIC
# MAGIC Quick data-quality check before anything gets written — confirms which
# MAGIC columns actually need null handling in the Silver layer.

# COMMAND ----------

from pyspark.sql.functions import col, when, count

raw_df.select([
    count(when(col(c).isNull(), True)).alias(c)
    for c in raw_df.columns
]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write raw data to the Bronze Delta table
# MAGIC
# MAGIC Written as-is — no cleaning, no filtering. All cleaning happens in
# MAGIC `02_silver_transformation`.

# COMMAND ----------

raw_df.write.format("delta") \
      .mode("overwrite") \
      .saveAsTable("workspace.bronze.bronze_sales")

spark.read.table("workspace.bronze.bronze_sales").display()
