# Databricks notebook source
from pyspark.sql.functions import col, count

# COMMAND ----------

event_log = spark.read.table("hive_metastore.default.event_log_csv")
display(event_log)

# COMMAND ----------

id_counts = event_log.groupBy("ID_OBJECTO").agg(count("*").alias("count"))

# Filter ID_OBJECTO with count greater than 1 (duplicate IDs)
duplicate_ids = id_counts.filter(col("count") > 1)

# Show the common IDs
common_ids.show()
