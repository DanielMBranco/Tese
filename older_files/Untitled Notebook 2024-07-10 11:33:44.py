# Databricks notebook source
# MAGIC %md
# MAGIC Load data into databricks
# MAGIC
# MAGIC We got a zip with huge files so we have to import the zip and work from there

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls 

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/working_data")

# COMMAND ----------


