# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

dbutils.fs.unmount("/mnt/working_data")


# COMMAND ----------

storage_account_name = "aimasterdata"
container_name = "working-data"
sas_token = "sp=racwdli&st=2024-09-11T23:20:17Z&se=2024-09-12T07:20:17Z&spr=https&sv=2022-11-02&sr=c&sig=qLnENqPifx5oNpNWWPAUciKNMlo%2BzgIPPE%2F7RfgYM%2FQ%3D"

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = "/mnt/working_data",
    extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/working_data/levels.csv", "file:/tmp/levels.csv")
#dbutils.fs.cp("dbfs:/mnt/working_data/EVENT_LOG.zip", "file:/tmp/EVENT_LOG.zip")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/working_data/ARQLMED.zip/

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/working_data/working_arqlmed.parquet/", True)

# COMMAND ----------



# COMMAND ----------

# List files in the /tmp directory
files = dbutils.fs.ls("file:/tmp")

# Print file names and paths
for file in files:
    print(f"Name: {file.name}, Path: {file.path}, Size: {file.size}")


# COMMAND ----------

import zipfile
import shutil

# Unzip the file in the local file system
with zipfile.ZipFile("/tmp/ARQLMED.zip", "r") as zip_ref:
    zip_ref.extractall("/tmp/working_data/")

# with zipfile.ZipFile("/tmp/EVENT_LOG.zip", "r") as zip_ref:
#     zip_ref.extractall("/tmp/working_data/")

# Move the extracted files to DBFS
dbutils.fs.cp("file:/tmp/working_data/archives/export/ARQLMED.csv", "dbfs:/mnt/working_data/working_arqlmed.csv")
#dbutils.fs.cp("file:/tmp/working_data/MEDIDAS.csv", "dbfs:/mnt/working_data/working_medidas")
#dbutils.fs.cp("file:/tmp/working_data/EVENT_LOG/EVENT_LOG.csv", "dbfs:/mnt/working_data/working_event_log")


# COMMAND ----------

import zipfile
import shutil
with zipfile.ZipFile("/tmp/EVENT_LOG.zip", "r") as zip_ref:
    zip_ref.extractall("/tmp/working_data/")

# Step 2: Copy the extracted file to DBFS
# Assuming the extracted CSV file is named "EVENT_LOG.csv"
dbutils.fs.cp("file:/tmp/working_data/EVENT_LOG.csv", "dbfs:/mnt/working_data/working_event_log.csv")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/working_data/

# COMMAND ----------

# Read the Parquet files into DataFrames
df_arqlmed = spark.read.option("header", "true").option("delimiter", ";").csv("dbfs:/mnt/working_data/working_arqlmed.csv")

# Show a few rows to ensure the DataFrame is populated correctly
df_arqlmed.display(5)
df_arqlmed.printSchema()


# COMMAND ----------

# Read and convert files to Parquet format
# df_arqlmed = spark.read.option("header", "true").option("delimiter", ";").csv("dbfs:/mnt/working_data/working_arqlmed")
# df_arqlmed.write.mode("overwrite").parquet("working_arqlmed")

df_arqlmed.display(5)
df_arqlmed.printSchema()

# df_medidas = spark.read.option("header", "true").option("delimiter", ";").csv("dbfs:/mnt/working_data/working_medidas")
# df_medidas.write.parquet.saveAsTable("working_medidas")

# df_event_log = spark.read.option("header", "true").option("delimiter", ";").csv("dbfs:/mnt/working_data/working_event_log")
# df_medidas.write.parquet.saveAsTable("working_event_log")


# COMMAND ----------

# Read the Parquet files into DataFrames
df_levels = spark.read.option("header", "true").option("delimiter", ",").csv("dbfs:/mnt/working_data/levels.csv")

# Show a few rows to ensure the DataFrame is populated correctly
df_levels.display(5)
df_levels.printSchema()


# COMMAND ----------

df_levels = spark.read.option("header", "true").option("delimiter", ",").csv("dbfs:/mnt/working_data/levels.csv")
df_levels.write.mode("overwrite").parquet("working_levels")

# COMMAND ----------

df_levels = spark.read.parquet("/mnt/working_data/working_levels.parquet")

# COMMAND ----------

# Read the Parquet files into DataFrames
df_arqlmed = spark.read.parquet("/mnt/working_data/working_arqlmed.parquet")
df_medidas = spark.read.parquet("/mnt/working_data/working_medidas.parquet")
df_event_log = spark.read.parquet("/mnt/working_data/working_event_log.parquet")


# COMMAND ----------


# File location and type
file_location = "dbfs:/mnt/working_data/working_arqlmed.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "working_arqlmed"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `working_arqlmed`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "working_arqlmed"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------


# File location and type
file_location = "dbfs:/mnt/working_data/levels.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Create a view or table

temp_table_name = "working_levels"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `working_levels`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "working_levels"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------


# File location and type
file_location = "dbfs:/mnt/working_data/working_medidas"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Create a view or table

temp_table_name = "working_medidas"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "working_medidas"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/working_data/working_arqlmed.parquet/

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/medidas.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "medidas_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `medidas_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "medidas_csv"

df.write.format("parquet").saveAsTable(permanent_table_name)
