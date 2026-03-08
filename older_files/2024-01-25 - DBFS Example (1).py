# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# Concatenate


folder_path = "/FileStore/tables/arqlmed"

dbutils.fs.ls(folder_path)



# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/arqlmed/arqlmed_000.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


for x in range(1,38):
    if x < 10:
        x = "0" + str(x)

    file_location = "/FileStore/tables/arqlmed/arqlmed_0" + str(x) + ".csv"

    dfpart = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location)    
    
    df = df.union(dfpart)


display(df)

# COMMAND ----------

df.count()

# COMMAND ----------



# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/arqlmed/arqlmed_010.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dfsample = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(dfsample)

# COMMAND ----------

dfsample.count()

# COMMAND ----------

# Create a view or table

temp_table_name = "arqlmed_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select count(*) from `arqlmed_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "arqlmed_csv"

df.write.format("parquet").saveAsTable(permanent_table_name)
