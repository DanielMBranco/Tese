# Databricks notebook source

from pyspark.sql.functions import col, trim, to_timestamp, max, min, to_date, from_unixtime, date_trunc, date_format, when, lit, least, greatest, avg, lag, array, array_sort, abs, expr, minute, hour, round, row_number, first, sum, unix_timestamp, regexp_extract, sequence, explode, udf, count, dayofmonth, coalesce, substring, collect_list, size, count_distinct, to_date
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# COMMAND ----------

df_model = spark.table("model_table")
df_levels = spark.table("working_levels")
medidas = spark.read.table("hive_metastore.default.medidas_csv")

# COMMAND ----------

df_model.display()

# COMMAND ----------

df_levels.display()

# COMMAND ----------

df_check = df_levels.filter(col('EXTERNALID').like('%1315S5021000%'))
df_check.display()

# COMMAND ----------

df_check = df_check.filter(col('DESC0').like('%501%'))
df_check.display()

# COMMAND ----------

df_check = df_levels.filter(col('TAG').like('%ASESR-5501-0%'))
df_check.display()

# COMMAND ----------

df_check = df_model.filter(col('ID') == 'FSALGW5506-0')
df_check.display()

# COMMAND ----------

# Get all columns of the dataframe
columns = df_levels.columns

# Apply trim to each column
for column in columns:
    df_levels = df_levels.withColumn(column, trim(col(column)))

# Display the updated DataFrame
df_levels.display()

# COMMAND ----------

df_check = df_levels.filter(col('TAG') == 'ASESR-5501-0I')
df_check.display()

# COMMAND ----------

common_rows = df_levels.join(medidas, df_levels['UID_'] == medidas['EQUIPMENTUID'], 'inner')

# Show the common rows
common_rows.display()

# COMMAND ----------

df_check = common_rows.filter(col('ID_MEDIDAS') == '86036')
df_check.display()

# COMMAND ----------

df_check = medidas.filter(col('TAG') == 'ASESR-5501-0II--')
df_check.display()

# COMMAND ----------

df_main_with_key = df_model.withColumn("TAG_KEY", substring("ID", 1, 12))
df_levels_with_key = df_levels.withColumn("TAG_KEY", substring("TAG", 1, 12))

# Perform the join to bring DESC0 into df_main
df_joined = df_main_with_key.join(
    df_levels_with_key.select("TAG_KEY", "DESC0", "TYPE"), 
    on="TAG_KEY", 
    how="left"
)

# Display the result
display(df_joined)
