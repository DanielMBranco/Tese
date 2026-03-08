# Databricks notebook source

from pyspark.sql.functions import col, trim, to_timestamp, max, min, to_date, from_unixtime, date_trunc, date_format, when, lit, least, greatest, avg, lag, array, array_sort, abs, expr, minute, hour, round, row_number, first, sum, unix_timestamp, regexp_extract, sequence, explode, udf, count, dayofmonth, coalesce, substring, collect_list, size, count_distinct
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from functools import reduce

# COMMAND ----------

df = spark.table("df_model")

# COMMAND ----------

result = df.agg(
    min("DATE").alias("earliest_timestamp"),
    max("DATE").alias("latest_timestamp")
)

result.show()

# COMMAND ----------

value_counts = df.groupBy("TIME_OVER_LIMIT").count()

sorted_value_counts = value_counts.orderBy("count", ascending=False)

sorted_value_counts.show()

# COMMAND ----------

# Group by the ID column and calculate the sum of the state column
id_state_sum = df.groupBy("ID").agg(sum("STATE").alias("state_sum"))

valid_ids = id_state_sum.filter("state_sum != 0").select("id")

df = df.join(valid_ids, on="id", how="inner")

# Show the result
df.show()


# COMMAND ----------

# Group by the ID column and calculate the sum of the state column
id_hlim_sum = df.groupBy("ID").agg(sum("H_LIM").alias("hlim_sum"))

valid_ids = id_hlim_sum.filter("hlim_sum != 0").select("id")

df = df.join(valid_ids, on="id", how="inner")

# Show the result
df.show()


# COMMAND ----------

id_state_sum = df.groupBy("ID").agg(sum("H_LIM").alias("state_sum"))

valid_ids = id_state_sum.filter("state_sum == 0").select("id")

valid_ids.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

# Define a window specification to look at the previous row
window_spec = Window.partitionBy("ID").orderBy("DATE")


df_with_lag = df.withColumn("prev_state", lag("STATE").over(window_spec)) \
                .withColumn("prev_time_over_limit", lag("TIME_OVER_LIMIT").over(window_spec))


result = df_with_lag.filter((col("STATE") == 0) & (col("prev_time_over_limit") != 0))

result.show()


# COMMAND ----------

filtered_df = df.filter(df.ID == 'LSMARV1123-0II--')

# Show the result
filtered_df.display()


# COMMAND ----------

# data inicio dados = 2023-11-07 data fim = 2023-12-03

# Specify the equipment ID and metric you want to plot
equipment_id = "FSALGW2223-0II--"
metric_column = "STATE"
start_date = "2023-07-07"  # Start date for filtering
end_date = "2024-07-03"    # End date for filtering (for a week-long span)

# Filter the DataFrame for the specified equipment ID and date range
filtered_df = df.filter((df["ID"] == equipment_id) &
                                 (df["DATE"] >= start_date) &
                                 (df["DATE"] <= end_date))

# Sort the DataFrame by the timestamp
sorted_df = filtered_df.orderBy("DATE")

# Convert PySpark DataFrame to Pandas DataFrame
pd_df = sorted_df.select("DATE", metric_column).toPandas()

# Extract the limit value (assuming it is constant for this equipment ID)
row = df.filter(df["ID"] == equipment_id).select("H_LIM").first()
limit_value = row["H_LIM"]

# Plot the data using Plotly
fig = px.line(pd_df, x="DATE", y=metric_column, title=f"Metric {metric_column} for Equipment ID {equipment_id}")

# Add a horizontal line representing the limit
fig.add_shape(
    type="line",
    x0=pd_df["DATE"].min(),
    y0=limit_value,
    x1=pd_df["DATE"].max(),
    y1=limit_value,
    line=dict(color="Red", width=2, dash="dash"),
    name="High Limit"
)

# Update layout to include the limit in the legend
fig.update_layout(
    shapes=[dict(
        type="line",
        x0=pd_df["DATE"].min(),
        x1=pd_df["DATE"].max(),
        y0=limit_value,
        y1=limit_value,
        line=dict(color="Red", width=2, dash="dash")
    )],
    annotations=[dict(
        x=pd_df["DATE"].mean(),
        y=limit_value,
        xref="x",
        yref="y",
        text=f"High Limit: {limit_value}",
        showarrow=False,
        font=dict(color="Red")
    )]
)

# Show the plot
fig.show()

# COMMAND ----------

# Extract object identifier (positions 12 and 13)
df = df.withColumn('Object_ID', substring(col('ID'), 2, 5))

df.display()

# COMMAND ----------

filtered_df = df.filter(col('Object_ID') == 'PPRL-')
display(filtered_df)

# COMMAND ----------

distinct_id_count = filtered_df.select('ID').distinct()
distinct_id_count.display()

# COMMAND ----------

# APCACI5504-0II--
# APCACI5504-0TU--

# COMMAND ----------

df_U = df.filter(col('ID') == 'APCACI5503-0TU--')
display(df_U)

# COMMAND ----------

df_I = df.filter(col('ID') == 'APCACI5503-0II--')
display(df_I)

# COMMAND ----------

df_U.count()

# COMMAND ----------

df_I.count()

# COMMAND ----------

# data inicio dados = 2023-11-07 data fim = 2023-12-03

# Specify the equipment ID and metric you want to plot
equipment_id = "ASESR-5501-0II--"
metric_column = "STATE"
start_date = "2023-07-07"  # Start date for filtering
end_date = "2024-07-03"    # End date for filtering (for a week-long span)

# Filter the DataFrame for the specified equipment ID and date range
filtered_df = df.filter((df["ID"] == equipment_id) &
                                 (df["DATE"] >= start_date) &
                                 (df["DATE"] <= end_date))

# Sort the DataFrame by the timestamp
sorted_df = filtered_df.orderBy("DATE")

# Convert PySpark DataFrame to Pandas DataFrame
pd_df = sorted_df.select("DATE", metric_column).toPandas()

# Extract the limit value (assuming it is constant for this equipment ID)
row = df.filter(df["ID"] == equipment_id).select("H_LIM").first()
limit_value = row["H_LIM"]

# Plot the data using Plotly
fig = px.line(pd_df, x="DATE", y=metric_column, title=f"Metric {metric_column} for Equipment ID {equipment_id}")

# Add a horizontal line representing the limit
fig.add_shape(
    type="line",
    x0=pd_df["DATE"].min(),
    y0=limit_value,
    x1=pd_df["DATE"].max(),
    y1=limit_value,
    line=dict(color="Red", width=2, dash="dash"),
    name="High Limit"
)

# Update layout to include the limit in the legend
fig.update_layout(
    shapes=[dict(
        type="line",
        x0=pd_df["DATE"].min(),
        x1=pd_df["DATE"].max(),
        y0=limit_value,
        y1=limit_value,
        line=dict(color="Red", width=2, dash="dash")
    )],
    annotations=[dict(
        x=pd_df["DATE"].mean(),
        y=limit_value,
        xref="x",
        yref="y",
        text=f"High Limit: {limit_value}",
        showarrow=False,
        font=dict(color="Red")
    )]
)

# Show the plot
fig.show()

# COMMAND ----------

# data inicio dados = 2023-11-07 data fim = 2023-12-03

# Specify the equipment ID and metric you want to plot
equipment_id = "APCACI5503-0II--"
metric_column = "STATE"
start_date = "2023-07-07"  # Start date for filtering
end_date = "2024-07-03"    # End date for filtering (for a week-long span)

# Filter the DataFrame for the specified equipment ID and date range
filtered_df = df.filter((df["ID"] == equipment_id) &
                                 (df["DATE"] >= start_date) &
                                 (df["DATE"] <= end_date))

# Sort the DataFrame by the timestamp
sorted_df = filtered_df.orderBy("DATE")

# Convert PySpark DataFrame to Pandas DataFrame
pd_df = sorted_df.select("DATE", metric_column).toPandas()

# Extract the limit value (assuming it is constant for this equipment ID)
row = df.filter(df["ID"] == equipment_id).select("H_LIM").first()
limit_value = row["H_LIM"]

# Plot the data using Plotly
fig = px.line(pd_df, x="DATE", y=metric_column, title=f"Metric {metric_column} for Equipment ID {equipment_id}")

# Add a horizontal line representing the limit
fig.add_shape(
    type="line",
    x0=pd_df["DATE"].min(),
    y0=limit_value,
    x1=pd_df["DATE"].max(),
    y1=limit_value,
    line=dict(color="Red", width=2, dash="dash"),
    name="High Limit"
)

# Update layout to include the limit in the legend
fig.update_layout(
    shapes=[dict(
        type="line",
        x0=pd_df["DATE"].min(),
        x1=pd_df["DATE"].max(),
        y0=limit_value,
        y1=limit_value,
        line=dict(color="Red", width=2, dash="dash")
    )],
    annotations=[dict(
        x=pd_df["DATE"].mean(),
        y=limit_value,
        xref="x",
        yref="y",
        text=f"High Limit: {limit_value}",
        showarrow=False,
        font=dict(color="Red")
    )]
)

# Show the plot
fig.show()

# COMMAND ----------

from pyspark.sql.functions import col, substring
filtered_df = df.filter(col("ID").rlike('^.{11}0'))
# Show the result
filtered_df.display()

# COMMAND ----------

# Extract the first 12 characters of the ID
df = df.withColumn("ID_prefix", substring(col("ID"), 1, 11))

#df.display()

# COMMAND ----------

# Group by the first 12 characters and filter groups with more than one distinct ID
filtered_df = df.groupBy("ID_prefix").agg(count_distinct("ID").alias("distinct_count")) \
    .filter(col("distinct_count") > 1)

# Join back with the original DataFrame to filter the relevant rows
result_df = df.join(filtered_df, "ID_prefix")

# Show the result
#result_df.display()

# COMMAND ----------

#result_df.display()

# COMMAND ----------

# result_df = result_df.withColumn("ID_prefix", substring(col("ID"), 1, 12)) \
#        .withColumn("suffix", substring(col("ID"), -3, 3))

# # Pivot the DataFrame to create separate columns for Tension and Intensity
# pivot_df = result_df.groupBy("ID_prefix", "DATE").pivot("suffix").agg(F.first("STATE"))

# # Rename the columns to Tension and Intensity
# pivot_df = pivot_df.withColumnRenamed("U--", "Tension").withColumnRenamed("I--", "Intensity")

# # Show the result
# pivot_df.show()

# COMMAND ----------

# p_df = result_df.withColumn("ID_prefix", substring(col("ID"), 1, 12)) \
#            .withColumn("suffix", substring(col("ID"), -3, 3))

# # Convert DATE column to timestamp with consistent format
# p_df = p_df.withColumn("DATE", F.date_format(F.col("DATE"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

# # List of columns to pivot
# columns_to_pivot = ["STATE", "H_LIM", "STATE_MAVERAGE_SHORT", "STATE_MAVERAGE_LONG", "EVENT_COUNT", "TIME_OVER_LIMIT"]

# # Pivot and rename columns for each column in columns_to_pivot
# pivoted_dfs = []
# for column in columns_to_pivot:
#     pivoted_df = p_df.groupBy("ID_prefix", "DATE").pivot("suffix").agg(first(column))
#     pivoted_df = pivoted_df.withColumnRenamed("U--", f"{column}_T").withColumnRenamed("I--", f"{column}_I")
#     pivoted_df = pivoted_df.withColumnRenamed("STATE_T", "Tension").withColumnRenamed("STATE_I", "Intensity")
#     pivoted_dfs.append(pivoted_df)



# # Combine all pivoted dataframes into one
# from functools import reduce

# p_df = reduce(lambda df1, df2: df1.join(df2, on=["ID_prefix", "DATE"], how="inner"), pivoted_dfs)

# COMMAND ----------

p_df = result_df.withColumn("ID_prefix", substring(col("ID"), 1, 12)) \
           .withColumn("suffix", substring(col("ID"), -3, 3))

# List of columns to pivot
columns_to_pivot = ["STATE", "H_LIM", "STATE_MAVERAGE_SHORT", "STATE_MAVERAGE_LONG", "EVENT_COUNT", "TIME_OVER_LIMIT"]


pivoted_dfs = []
for column in columns_to_pivot:
    pivoted_df = p_df.groupBy("ID_prefix", "DATE").pivot("suffix").agg(F.first(column))
    
    # Check the column names in the pivoted DataFrame
    print(pivoted_df.columns)
    
    # Rename columns based on expected pivot values
    pivoted_df = pivoted_df.withColumnRenamed("U--", f"{column}_T").withColumnRenamed("I--", f"{column}_I")
    
    # Ensure that renaming reflects actual column names after pivot
    if 'STATE_T' in pivoted_df.columns and 'STATE_I' in pivoted_df.columns:
        pivoted_df = pivoted_df.withColumnRenamed("STATE_T", "Tension").withColumnRenamed("STATE_I", "Intensity")
    
    pivoted_dfs.append(pivoted_df)

# COMMAND ----------

result_df.display()	

# COMMAND ----------

display(pivoted_dfs[0])

# COMMAND ----------

# for pivoted_df in pivoted_dfs:
#     pivoted_df.display()
#     print(pivoted_df.columns)

# COMMAND ----------

p_df_final = reduce(lambda df1, df2: df1.join(df2, on=["ID_prefix", "DATE"], how="inner"), pivoted_dfs)

# Check the result of the join operation
p_df_final.display()

# COMMAND ----------



# COMMAND ----------

# Add new columns based on the timestamp column 'DATE'
p_df_final = p_df_final.withColumn("day_of_week", ((F.dayofweek(F.col("DATE")) + 5) % 7 + 1)) \
       .withColumn("day_of_month", F.dayofmonth(F.col("DATE"))) \
       .withColumn("day_of_year", F.dayofyear(F.col("DATE"))) \
       .withColumn("hour_of_day", F.hour(F.col("DATE"))) \
       .withColumn("am_pm", F.date_format(F.col("DATE"), "a"))

# COMMAND ----------

display(p_df_final)

# COMMAND ----------

p_df_final.write.format("delta").mode("append").saveAsTable("pivot_table")

# COMMAND ----------

#p_df.display()

# COMMAND ----------

p_df_final.count()

# COMMAND ----------

p_df_final.select("ID_prefix").distinct().count()

# COMMAND ----------

result_df_check = p_df_final.filter(col('ID_prefix') == 'APACL-5502-0')
##display(result_df_check)

# COMMAND ----------

result_df_check.count()

# COMMAND ----------

df_U = df.filter(col('ID') == 'APACL-5502-0TU--')
#display(df_U)

# COMMAND ----------

df_U.count()

# COMMAND ----------

df_U = df.filter(col('ID') == 'APACL-5502-0II--')
#display(df_U)
