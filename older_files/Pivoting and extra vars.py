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

# Extract object identifier (positions 12 and 13)
df = df.withColumn('Object_ID', substring(col('ID'), 2, 5))

df.display()

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
result_df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

pivot_df = spark.table("pivot_table")
pivot_df.display()

# COMMAND ----------

pivot_df.count()


# COMMAND ----------

pivot_df.select("ID_prefix").distinct().count()

# COMMAND ----------

result_df_check = pivot_df.filter(col('ID_prefix') == 'APACL-5502-0')

# COMMAND ----------

result_df_check.count()

# COMMAND ----------

df_U = df.filter(col('ID') == 'APACL-5502-0TU--')

# COMMAND ----------

#display(df_U)
df_U.count()

# COMMAND ----------

pivot_df.display()

# COMMAND ----------

df_U = df.filter(col('ID') == 'APACL-5502-0II--')
display(df_U)

# COMMAND ----------

# common_columns = ["ID_prefix", "DATE"]

# # Perform an anti-join to find rows in df1 that are not in df2
# extra_rows_df = df.join(pivot_df, on=common_columns, how='left_anti')


# # Filter the rows where one of the columns is null
# extra_rows_df = extra_rows_df.filter(
#     (col(f'{common_columns[0]}_df1').isNull() | col(f'{common_columns[0]}_df2').isNull()) |
#     (col(f'{common_columns[1]}_df1').isNull() | col(f'{common_columns[1]}_df2').isNull())
# )

# # Show the extra rows
# extra_rows_df.display()

# COMMAND ----------

result_df_check = pivot_df.filter(col('ID_prefix') == 'APACL-5502-0')
result_df_check.display()

# COMMAND ----------

df_U = df.filter(col('ID') == 'APACL-5502-0TU--')
df_U.display()

# COMMAND ----------

df_U = df_U.withColumn("DATE", col("DATE").cast("timestamp"))
result_df_check = result_df_check.withColumn("DATE", col("DATE").cast("timestamp"))

joined_df = df_U.join(result_df_check, on=["ID_prefix", "DATE"], how='outer')

# Filter to find rows where the DATE is not common (i.e., where columns from one dataframe are null)
diff_df = joined_df.filter(
    col(df_U.columns[0]).isNull() | col(result_df_check.columns[0]).isNull()
).select("ID_prefix", "DATE")

# COMMAND ----------

diff_df.display()

# COMMAND ----------

# Get the minimum and maximum timestamps in df_U
min_timestamp = df_U.agg({"DATE": "min"}).collect()[0][0]
max_timestamp = df_U.agg({"DATE": "max"}).collect()[0][0]

# Generate a DataFrame with the full sequence of timestamps at 15-minute intervals
full_timestamps = spark.createDataFrame(
    [(min_timestamp, max_timestamp)],
    ["min_timestamp", "max_timestamp"]
).select(explode(sequence(col("min_timestamp"), col("max_timestamp"), expr("INTERVAL 15 MINUTES"))).alias("DATE"))

# Perform a left anti-join to find missing timestamps
missing_timestamps = full_timestamps.join(df_U, on="DATE", how="left_anti")

# Show the missing timestamps
missing_timestamps.display()

# COMMAND ----------

# Specify the date you want to filter (e.g., '2023-07-04')
specific_date = "2024-05-06"

# Filter for all records from that specific day
filtered_df = df_U.filter(date_format(col("DATE"), "yyyy-MM-dd") == specific_date)

# Show the result
filtered_df.display()

# COMMAND ----------

# Specify the date you want to filter (e.g., '2023-07-04')
specific_date = "2024-05-06"

# Filter for all records from that specific day
filtered_df = result_df_check.filter(date_format(col("DATE"), "yyyy-MM-dd") == specific_date)

# Show the result
filtered_df.display()

# COMMAND ----------

df_U.display()

# COMMAND ----------

nullcheck = df.filter((col('ID') == 'ASESR-5501-0TU--') & (date_format(col("DATE"), "yyyy-MM-dd") == "2024-03-30"))
nullcheck.display()

# COMMAND ----------

result_df_check.display()

# COMMAND ----------

null_counts = [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in pivot_df.columns]

# Apply the aggregation
df_null_counts = pivot_df.agg(*null_counts)

# Show the result
df_null_counts.display()

# COMMAND ----------

pivot_df.count()

# COMMAND ----------

condition = " OR ".join([f"`{c}` IS NULL" for c in pivot_df.columns])

# Filter the DataFrame to get rows with null values
df_with_nulls = pivot_df.filter(condition)

# Show the rows with null values
df_with_nulls.display()

# COMMAND ----------

# data inicio dados = 2023-11-07 data fim = 2023-12-03

# Specify the equipment ID and metric you want to plot
equipment_id = "ASESR-5501-0TU--"
metric_column = "STATE"
start_date = "2023-01-01"  # Start date for filtering
end_date = "2024-12-31"    # End date for filtering (for a week-long span)

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
equipment_id = "ASESR-5501-0II--"
metric_column = "STATE"
start_date = "2023-01-01"  # Start date for filtering
end_date = "2024-12-31"    # End date for filtering (for a week-long span)

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

start_date = "2024-05-13"
end_date = "2024-05-14"

dfcheck = df.filter((col('ID') == 'GSCAEI5515-0II--') &
                                 (df["DATE"] >= start_date) &
                                 (df["DATE"] <= end_date))
dfcheck.display()

# COMMAND ----------

start_date = "2024-03-25"
end_date = "2024-03-26"

dfcheck = df.filter((col('ID') == 'ASESR-5501-0TU--') &
                                 (df["DATE"] >= start_date) &
                                 (df["DATE"] <= end_date))
dfcheck.display()

# COMMAND ----------

pivot_df.display()

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

pivot_df = spark.table("pivot_table")

# COMMAND ----------

pivot_pd = pivot_df.toPandas()

# Define a function to sort by 'DATE' and interpolate the 'Intensity'
def interpolate_group(group):
    # Sort by DATE within each group
    group = group.sort_values('DATE')
    
        # Interpolate and replace values directly in the 'Intensity' column using linear interpolation
    group['Intensity'] = group['Intensity'].interpolate(method='linear').ffill().bfill().round(0)
    
    # Forward fill other columns
    group['H_LIM_I'] = group['H_LIM_I'].ffill().bfill().round(0)
    group['EVENT_COUNT_I'] = group['EVENT_COUNT_I'].ffill().bfill().round(0)
    group['TIME_OVER_LIMIT_I'] = group['TIME_OVER_LIMIT_I'].ffill().bfill().round(0)

       # Interpolate and replace values directly in the 'Intensity' column using linear interpolation
    group['Tension'] = group['Tension'].interpolate(method='linear').ffill().bfill().round(0)
    
    # Forward fill other columns
    group['H_LIM_T'] = group['H_LIM_T'].ffill().bfill().round(0)
    group['EVENT_COUNT_T'] = group['EVENT_COUNT_T'].ffill().bfill().round(0)
    group['TIME_OVER_LIMIT_T'] = group['TIME_OVER_LIMIT_T'].ffill().bfill().round(0)
    
    return group

# Apply the function to each group of 'ID_prefix'
pivot_pd = pivot_pd.groupby('ID_prefix').apply(interpolate_group)

# Display the resulting DataFrame
print(pivot_pd)

# COMMAND ----------

print(pivot_pd.dtypes)

# COMMAND ----------

pivot_df_interpolated = spark.createDataFrame(pivot_pd)


# COMMAND ----------

windowSpec = Window.partitionBy("ID_prefix").orderBy("DATE").rowsBetween(-7, 0)  # Considering a window of 8 rows = 2 HOURS

# Add a column for the moving average
pivot_df_interpolated = pivot_df_interpolated.withColumn("STATE_MAVERAGE_SHORT_I", round(avg(col("Intensity")).over(windowSpec),0))


# Creating a second moving avg for State, a longer one
windowSpec = Window.partitionBy("ID_prefix").orderBy("DATE").rowsBetween(-95, 0)  # Considering a window of 8 rows = 1 DAY

# Add a column for the moving average
pivot_df_interpolated = pivot_df_interpolated.withColumn("STATE_MAVERAGE_LONG_I", round(avg(col("Intensity")).over(windowSpec),0))


windowSpec = Window.partitionBy("ID_prefix").orderBy("DATE").rowsBetween(-7, 0)  # Considering a window of 8 rows = 2 HOURS

# Add a column for the moving average
pivot_df_interpolated = pivot_df_interpolated.withColumn("STATE_MAVERAGE_SHORT_T", round(avg(col("Tension")).over(windowSpec),0))


# Creating a second moving avg for State, a longer one
windowSpec = Window.partitionBy("ID_prefix").orderBy("DATE").rowsBetween(-95, 0)  # Considering a window of 8 rows = 1 DAY

# Add a column for the moving average
pivot_df_interpolated = pivot_df_interpolated.withColumn("STATE_MAVERAGE_LONG_T", round(avg(col("Tension")).over(windowSpec),0))

# COMMAND ----------

start_date = "2024-03-25"
end_date = "2024-03-27"

dfcheck = pivot_df_interpolated.filter((col('ID_prefix') == 'NSAMI-5500-0'))
dfcheck.display()

# COMMAND ----------

null_counts = [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in pivot_df_interpolated.columns]

# Apply the aggregation
df_null_counts = pivot_df_interpolated.agg(*null_counts)

# Show the result
df_null_counts.display()

# COMMAND ----------

condition = " OR ".join([f"`{c}` IS NULL" for c in pivot_df_interpolated.columns])

# Filter the DataFrame to get rows with null values
df_with_nulls = pivot_df_interpolated.filter(condition)

# Show the rows with null values
df_with_nulls.display()

# COMMAND ----------

rename_columns = {
    "ID_prefix": "ID",
    "Tension": "TENSION",
    "Intensity": "INTENSITY",
    "STATE_MAVERAGE_SHORT_I": "MAVERAGE_2H_I",
    "STATE_MAVERAGE_SHORT_T": "MAVERAGE_2H_T",
    "STATE_MAVERAGE_LONG_I": "MAVERAGE_1D_I",
    "STATE_MAVERAGE_LONG_T": "MAVERAGE_1D_T",
    "day_of_week": "DAY_OF_WEEK",
    "day_of_month": "DAY_OF_MONTH",
    "day_of_year": "DAY_OF_YEAR",
    "hour_of_day": "HOUR_OF_DAY",
    "am_pm": "AM_PM"
}

# Apply renaming for each column
for old_name, new_name in rename_columns.items():
    pivot_df_interpolated = pivot_df_interpolated.withColumnRenamed(old_name, new_name)

pivot_df_interpolated.display()

# COMMAND ----------

pivot_df_interpolated.write.format("delta").mode("append").saveAsTable("model_table")
