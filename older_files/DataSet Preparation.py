# Databricks notebook source
# MAGIC %md
# MAGIC Filtrar apenas os alarmes que nos interessam, os que tem High/Alto no evdesc
# MAGIC
# MAGIC Agrupar alarmes em espaços de minutos (15 em 15 minutos) e adicionar uma coluna com o nr de alarmes nesses 15 minutos
# MAGIC
# MAGIC Nova variável: Está abaixo do limite (0), passou o limite há menos de 1 hora (1), passou o limite entre 1 hora e 1 dia (2) passou o limite há mais de 1 dia (3)
# MAGIC
# MAGIC Agrupar o dataset principal por objeto(id)
# MAGIC
# MAGIC Em principio, com alarmes agrupados a cada 15 min será possível juntar alarmes ao dataset sem grandes problemas

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_timestamp, max, min, to_date, from_unixtime, date_trunc, date_format, when, lit, least, greatest, avg, lag, array, array_sort, abs, expr, minute, hour, round, row_number, first, sum, unix_timestamp, regexp_extract, sequence, explode, udf, count, dayofmonth, coalesce
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# COMMAND ----------

df_arqlmed = spark.read.table("hive_metastore.default.working_arqlmed")

# COMMAND ----------

columns_to_convert = ["STATE"]

# Convert selected string columns to integer
for col_name in columns_to_convert:
    df_arqlmed = df_arqlmed.withColumn(col_name, col(col_name).cast("long"))

# COMMAND ----------

df_arqlmed.printSchema()

# COMMAND ----------

df_arqlmed = df_arqlmed.drop(*["ID_ARQLMED","ID_TIME","NTIME","OPR_TM_LOAD","OPR_TIPO_OPERACAO","OPR_STM_FONTE","CIM_UID", "ID_ATRIBUTO", "TYP", "SUBTYP", "STATUS", "NSTATUS2", "OPR_ID_LOAD", "POLO"])

# COMMAND ----------

df_arqlmed = df_arqlmed.withColumn("DATA_", to_timestamp(col("DATA_"), "dd-MM-yyyy HH:mm:ss"))

# COMMAND ----------

df_arqlmed.show(5)

# COMMAND ----------

df_medidas = spark.read.table("hive_metastore.default.working_medidas")

# COMMAND ----------


limites_df = df_medidas.select(["ID_MEDIDAS","ALR_LL_LIM","ALR_L_LIM","ALR_H_LIM","ALR_HH_LIM"])

# COMMAND ----------

columns_to_convert = ["ALR_LL_LIM", "ALR_L_LIM", "ALR_H_LIM", "ALR_HH_LIM"]

# Convert selected string columns to integer
for col_name in columns_to_convert:
    limites_df = limites_df.withColumn(col_name, col(col_name).cast("long"))

# COMMAND ----------

final_df = df_arqlmed.join(limites_df, "ID_MEDIDAS", "inner")

# COMMAND ----------

#final_df.display()

# COMMAND ----------

final_df = final_df.filter(
    (col("ID").substr(2, 1).isin("P", "S")) &  # Check position 2
    (~col("ID").substr(7, 1).isin("-", "9", "4"))  # Check position 7
)

# COMMAND ----------

# Generate df_arqlmed_U- using filter and like operations
final_df = final_df.filter((col("ID").rlike("U--$")) | (col("ID").rlike("0II--$")))


# COMMAND ----------

#final_df.display()

# COMMAND ----------

final_df = final_df.filter(final_df["STATE"] >= 0)

# COMMAND ----------

specific_id = "JPSMAD5508-0TU--"

# Filter the DataFrame and get the row
row = final_df.filter(final_df["ID"] == specific_id).select("ALR_H_LIM").first()
row["ALR_H_LIM"]

# COMMAND ----------

# data inicio dados = 2023-11-07 data fim = 2023-12-03

# Specify the equipment ID and metric you want to plot
equipment_id = "QSVLN-5505-0TU--"
metric_column = "STATE"
start_date = "2023-11-07"  # Start date for filtering
end_date = "2023-12-03"    # End date for filtering (for a week-long span)

# Filter the DataFrame for the specified equipment ID and date range
filtered_df = final_df.filter((final_df["ID"] == equipment_id) &
                                 (final_df["DATA_"] >= start_date) &
                                 (final_df["DATA_"] <= end_date))

# Sort the DataFrame by the timestamp
sorted_df = filtered_df.orderBy("DATA_")

# Convert PySpark DataFrame to Pandas DataFrame
pd_df = sorted_df.select("DATA_", metric_column).toPandas()

# Extract the limit value (assuming it is constant for this equipment ID)
row = final_df.filter(final_df["ID"] == equipment_id).select("ALR_H_LIM").first()
limit_value = row["ALR_H_LIM"]

# Plot the data using Plotly
fig = px.line(pd_df, x="DATA_", y=metric_column, title=f"Metric {metric_column} for Equipment ID {equipment_id}")

# Add a horizontal line representing the limit
fig.add_shape(
    type="line",
    x0=pd_df["DATA_"].min(),
    y0=limit_value,
    x1=pd_df["DATA_"].max(),
    y1=limit_value,
    line=dict(color="Red", width=2, dash="dash"),
    name="High Limit"
)

# Update layout to include the limit in the legend
fig.update_layout(
    shapes=[dict(
        type="line",
        x0=pd_df["DATA_"].min(),
        x1=pd_df["DATA_"].max(),
        y0=limit_value,
        y1=limit_value,
        line=dict(color="Red", width=2, dash="dash")
    )],
    annotations=[dict(
        x=pd_df["DATA_"].mean(),
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

#Aqui quero acertar os limites, assegurar que LL<L<H<HH e remover as linhas que tem os limites a 0s


final_df = final_df.withColumn("sorted_values", array_sort(array(col("ALR_LL_LIM"), 
                                                                     col("ALR_L_LIM"), 
                                                                     col("ALR_H_LIM"), 
                                                                     col("ALR_HH_LIM"))))

# Assign the sorted values back to the columns
final_df = final_df.withColumn("ALR_LL_LIM", col("sorted_values")[0]) \
                         .withColumn("ALR_L_LIM", col("sorted_values")[1]) \
                         .withColumn("ALR_H_LIM", col("sorted_values")[2]) \
                         .withColumn("ALR_HH_LIM", col("sorted_values")[3]) \
                         .drop("sorted_values")



# Show the adjusted DataFrame

#show_medidas = final_df.filter(final_df["ID_MEDIDAS"] == 74126)

#show_medidas.display()


# COMMAND ----------

# Now I will create a column with the moving average of the state value

windowSpec = Window.partitionBy("ID").orderBy("DATA_").rowsBetween(-3, 0)  # Considering a window of 4 rows = 1 HOUR

# Add a column for the moving average
final_df = final_df.withColumn("STATE_MAVERAGE_SHORT", avg(col("STATE")).over(windowSpec))


# Creating a second moving avg for State, a longer one
windowSpec = Window.partitionBy("ID").orderBy("DATA_").rowsBetween(-95, 0)  # Considering a window of 96 rows = 1 DAY

# Add a column for the moving average
final_df = final_df.withColumn("STATE_MAVERAGE_LONG", avg(col("STATE")).over(windowSpec))


final_df.display()

# COMMAND ----------

show_medidas = final_df.filter(final_df["ID"] == "APCRT-5503-0TU--")

show_medidas.display()

# COMMAND ----------

final_df = final_df.withColumnRenamed('DATA_', 'DATE') \
    .withColumnRenamed('ALR_LL_LIM', 'LL_LIM') \
    .withColumnRenamed('ALR_L_LIM', 'L_LIM') \
    .withColumnRenamed('ALR_H_LIM', 'H_LIM') \
    .withColumnRenamed('ALR_HH_LIM', 'HH_LIM')

# COMMAND ----------

df_event = spark.read.table("hive_metastore.default.working_event_log")

# COMMAND ----------

df_event = df_event.withColumn("EVDATE", to_timestamp(col("EVDATE"), "dd-MM-yyyy HH:mm:ss"))

# COMMAND ----------


df_event = df_event.drop(*["ID_EVDATE","ID_AREAGEOGRAFICA","ID_TIPOINSTALACAO","ID_SIGLA","ID_NIVELTENSAO","ID_OBJETO","ID_OPERATOR","NTIME","NTTAGTIME","STTAGMS","INS_COUNT","LOG_TYPE","STYPE","STTAGATR","STTAGCAUSE","STIMEMS","OPR_ID_LOAD","OPR_ID_LOAD","OPR_TM_LOAD","OPR_TIPO_OPERACAO","OPR_STM_FONTE","STATE_NUMBER","EVDESC_ML", "ID_TIPOEVENTO1", "ID_TIPOEVENTO2", "ID_TIPOEVENTO3", "ID_TIPOEVENTO4", "ID_TIPOEVENTO5","ID_EVENT_LOG", "TAGHL0", "TAGHL1", "TAGOPR", "LOGTYPE", "STATENUMBER", "ID_OBJECTO", "ID_PAINEL", "ID_ATRIBUTO", "POLO"])

# COMMAND ----------


df_event = df_event.withColumnRenamed('TAG1', 'ID') \
    .withColumnRenamed('EVDATE', 'DATE')

# COMMAND ----------

reordered_columns = ["ID"] + [col for col in df_event.columns if col != "ID"]
df_event = df_event.select(reordered_columns)

# COMMAND ----------

df_event = df_event.filter(
    (col("ID").substr(2, 1).isin("P", "S")) &  # Check position 2
    (~col("ID").substr(7, 1).isin("-", "9", "4"))  # Check position 7
)

# COMMAND ----------

df_event = df_event.filter((col("ID").rlike("U--$")) | (col("ID").rlike("0II--$")))

# COMMAND ----------

#Observei que os alarmes que me são úteis tem "Alto"na descrição, então vou apenas considerar esses para o meu problema

# Filter rows with the specific word in the description column
df_event = df_event.filter(~col("EVDESC").contains("M. Alto"))
df_event = df_event.filter(col("EVDESC").contains("Alto"))

# Show the resulting DataFrame
df_event.display()

# COMMAND ----------

import datetime
#First - Group by ID
#Create the timestamp interval
#Shove events into these intervals and create the count column that has the number of events in a certain interval
# Grouping by ID

# Define a function to generate timestamps at 15-minute intervals
def generate_timestamps():
    start_time = datetime.datetime(2023, 12, 1, 0, 0)
    interval = datetime.timedelta(minutes=15)
    current_time = start_time
    while current_time < datetime.datetime(2024, 1, 1, 0, 0):
        yield current_time
        current_time += interval

# Test the timestamp generator
timestamps = []
for timestamp in generate_timestamps():
    timestamps.append(timestamp)
    print(timestamp)

print("Total timestamps:", len(timestamps))



# COMMAND ----------

# Define the UDF to round timestamps to the nearest 15-minute interval
def round_to_nearest_15_min(timestamp):
    if timestamp is None:
        return None
    timestamp = pd.Timestamp(timestamp)
    # Round to the nearest 15-minute interval
    nearest_15_min = timestamp.round('15min')
    return nearest_15_min

round_to_nearest_15_min_udf = udf(round_to_nearest_15_min, TimestampType())

# COMMAND ----------


final_df = final_df.withColumn("DATE", col("DATE").cast(TimestampType()))

# Apply the UDF to round timestamps to the nearest 15-minute interval
final_df = final_df.withColumn("DATE", round_to_nearest_15_min_udf(col("DATE")))


# COMMAND ----------

df_event = df_event.withColumn("DATE", col("DATE").cast(TimestampType()))

# Apply the UDF to round timestamps to the nearest 15-minute interval
df_event = df_event.withColumn("DATE", round_to_nearest_15_min_udf(col("DATE")))

# COMMAND ----------

#df_event.display()

# COMMAND ----------

#Agrupar eventos por timestamp, como arredondei todos os timestamps para os 15 minutos, vao existir eventos com o mesmo id e o mesmo timestamp, o que vou fazer é criar a coluna event_count e agrupar os eventos por id e timestamp, desta forma teremos apenas uma linha por id e timestamp mas teremos na mesma a informação de quantos eventos ocorreram naquele espaço de tempo.

# Group by ID and the rounded DATE, and aggregate the data
df_event = df_event.groupBy("ID", "DATE") \
                   .agg(count("*").alias("EVENT_COUNT"),
                        first("EVDESC").alias("EVDESC"))

# Show the result
#df_event.display()

# COMMAND ----------


df_event = df_event.drop(*["EVDESC"])

# COMMAND ----------

# filtered = df_event.filter(col("ID") == "APCACI5503-0II--")
# filtered.display()

# COMMAND ----------

# filtered_df_by_hour_day = df_event.filter((hour(col("DATE")) == 17) & (dayofmonth(col("DATE")) == 13) & (col("ID") == "ASAVCA2223-0II--"))

# # Show the filtered result
# filtered_df_by_hour_day.display()

# COMMAND ----------

# filtered_df_by_hour_day = df_event.filter((hour(col("DATE")) == 16) & (dayofmonth(col("DATE")) == 13) & (col("ID") == "ASAVCA2223-0II--"))

# # Show the filtered result
# filtered_df_by_hour_day.display()

# COMMAND ----------

#df_event.display()

# COMMAND ----------

# Perform a left join
final_df = final_df.join(df_event, on=["ID", "DATE"], how="left")

# Fill null values in the event_count column with 0
final_df = final_df.withColumn("EVENT_COUNT", coalesce(col("EVENT_COUNT"), lit(0)))

# COMMAND ----------

# final_df.display()

# COMMAND ----------

# value_counts_combination = final_df.groupBy("event_count").count()
# value_counts_combination.display()

# COMMAND ----------

# final_df.count()

# COMMAND ----------

# Nova variável: Está abaixo do limite (0), passou o limite há menos de 1 hora (1), passou o limite entre 1 hora e 1 dia (2) passou o limite há mais de 1 dia (3)

# Define the window specification
window_spec = Window.partitionBy("ID").orderBy("DATE")

# Calculate whether STATE is above H_LIM
df = final_df.withColumn("above_H_LIM", when(col("STATE") > col("H_LIM"), 1).otherwise(0))

# Identify transitions where STATE crosses H_LIM
df = df.withColumn("prev_above_H_LIM", lag("above_H_LIM").over(window_spec))
df = df.withColumn("group", 
                   when((col("above_H_LIM") == 1) & ((col("prev_above_H_LIM") == 0) | col("prev_above_H_LIM").isNull()), 
                        row_number().over(window_spec))
                   .otherwise(0))

# Forward fill the group identifier to create segments
window_spec_ffill = Window.partitionBy("ID").orderBy("DATE").rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("group", sum("group").over(window_spec_ffill))

# Calculate the time difference in seconds for when STATE is above H_LIM
df = df.withColumn("time_diff", when(col("above_H_LIM") == 1, col("DATE").cast("long") - lag("DATE").over(window_spec).cast("long")).otherwise(0))

# Fill nulls in time_diff with 0
df = df.fillna({'time_diff': 0})

# Calculate cumulative sum of time_diff to get the total duration above H_LIM within each segment
window_spec_segment = Window.partitionBy("ID", "group").orderBy("DATE")
df = df.withColumn("cumulative_time_above_H_LIM", sum("time_diff").over(window_spec_segment))

# Define the new feature based on cumulative_time_above_H_LIM
df = df.withColumn("TIME_OVER_LIMIT", 
                   when(col("STATE") <= col("H_LIM"), 0)
                   .when(col("cumulative_time_above_H_LIM") < 3600, 1) # less than 1 hour
                   .when((col("cumulative_time_above_H_LIM") >= 3600) & (col("cumulative_time_above_H_LIM") < 86400), 2) # between 1 hour and 1 day
                   .otherwise(3)) # more than 1 day

# COMMAND ----------

#df.display()

# COMMAND ----------

# value_counts_combination = df.groupBy("TIME_OVER_LIMIT").count()
# value_counts_combination.display()

# COMMAND ----------

# filtered_ids = df.filter(col("new_feature") == 3).select("ID").distinct()
# filtered_ids.display()

# COMMAND ----------

# show_medidas = df.filter(df["ID"] == "MPAREA3BR1-0TU--")

# show_medidas.display()

# COMMAND ----------

df = df.drop(*["prev_above_H_LIM", "time_diff", "cumulative_time_above_H_LIM","group", "above_H_LIM"])

# COMMAND ----------

#df.show(5)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("df_model")

# COMMAND ----------

# show_medidas = df_arqlmed.filter(df["ID"] == "ASILH-5BR2-0TU--")

# show_medidas.display()

# COMMAND ----------

# show_medidas = df_medidas.filter(df_medidas["ID_MEDIDAS"] == "6309")

# show_medidas.display()

# COMMAND ----------

# # Assuming df_event is your DataFrame
# # Grouping by ID
# grouped_df = df_event.groupby("ID")

# min_date = final_df.selectExpr("min(Date)").collect()[0][0]
# max_date = final_df.selectExpr("max(Date)").collect()[0][0]

# # Define a function to generate timestamps at 15-minute intervals
# def generate_timestamps():
#     start_time = pd.Timestamp(min_date)
#     end_time = pd.Timestamp(max_date)
#     return pd.date_range(start=start_time, end=end_time, freq="15T")

# # Create a DataFrame with timestamps at 15-minute intervals
# timestamps = generate_timestamps()
# timestamps_df = pd.DataFrame({"timestamp": timestamps})

# # Extract unique IDs from the DataFrame
# ids = df_event.select("ID").distinct().rdd.flatMap(lambda x: x).collect()

# # Create a DataFrame with all combinations of IDs and timestamps
# all_combinations = pd.MultiIndex.from_product([ids, timestamps], names=["ID", "timestamp"])
# timestamps_with_ids_df = pd.DataFrame(index=all_combinations).reset_index()

# # Show the first few rows to verify
# timestamps_with_ids_df.display()

# COMMAND ----------

# min_date = final_df.selectExpr("min(DATE)").collect()[0][0]
# max_date = final_df.selectExpr("max(DATE)").collect()[0][0]

# start_time = pd.Timestamp(min_date)
# end_time = pd.Timestamp(max_date)

# start_time

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


