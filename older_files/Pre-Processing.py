# Databricks notebook source
# MAGIC %md
# MAGIC Processar os datasets para todos ficarem com os dados nos tipos certos e criar tambem novas variaveis

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_timestamp, max, min, to_date, from_unixtime, date_trunc, date_format, when, lit, least, greatest, avg, lag, array, array_sort, abs, expr, minute, hour, round, row_number, first, sum, unix_timestamp
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from pyspark.sql.window import Window
from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md
# MAGIC ### Arqlmed

# COMMAND ----------

df_arqlmed = spark.read.table("hive_metastore.default.arqlmed_csv")
display(df_arqlmed)

# COMMAND ----------

unique_ids_df = (
    df_arqlmed
        .filter(col("ID").startswith("ASSNGD"))
        .select("ID")
        .distinct()
        .orderBy("ID")   # optional, just to keep it tidy
)

display(unique_ids_df)

# COMMAND ----------

columns_to_convert = ["ID_ARQLMED", "ID_MEDIDAS", "STATE"]

# Convert selected string columns to integer
for col_name in columns_to_convert:
    df_arqlmed = df_arqlmed.withColumn(col_name, col(col_name).cast("long"))

# COMMAND ----------

df_arqlmed.printSchema()

# COMMAND ----------

    df_arqlmed_errado = df_arqlmed.filter(~trim(col("POLO")).isin(['Sul', 'Norte']))
    display(df_arqlmed_errado)

# COMMAND ----------

    df_arqlmed_correto = df_arqlmed.filter(trim(col("POLO")).isin(['Sul', 'Norte']))
    display(df_arqlmed_correto)

# COMMAND ----------

df_arqlmed_correto.count()

# COMMAND ----------

df_arqlmed_errado.count()

# COMMAND ----------

df_correction = df_arqlmed_errado.drop("POLO")
df_correction = df_correction.withColumnRenamed("OPR_ID_LOAD", "POLO")

display(df_correction)

# COMMAND ----------

df_arqlmed_correto = df_arqlmed_correto.drop("OPR_ID_LOAD")

# COMMAND ----------

df_correction.display()

# COMMAND ----------

df_arqlmed_correto.display()

# COMMAND ----------

merge_arqlmed = df_correction.union(df_arqlmed_correto)

# COMMAND ----------

merge_arqlmed = merge_arqlmed.drop(*["ID_TIME","NTIME","OPR_TM_LOAD","OPR_TIPO_OPERACAO","OPR_STM_FONTE","CIM_UID", "ID_ATRIBUTO", "TYP", "SUBTYP", "STATUS", "NSTATUS2", "OPR_ID_LOAD"])

# COMMAND ----------

merge_arqlmed.count()

# COMMAND ----------



# COMMAND ----------

merge_arqlmed = merge_arqlmed.withColumn("DATA_", to_timestamp(col("DATA_"), "dd/MM/yyyy HH:mm:ss"))


# COMMAND ----------

display(merge_arqlmed)

# COMMAND ----------

merge_arqlmed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Medidas
# MAGIC

# COMMAND ----------

medidas = spark.read.table("hive_metastore.default.medidas_csv")
display(medidas)

# COMMAND ----------


medidas_lims = medidas.select(["ID_MEDIDAS","ALR_LL_LIM","ALR_L_LIM","ALR_H_LIM","ALR_HH_LIM"])

# COMMAND ----------

medidas_lims.display()

# COMMAND ----------


columns_to_convert = ["ALR_LL_LIM", "ALR_L_LIM", "ID_MEDIDAS", "ALR_H_LIM", "ALR_HH_LIM", "ALR_H_LIM"]

# Convert selected string columns to integer
for col_name in columns_to_convert:
    medidas_lims = medidas_lims.withColumn(col_name, col(col_name).cast("long"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


joined_df = merge_arqlmed.join(medidas_lims, "ID_MEDIDAS", "inner")


# COMMAND ----------

display(joined_df)

# COMMAND ----------

# Generate df_arqlmed_U- using filter and like operations
joined_df_U = joined_df.filter(col("ID").rlike("U--$"))
display(joined_df_U)


# COMMAND ----------


joined_df_U = joined_df_U.filter(
    (col("ID").substr(2, 1).isin("P", "S")) &  # Check position 2
    (~col("ID").substr(7, 1).isin("-", "9", "4"))  # Check position 7
)

display(joined_df_U)


# COMMAND ----------


joined_df_U = joined_df_U.filter(joined_df_U["STATE"] >= 0)

# Show the filtered DataFrame
joined_df_U.display()

# COMMAND ----------

count_df = joined_df_U.groupBy("ID_MEDIDAS").count()
count_df.display()

# COMMAND ----------

show_medidas = joined_df_U.filter(joined_df_U["ID_MEDIDAS"] == 8340)

show_medidas.display()

# COMMAND ----------

wtf = medidas.filter(medidas["ID_MEDIDAS"] == 8340)

wtf.display()

# COMMAND ----------

equipment_id = 42050

# Filter the DataFrame for the specific equipment ID
filtered_df = joined_df_U.filter(col("ID") == equipment_id)

# Group by timestamp and count occurrences
timestamp_counts = filtered_df.groupBy("DATA_").count()

# Filter timestamps with counts greater than 1
repeated_timestamps = timestamp_counts.filter(col("count") > 1)

# Show repeated timestamps
repeated_timestamps.show()

# Count the number of repeated timestamps
num_repeated_timestamps = repeated_timestamps.count()
print("Number of repeated timestamps:", num_repeated_timestamps)

# COMMAND ----------

# Specify the equipment ID and metric you want to plot
equipment_id = 42050
metric_column = "STATE"
date_to_plot = "2023-11-19"

# Filter the DataFrame for the specified equipment ID
filtered_df = joined_df_U.filter(joined_df_U["ID_MEDIDAS"] == equipment_id)

sorted_df = filtered_df.orderBy("DATA_")


# Convert PySpark DataFrame to Pandas DataFrame
pd_df = sorted_df.select("DATA_", metric_column).toPandas()


px.line(pd_df, x = "DATA_", y = metric_column)


# COMMAND ----------

equipment_id = 253061
metric_column = "STATE"
date_to_plot = "2023-11-19"

# Filter the DataFrame for the specified equipment ID
filtered_df = joined_df_U.filter(joined_df_U["ID_MEDIDAS"] == equipment_id)

sorted_df = filtered_df.orderBy(col("DATA_"))


# Convert PySpark DataFrame to Pandas DataFrame
pd_df = sorted_df.select("DATA_", metric_column).toPandas()

pd_df

# COMMAND ----------

exceeded_df = joined_df_U.filter(col("STATE") > col("ALR_HH_LIM"))

# Group by equipment ID and count occurrences
count_df = exceeded_df.groupBy("ID_MEDIDAS").count()

# Order by count in descending order
ordered_df = count_df.orderBy(col("count").desc())

# Take the top 5 rows
top5_equipment_ids = ordered_df.limit(5)

# Show the top 5 equipment IDs
top5_equipment_ids.show()

# COMMAND ----------

#Aqui quero acertar os limites, assegurar que LL<L<H<HH e remover as linhas que tem os limites a 0s


# adjusted_df = joined_df_U.withColumn("ALR_LL_LIM", 
#                              least(col("ALR_LL_LIM"), 
#                                    col("ALR_L_LIM") - 1, 
#                                    col("ALR_H_LIM") - 2, 
#                                    col("ALR_HH_LIM") - 3)) \
#                 .withColumn("ALR_L_LIM", 
#                             least(col("ALR_L_LIM"), 
#                                   col("ALR_H_LIM") - 1, 
#                                   col("ALR_HH_LIM") - 2, 
#                                   col("ALR_LL_LIM") + 1)) \
#                 .withColumn("ALR_H_LIM", 
#                             greatest(col("ALR_H_LIM"), 
#                                      col("ALR_LL_LIM") + 1, 
#                                      col("ALR_L_LIM") + 1, 
#                                      col("ALR_HH_LIM") - 1)) \
#                 .withColumn("ALR_HH_LIM", 
#                             greatest(col("ALR_HH_LIM"), 
#                                      col("ALR_LL_LIM") + 2, 
#                                      col("ALR_L_LIM") + 2, 
#                                      col("ALR_H_LIM") + 1))


adjusted_df = joined_df_U.withColumn("sorted_values", array_sort(array(col("ALR_LL_LIM"), 
                                                                     col("ALR_L_LIM"), 
                                                                     col("ALR_H_LIM"), 
                                                                     col("ALR_HH_LIM"))))

# Assign the sorted values back to the columns
adjusted_df = adjusted_df.withColumn("ALR_LL_LIM", col("sorted_values")[0]) \
                         .withColumn("ALR_L_LIM", col("sorted_values")[1]) \
                         .withColumn("ALR_H_LIM", col("sorted_values")[2]) \
                         .withColumn("ALR_HH_LIM", col("sorted_values")[3]) \
                         .drop("sorted_values")



# Show the adjusted DataFrame

show_medidas = adjusted_df.filter(adjusted_df["ID_MEDIDAS"] == 74126)

show_medidas.display()


# COMMAND ----------

show_medidas = joined_df_U.filter(joined_df_U["ID_MEDIDAS"] == 8340)

show_medidas.display()

# COMMAND ----------

exceeded_df = adjusted_df.filter(col("STATE") > col("ALR_HH_LIM"))

# Group by equipment ID and count occurrences
count_df = exceeded_df.groupBy("ID_MEDIDAS").count()

# Order by count in descending order
ordered_df = count_df.orderBy(col("count").desc())

# Take the top 5 rows
top5_equipment_ids = ordered_df.limit(5)

# Show the top 5 equipment IDs
top5_equipment_ids.show()

# COMMAND ----------

exceeded_df = adjusted_df.filter((col("STATE") < col("ALR_HH_LIM")) & ((col("STATE") >= col("ALR_H_LIM"))))

# Group by equipment ID and count occurrences
count_df = exceeded_df.groupBy("ID_MEDIDAS").count()

# Order by count in descending order
ordered_df = count_df.orderBy(col("count").desc())

# Take the top 5 rows
top5_equipment_ids = ordered_df.limit(5)

# Show the top 5 equipment IDs
top5_equipment_ids.show()

# COMMAND ----------

show_medidas = adjusted_df.filter(adjusted_df["ID_MEDIDAS"] == 8340)

show_medidas.display()

# COMMAND ----------

exceeded_df = adjusted_df.filter(col("STATE") > col("ALR_HH_LIM"))

# Group by equipment ID and count occurrences
count_df = exceeded_df.groupBy("ID_MEDIDAS").count()

# Order by count in descending order
ordered_df = count_df.orderBy(col("count").desc())

# Take the top 5 rows
top5_equipment_ids = ordered_df.limit(5)

# Show the top 5 equipment IDs
top5_equipment_ids.show()

# COMMAND ----------

# Now I will create a column with the moving average of the state value

windowSpec = Window.partitionBy("ID_MEDIDAS").orderBy("DATA_").rowsBetween(-7, 0)  # Considering a window of 8 rows = 2 HOURS

# Add a column for the moving average
df_with_ma = adjusted_df.withColumn("STATE_MA_SHORT", avg(col("STATE")).over(windowSpec))


# Creating a second moving avg for State, a longer one
windowSpec = Window.partitionBy("ID_MEDIDAS").orderBy("DATA_").rowsBetween(-95, 0)  # Considering a window of 8 rows = 1 DAY

# Add a column for the moving average
df_with_ma = df_with_ma.withColumn("STATE_MA_LONG", avg(col("STATE")).over(windowSpec))


df_with_ma.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Event Log

# COMMAND ----------

event_log = spark.read.table("hive_metastore.default.event_log_csv")
display(event_log)

# COMMAND ----------

#Formatar a data do event log para ficar igual ao arqlmed

event_log = event_log.withColumn("EVDATE", to_timestamp(col("EVDATE"), "dd/MM/yyyy HH:mm:ss"))

display(event_log)

# COMMAND ----------

event_log_short = event_log.drop(*["ID_EVDATE","ID_AREAGEOGRAFICA","ID_TIPOINSTALACAO","ID_SIGLA","ID_NIVELTENSAO","ID_OBJETO","ID_OPERATOR","NTIME","NTTAGTIME","STTAGMS","INS_COUNT","LOG_TYPE","STYPE","STTAGATR","STTAGCAUSE","STIMEMS","OPR_ID_LOAD","OPR_ID_LOAD","OPR_TM_LOAD","OPR_TIPO_OPERACAO","OPR_STM_FONTE","STATE_NUMBER","EVDESC_ML", "ID_TIPOEVENTO1", "ID_TIPOEVENTO2", "ID_TIPOEVENTO3", "ID_TIPOEVENTO4", "ID_TIPOEVENTO5","ID_EVENT_LOG", "TAGHL0", "TAGHL1", "TAGOPR", "LOGTYPE", "STATENUMBER"])

# COMMAND ----------

event_log_short.display()

# COMMAND ----------

event_log_short = event_log_short.withColumnRenamed('TAG1', 'ID')

# COMMAND ----------

columns_to_convert = ["ID_PAINEL", "ID_OBJECTO", "ID_ATRIBUTO"]

# Convert selected string columns to integer
for col_name in columns_to_convert:
    event_log_short = event_log_short.withColumn(col_name, col(col_name).cast("long"))

# COMMAND ----------

show_events = event_log_short.filter(event_log["ID_ATRIBUTO"] == "1863196")

show_events.display()

# COMMAND ----------

show_arlqmed = merge_arqlmed.filter(merge_arqlmed["ID"] == "HSMONC5TP1")

show_arlqmed.display()

# COMMAND ----------

#Filtrar apenas para os U--

event_log_short_U = event_log_short.filter(col("ID").rlike("U--$"))

display(event_log_short_U)

# COMMAND ----------

event_log_short_U = event_log_short_U.filter(
    (col("ID").substr(2, 1).isin("P", "S")) &  # Check position 2
    (~col("ID").substr(7, 1).isin("-", "9", "4"))  # Check position 7
)


# COMMAND ----------

event_log_short.select(
    min("EVDATE").alias("earliest_timestamp"),
    max("EVDATE").alias("latest_timestamp")
).display()

# COMMAND ----------

show_medidas = event_log_short_U.filter(event_log_short_U["ID_ATRIBUTO"] == 379820)

show_medidas.display()

# COMMAND ----------

joined_df_U.display()

# COMMAND ----------

event_log_short_U.display()

# COMMAND ----------

# common_values_df = joined_df_U.join(event_log, joined_df_U["ID"] == event_log_short_U["ID"], "inner")
# common_values_df.display()

common_values_df = joined_df_U.join(event_log_short_U, joined_df_U["ID"] == event_log_short_U["ID"], "inner")
common_values_df.display()

# COMMAND ----------

common_values_df.count()

# COMMAND ----------

show_medidas = common_values_df.filter(common_values_df["ID_MEDIDAS"] == 8340)

show_medidas.display()

# COMMAND ----------

show_medidas = df_arqlmed.filter(df_arqlmed["ID_MEDIDAS"] == 8340)

show_medidas.display()

# COMMAND ----------

show_medidas = adjusted_df.filter(adjusted_df["ID_MEDIDAS"] == 8340)

show_medidas.display()

# COMMAND ----------

show_medidas = medidas.filter(medidas["ID_MEDIDAS"] == 8340)

show_medidas.display()

# COMMAND ----------

# unique_ids_df = common_values_df.dropDuplicates(["ID"])

# unique_ids_df.display()

# COMMAND ----------

#unique_ids_df.count()

# COMMAND ----------

#Aqui quero acertar os limites, assegurar que LL<L<H<HH e remover as linhas que tem os limites a 0s



adjusted_common_values_df = common_values_df.withColumn("sorted_values", array_sort(array(col("ALR_LL_LIM"), 
                                                                     col("ALR_L_LIM"), 
                                                                     col("ALR_H_LIM"), 
                                                                     col("ALR_HH_LIM"))))

# Assign the sorted values back to the columns
adjusted_common_values_df = adjusted_common_values_df.withColumn("ALR_LL_LIM", col("sorted_values")[0]) \
                         .withColumn("ALR_L_LIM", col("sorted_values")[1]) \
                         .withColumn("ALR_H_LIM", col("sorted_values")[2]) \
                         .withColumn("ALR_HH_LIM", col("sorted_values")[3]) \
                         .drop("sorted_values")



# Show the adjusted DataFrame

show_medidas = adjusted_common_values_df.filter(adjusted_common_values_df["ID_MEDIDAS"] == 74126)

adjusted_common_values_df.display()


# COMMAND ----------

#common_values_df.count()

# COMMAND ----------

# unique_ids_df = adjusted_common_values_df.dropDuplicates(["ID"])

# unique_ids_df.display()

# COMMAND ----------

word_to_filter = "Alto"

# Filter rows with the specific word in the description column
eventos_alto_df = adjusted_common_values_df.filter(col("EVDESC").contains(word_to_filter))

# Show the resulting DataFrame
eventos_alto_df.display()

# COMMAND ----------

true_events = eventos_alto_df.filter((col("STATE") >= col("ALR_H_LIM")) & (col("STATE") < col("ALR_HH_LIM")))

true_events = true_events.select(["ID_MEDIDAS","DATA_","STATE","ALR_LL_LIM","ALR_L_LIM","ALR_H_LIM","ALR_HH_LIM","EVDESC"])

# Show the resulting DataFrame
true_events.display()

# COMMAND ----------

true_events.count()

# COMMAND ----------

eventos_alto_df.count()

# COMMAND ----------

show_medidas = adjusted_common_values_df.filter(adjusted_common_values_df["ID_MEDIDAS"] == 4856)

show_medidas = show_medidas.select(["ID_MEDIDAS","DATA_","STATE","ALR_LL_LIM","ALR_L_LIM","ALR_H_LIM","ALR_HH_LIM","EVDESC"])

show_medidas.display()

# COMMAND ----------

df_arqlmed.display()

# COMMAND ----------

df_with_ma.display()

# COMMAND ----------

event_log.display()

# COMMAND ----------

show_medidas = event_log.filter(event_log["TAG1"] == "APCRT-5503-0TU--")

show_medidas.display()

# COMMAND ----------

show_medidas = df_with_ma.filter(df_with_ma["ID"] == "APCRT-5503-0TU--")

show_medidas.display()

# COMMAND ----------

#testar filtrar por ID e timestamp para depois juntar o event_log ao arqlmed para ter os eventos nas datas em que eles ocorrem

filtered_dateid = df_with_ma.filter((df_with_ma.ID == "APCRT-5503-0TU--") & (df_with_ma.DATA_ == "2023-11-13T10:00:01.000+00:00"))


display(filtered_dateid)

# COMMAND ----------

df_with_ma.display()

# COMMAND ----------

event_log_short_U.display()

# COMMAND ----------

event_log_short_U.count()

# COMMAND ----------

unique_ids_count = df_with_ma.select("ID").distinct().count()
unique_ids_count

# COMMAND ----------

df_with_ma.count()

# COMMAND ----------

# Perform a cross join between the two DataFrames on the ID column
joined_df_try = df_with_ma.join(
    event_log_short_U.withColumnRenamed("timestamp", "DATA_"), ["ID"], "left_outer"
)

# Calculate the absolute time difference between EVDATE and DATA_
joined_df_try = joined_df_try.withColumn("time_diff", abs(col("EVDATE") - col("DATA_")))

# Create a window specification partitioned by ID and EVDATE, ordered by the absolute time difference
windowSpec = Window.partitionBy("ID", "EVDATE").orderBy(col("time_diff"))

# Use the window function to assign row numbers based on the time difference within each partition
joined_df_try = joined_df_try.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the closest matching event log entry for each row in df_with_ma
closest_df = joined_df_try.filter(col("row_num") == 1).drop("row_num", "time_diff")


# COMMAND ----------

#closest_df.display()

# COMMAND ----------

#unique_ids_count = closest_df.select("ID").distinct().count()
#unique_ids_count

# COMMAND ----------

#closest_df.count()

# COMMAND ----------

filtered_df = event_log_short_U.filter(col("ID") == "BSVARG3TP1-0TU--")
#display(filtered_df)

# COMMAND ----------

filtered_df = df_with_ma.filter(col("ID") == "BSVARG3TP1-0TU--")
#display(filtered_df)

# COMMAND ----------

closest_df_filter = closest_df.filter(col("ID_OBJECTO") == "1234")
#display(closest_df_filter)

# COMMAND ----------

count_df = closest_df.groupBy("ID_ARQLMED").count()
#count_df.display()

# COMMAND ----------



# COMMAND ----------

filtered_df = df_with_ma.filter(col("ID") == "PSGRDL2BR1-0TU--")
#display(filtered_df)

# COMMAND ----------

missing_ids_df = event_log_short_U.select("ID").distinct().join(
    df_with_ma.select("ID").distinct(), ["ID"], "left_anti"
)

missing_ids_df.display()

#Estes sao os ids que nao tiveram qualquer evento

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Penso que o join ate está correto, os nulls sao dos ids que nao tem nenhum evento, agora acho que o workaround para ter o dataset final será fazer merge desse dataset com o df_with_ma e assim consigo o dataset final e labels
# MAGIC

# COMMAND ----------

# Group by the column "ID_ARQLMED" and count the occurrences
count_df = closest_df.groupBy("ID_ARQLMED").count()

# Filter to keep only those ID_ARQLMED that are repeated (count > 1)
repeated_values_df = count_df.filter(col("count") > 1)

# COMMAND ----------

repeated_values_df.sort("count", ascending = False).display()

# COMMAND ----------

filtered_df = closest_df.filter(col("ID_ARQLMED") == "16704437460")
#display(filtered_df)

# COMMAND ----------

filtered_df = event_log_short_U.filter(col("ID_OBJECTO") == "33755")
display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explorar intensidade de corrente

# COMMAND ----------

joined_df.display()

# COMMAND ----------

# Generate df_arqlmed_U- using filter and like operations
joined_df_I = joined_df.filter(col("ID").rlike("0II--$"))
display(joined_df_I)


# COMMAND ----------

joined_df_I = joined_df_I.filter(
    (col("ID").substr(2, 1).isin("P", "S")) &  # Check position 2
    (~col("ID").substr(7, 1).isin("-", "9", "4"))  # Check position 7
)

display(joined_df_I)

# COMMAND ----------

joined_df_I.count()

# COMMAND ----------

intensity_over_H = joined_df_I.filter((joined_df_I["STATE"] > joined_df_I["ALR_H_LIM"]) & (joined_df_I["STATE"] < joined_df_I["ALR_HH_LIM"]))

# Display the filtered DataFrame
intensity_over_H.display()

# COMMAND ----------

intensity_over_H.count()

# COMMAND ----------

intensity_over_H_0 = intensity_over_H.filter(intensity_over_H["ALR_H_LIM"] == 0).count()
intensity_over_H_0

## apenas cerca de 8000 valores acima do H limit

# COMMAND ----------

i_relevant = joined_df_I.filter(F.col("ALR_H_LIM") > 10)

# COMMAND ----------

# Define window specification
windowSpec = Window.partitionBy("ID").orderBy("DATA_")

# Use lag to get the previous state
df = i_relevant.withColumn("prev_state", lag("STATE", 1).over(windowSpec))

# Identify transitions into and out of the above-limit state
df = df.withColumn("is_above", col("STATE") > col("ALR_H_LIM"))
df = df.withColumn("was_above", lag("is_above", 1).over(windowSpec))

# Create period labels for stretches above the limit
df = df.withColumn("period_start", when((col("is_above") == True) & (col("was_above") == False), 1).otherwise(0))
df = df.withColumn("period_id", sum("period_start").over(windowSpec.rangeBetween(Window.unboundedPreceding, 0)))

# Calculate time difference in minutes
df = df.withColumn(
    "time_diff", 
    (unix_timestamp("DATA_") - lag(unix_timestamp("DATA_"), 1).over(windowSpec)) / 60
)

# Fill null values in time_diff (first row in each period) with 15 (assuming each record is 15 minutes apart)
df = df.withColumn("time_diff", when(col("time_diff").isNull(), 15).otherwise(col("time_diff")))

# Group by ID and period, and sum the time_diff to get total duration above limit per period
result = df.filter(col("is_above") == True).groupBy("ID", "period_id").agg(sum("time_diff").alias("total_duration_above_limit"))

# Optionally, filter to find periods that exceed a certain duration threshold (e.g., 60 minutes)
threshold_minutes = 60
prolonged_periods = result.filter(col("total_duration_above_limit") > threshold_minutes)

# Show the periods where the duration above the limit is prolonged
prolonged_periods.display()

# COMMAND ----------

checker = joined_df_I.filter(col("ID") == "RSTLR-3302-0II--")
display(checker)

# COMMAND ----------

checker = event_log_short.filter(col("TAG1") == "RSTLR-3302-0II--")
display(checker)

# COMMAND ----------

joined_df_I.display()
