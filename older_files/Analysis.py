# Databricks notebook source
# MAGIC %md
# MAGIC This is the initial notebook where analysis will be performed

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_timestamp, max, min, contains, substring, collect_set
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# COMMAND ----------

# MAGIC %md
# MAGIC ###ARQLMED

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`arqlmed_csv`;

# COMMAND ----------

df_arqlmed = spark.read.table("hive_metastore.default.arqlmed_csv")
display(df_arqlmed)

# COMMAND ----------

df_arqlmed.printSchema()

# COMMAND ----------

unique_values_count_id_atributo = df_arqlmed.select("ID_ATRIBUTO").distinct().count()
print(unique_values_count_id_atributo)

# COMMAND ----------

unique_values_count_id_medidas = df_arqlmed.select("ID_MEDIDAS").distinct().count()
print(unique_values_count_id_medidas)

# COMMAND ----------

filtered_df = df_arqlmed.filter(df_arqlmed["ID_ATRIBUTO"] == 837576)
display(filtered_df)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df_arqlmed.count()

# COMMAND ----------

df_arqlmed.printSchema()

# COMMAND ----------

columns_to_convert = ["ID_ARQLMED", "ID_ATRIBUTO", "ID_MEDIDAS", "TYP", "SUBTYP", "STATUS", "STATE"]

# Convert selected string columns to integer
for col_name in columns_to_convert:
    df_arqlmed = df_arqlmed.withColumn(col_name, col(col_name).cast("long"))

# COMMAND ----------

df_arqlmed.printSchema()

# COMMAND ----------

display(df_arqlmed)

# COMMAND ----------

# MAGIC %md
# MAGIC Remover colunas nao importantes (nao removi o ID Load ainda porque tem dados relevantes errados)

# COMMAND ----------

df_arqlmed_short = df_arqlmed.drop(*["ID_ATRIBUTO","ID_TIME","SUBTYP","NSTATUS2","NTIME","OPR_TM_LOAD","OPR_TIPO_OPERACAO","OPR_STM_FONTE","CIM_UID"])

# COMMAND ----------

display(df_arqlmed_short)

# COMMAND ----------

# MAGIC %md
# MAGIC See what kind of IDs we have

# COMMAND ----------

df_arqlmed_short.groupBy('ID').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Os que terminam em U-- são aqueles que me são potencialmente úteis

# COMMAND ----------



df_arqlmed_short.filter(col("ID").like('%U-%')).count()

# COMMAND ----------

# MAGIC %md
# MAGIC temos 11 milhoes de valores uteis

# COMMAND ----------

# Generate df_arqlmed_U- using filter and like operations
df_arqlmed_U = df_arqlmed_short.filter(col("ID").like('%U-%'))

display(df_arqlmed_U)


# COMMAND ----------

#filtered_df = df_arqlmed.filter(df_arqlmed["ID_ATRIBUTO"] == 837576)
#display(filtered_df)

# COMMAND ----------

df_arqlmed_U.count()

# COMMAND ----------

    df_arqlmed_U_errado = df_arqlmed_U.filter(~trim(col("POLO")).isin(['Sul', 'Norte']))
    display(df_arqlmed_U_errado)

# COMMAND ----------

    df_arqlmed_U_correto = df_arqlmed_U.filter(trim(col("POLO")).isin(['Sul', 'Norte']))
    display(df_arqlmed_U_correto)

# COMMAND ----------

df_arqlmed_U_correto.count()

# COMMAND ----------

df_arqlmed_U_errado.count()

# COMMAND ----------

df_arqlmed_U.count()

# COMMAND ----------

# MAGIC %md
# MAGIC No df errado removi o polo original que tinha numeros e troquei pelo OPR_ID_LOAD que tinha o valor do Polo
# MAGIC
# MAGIC Dúvida: NStatus2 interessa??

# COMMAND ----------

df_correction = df_arqlmed_U_errado.drop("POLO")
df_correction = df_correction.withColumnRenamed("OPR_ID_LOAD", "POLO")

display(df_correction)

# COMMAND ----------

df_arqlmed_U_correto = df_arqlmed_U_correto.drop("OPR_ID_LOAD")

# COMMAND ----------

merge_arqlmed_U = df_correction.union(df_arqlmed_U_correto)

# COMMAND ----------

merge_arqlmed_U.count()

# COMMAND ----------

display(merge_arqlmed_U)

# COMMAND ----------

typ_values = merge_arqlmed_U.groupBy("TYP").count()

# Show the result
typ_values.show()

# COMMAND ----------

unique_values_state = merge_arqlmed_U.select("STATE").distinct().collect()
for row in unique_values_state:
    print(row["STATE"])

# COMMAND ----------

merge_arqlmed_U_date_timestamp = merge_arqlmed_U.withColumn("DATA_", to_timestamp(col("DATA_"), "dd/MM/yyyy HH:mm:ss"))

# COMMAND ----------

oldest_date = merge_arqlmed_U_date_timestamp.selectExpr("min(DATA_) as Oldest_Date").collect()[0]["Oldest_Date"]
most_recent_date = merge_arqlmed_U_date_timestamp.selectExpr("max(DATA_) as Most_Recent_Date").collect()[0]["Most_Recent_Date"]

print("Oldest Date:", oldest_date)
print("Most Recent Date:", most_recent_date)

# COMMAND ----------

# MAGIC %md
# MAGIC acabamos por ficar com um arqlmed com 11M de linhas e (em principio apenas 6 colunas)

# COMMAND ----------

# MAGIC %md
# MAGIC ###EVENT_LOG

# COMMAND ----------

event_log = spark.read.table("hive_metastore.default.event_log_csv")
display(event_log)

# COMMAND ----------

event_log.printSchema()

# COMMAND ----------

filtered_df = event_log.filter(
    (col("EVDESC").isNotNull()) & (col("EVDESC").rlike("(?i)BUCHHOLZ"))
)

display(filtered_df)

# COMMAND ----------

filtered_df.count()

# COMMAND ----------

event_log_short = filtered_df.withColumn("TAG1", substring(trim(col("TAG1")), 1, 12))

display(event_log_short)

# COMMAND ----------

df_arqlmed_short = df_arqlmed.withColumn("ID", substring(col("ID"), 1, 12))

display(df_arqlmed_short)

# COMMAND ----------

df = spark.table("model_table")

# COMMAND ----------

ids = [str(row.ID) for row in df.select("ID").distinct().collect()]

# 3. Filter event_log2 based on TAG1_short being in the list of IDs
filtered_event_log = event_log_short.filter(col("TAG1").isin(ids))

display(filtered_event_log)

# COMMAND ----------

filtered_df = filtered_event_log.filter(
    (col("EVDESC").isNotNull()) & (col("EVDESC").rlike("(?i)BUCHHOLZ"))
)

display(filtered_df)

# COMMAND ----------

# Group by 'ID' and collect distinct group keys
group_keys = filtered_event_log.groupBy("TAG1").agg(collect_set("TAG1")).select("TAG1").distinct()

# Show the distinct group keys
group_keys.display()

# COMMAND ----------

# Group by 'ID' and collect distinct group keys
group_keys = df.groupBy("ID").agg(collect_set("ID")).select("ID").distinct()

# Show the distinct group keys
group_keys.display()

# COMMAND ----------

matched_ids_df = df.join(event_log_short, df["ID"] == event_log_short["TAG1"], "left_semi")

display(matched_ids_df)

# COMMAND ----------

# Group by 'ID' and collect distinct group keys
group_keys = matched_ids_df.groupBy("ID").agg(collect_set("ID")).select("ID").distinct()

# Show the distinct group keys
group_keys.display()

# COMMAND ----------

check_ids = df.filter(df["ID"] == "LPFANH5506-0")
display(check_ids)

# COMMAND ----------

check_ids = df.filter(df["ID"] == "LSMACA5506-0")
display(check_ids)

# COMMAND ----------

# Filter rows where TENSION exceeds the tension limit H_LIM_T
df_exceed = df.filter(col("INTENSITY") > col("H_LIM_I"))

# Select the distinct IDs that meet this condition
df_exceed_ids = df_exceed.select("ID").distinct()

# Show the IDs
df_exceed_ids.display()

# COMMAND ----------

# Convert the DATE column to timestamp in the Spark DataFrame
df = df.withColumn('DATE', to_timestamp(df['DATE']))

# Filter for the specific transformer id
specific_id = "CSGMR-5506-0"
df_specific = df.filter(col("ID") == specific_id)

# Convert the filtered Spark DataFrame to a Pandas DataFrame
pdf = df_specific.toPandas()

# Ensure the DATE column is in datetime format
pdf['DATE'] = pd.to_datetime(pdf['DATE'])

# Optional: Verify the limit values for this specific id
print("Unique intensity limits for", specific_id, ":", pdf['H_LIM_I'].unique())
print("Unique tension limits for", specific_id, ":", pdf['H_LIM_T'].unique())

# Create subplots: two rows, one column; share the x-axis
fig, axs = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

# ---------------------------
# Plot for Intensity
# ---------------------------
axs[0].plot(pdf['DATE'], pdf['INTENSITY'], label='Intensity', marker='o', color='blue')

# Plot the intensity limit (using the first value, assuming it's constant)
if pdf['H_LIM_I'].nunique() == 1:
    intensity_limit = pdf['H_LIM_I'].iloc[0]
    axs[0].axhline(y=intensity_limit, color='blue', linestyle='--', label='Intensity Limit')
else:
    # If the limits vary, you might plot the entire series:
    axs[0].plot(pdf['DATE'], pdf['H_LIM_I'], label='Intensity Limit', linestyle='--', color='blue')

axs[0].set_ylabel('Intensity')
axs[0].set_title('Transformer Intensity Over Time for ' + specific_id)
axs[0].legend()
axs[0].grid(True)

# ---------------------------
# Plot for Tension
# ---------------------------
axs[1].plot(pdf['DATE'], pdf['TENSION'], label='Tension', marker='o', color='orange')

# Plot the tension limit (using the first value, assuming it's constant)
if pdf['H_LIM_T'].nunique() == 1:
    tension_limit = pdf['H_LIM_T'].iloc[0]
    axs[1].axhline(y=tension_limit, color='orange', linestyle='--', label='Tension Limit')
else:
    # If the limits vary, plot them over time:
    axs[1].plot(pdf['DATE'], pdf['H_LIM_T'], label='Tension Limit', linestyle='--', color='orange')

axs[1].set_ylabel('Tension')
axs[1].set_title('Transformer Tension Over Time for ' + specific_id)
axs[1].legend()
axs[1].grid(True)
axs[1].set_xlabel('Time')

# Set the x-axis to cover the full date range and format the date labels
axs[1].set_xlim(pdf['DATE'].min(), pdf['DATE'].max())
axs[1].xaxis.set_major_locator(mdates.AutoDateLocator())
axs[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Medidas

# COMMAND ----------

df_medidas = spark.read.table("hive_metastore.default.medidas_csv")
display(df_medidas)

# COMMAND ----------

df_medidas.printSchema()

# COMMAND ----------

L_LIM_COUNT = df_medidas.select("ALR_L_LIM").distinct()

# Show the result
L_LIM_COUNT.show(truncate=False)

# COMMAND ----------

unique_values = df_medidas.select("ALR_L_LIM").distinct().collect()
for row in unique_values:
    print(row["ALR_L_LIM"])

# COMMAND ----------

display(df_medidas.filter(col("ALR_L_LIM").like("S")))

# COMMAND ----------

LL_LIM_COUNT = df_medidas.select("ALR_LL_LIM").distinct()

# Show the result
LL_LIM_COUNT.show(truncate=False)

# COMMAND ----------

display(df_medidas.filter(col("ALR_LL_LIM").like("M")))

# COMMAND ----------

unique_values = df_medidas.select("ALR_H_LIM").distinct().collect()
for row in unique_values:
    print(row["ALR_H_LIM"])

# COMMAND ----------

display(df_medidas.filter(col("ALR_H_LIM").like("S")))

# COMMAND ----------

unique_values = df_medidas.select("ALR_HH_LIM").distinct().collect()
for row in unique_values:
    print(row["ALR_HH_LIM"])

# COMMAND ----------

max_value_L = df_medidas.agg(max("ALR_L_LIM")).collect()[0][0]
min_value_L = df_medidas.agg(min("ALR_L_LIM")).collect()[0][0]

# Print the results
print("Maximum value:", max_value_L)
print("Minimum value:", min_value_L)


# COMMAND ----------

from pyspark.sql.functions import col, substring, count

# Parse the ARQLMED ID to extract key features
df_arqlmed = df_arqlmed.withColumn("geographical_area", substring(col("ID"), 1, 1)) \
                       .withColumn("network_type", substring(col("ID"), 2, 1)) \
                       .withColumn("installation_code", substring(col("ID"), 3, 4)) \
                       .withColumn("tension_level", substring(col("ID"), 7, 1)) \
                       .withColumn("panel_id", substring(col("ID"), 8, 4)) \
                       .withColumn("equipment_type", substring(col("ID"), 12, 2)) \
                       .withColumn("attribute_type", substring(col("ID"), 14, 3))

display(df_arqlmed.limit(10))

# Filter for intensity and tension records based on attribute_type, and create new measurement columns from STATE
df_intensity = df_arqlmed.filter(col("attribute_type").like("I%")).alias("i") \
    .withColumn("intensity_value", col("STATE").cast("double"))
df_tension = df_arqlmed.filter(col("attribute_type").like("U%")).alias("t") \
    .withColumn("tension_value", col("STATE").cast("double"))

# Join intensity and tension records on installation_code and panel_id
df_joined = df_intensity.join(
    df_tension,
    (col("i.installation_code") == col("t.installation_code")) &
    (col("i.panel_id") == col("t.panel_id")),
    "inner"
)

# Select columns from the intensity side (renaming as needed) and bring in the tension measurement
df_final = df_joined.select(
    col("i.*"),
    col("tension_value")
)

display(df_final.limit(10))

# Group events by TAG1 (which corresponds to installation_code) to count events
df_event_counts = event_log.groupBy("TAG1").agg(count("*").alias("event_count"))
display(df_event_counts.limit(10))

# Join event counts to the final dataframe using the unambiguous installation_code from df_final
df_final = df_final.join(
    df_event_counts,
    df_final["installation_code"] == df_event_counts["TAG1"],
    "left"
)

display(df_final.limit(10))


# COMMAND ----------



# COMMAND ----------

what

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


