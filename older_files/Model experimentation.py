# Databricks notebook source

from pyspark.sql.functions import col, trim, to_timestamp, max, min, to_date, from_unixtime, date_trunc, date_format, when, lit, least, greatest, avg, lag, array, array_sort, abs, expr, minute, hour, round, row_number, first, sum, unix_timestamp, regexp_extract, sequence, explode, udf, count, dayofmonth, coalesce, substring, collect_list, size, count_distinct, greatest, corr
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from functools import reduce
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

# COMMAND ----------

df = spark.table("model_table")

# COMMAND ----------

from pyspark.sql.functions import min, max

df_dates = df.select(
    min("DATE").alias("oldest_date"),
    max("DATE").alias("most_recent_date")
)

display(df_dates)

# COMMAND ----------

df.display()

# COMMAND ----------

df_try = df.drop("TIME_OVER_LIMIT_T","TIME_OVER_LIMIT_I", "H_LIM_T", "H_LIM_I")

# COMMAND ----------

df_try.display()

# COMMAND ----------

# Convert AM/PM to numeric values (0 for AM, 1 for PM)
df_try = df_try.withColumn(
    'AM_PM',
    F.when(F.col('AM_PM') == 'AM', 0).otherwise(1)
)


# Show the result
df_try.display()

# COMMAND ----------

target_columns = ["TENSION", "INTENSITY"]
feature_columns = ["TENSION", "INTENSITY", "MAVERAGE_2H_I", "MAVERAGE_2H_T", 
                   "MAVERAGE_1D_I", "MAVERAGE_1D_T", "EVENT_COUNT_I", "EVENT_COUNT_T", "DAY_OF_WEEK", 
                   "DAY_OF_MONTH", "DAY_OF_YEAR", "HOUR_OF_DAY", "AM_PM"]

correlations = {target: {} for target in target_columns}

# Calculate correlation for each feature with each target
for target in target_columns:
    for feature in feature_columns:
        correlation = df_try.select(corr(feature, target)).collect()[0][0]
        correlations[target][feature] = correlation

# Display results
for target, corrs in correlations.items():
    print(f"\nCorrelations with {target}:")
    for feature, value in corrs.items():
        print(f"{feature}: {value}")

# COMMAND ----------

import pandas as pd

# Convert the correlations dictionary to a pandas DataFrame
correlation_df = pd.DataFrame(correlations)
print(correlation_df)

# COMMAND ----------

# Group by 'ID' and collect distinct group keys
group_keys = df_try.groupBy("ID").agg(F.collect_set("ID")).select("ID").distinct()

# Show the distinct group keys
group_keys.display()

# COMMAND ----------

summary_df = df.groupBy("ID").agg(
    avg("intensity").alias("avg_intensity"),
    avg("tension").alias("avg_tension")
)
display(summary_df)

# Compute quantiles and IQR for avg_intensity
quantiles_intensity = summary_df.approxQuantile("avg_intensity", [0.25, 0.75], 0.05)
q1_int, q3_int = quantiles_intensity[0], quantiles_intensity[1]
IQR_int = q3_int - q1_int
lower_bound_int = q1_int - 1.5 * IQR_int
upper_bound_int = q3_int + 1.5 * IQR_int

# Compute quantiles and IQR for avg_tension
quantiles_tension = summary_df.approxQuantile("avg_tension", [0.25, 0.75], 0.05)
q1_tens, q3_tens = quantiles_tension[0], quantiles_tension[1]
IQR_tens = q3_tens - q1_tens
lower_bound_tens = q1_tens - 1.5 * IQR_tens
upper_bound_tens = q3_tens + 1.5 * IQR_tens

# Filter out outliers for both intensity and tension
filtered_summary_df = summary_df.filter(
    (col("avg_intensity") >= lower_bound_int) & (col("avg_intensity") <= upper_bound_int) &
    (col("avg_tension") >= lower_bound_tens) & (col("avg_tension") <= upper_bound_tens)
)
display(filtered_summary_df)

# COMMAND ----------

filtered_pd = filtered_summary_df.toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))
plt.hist(filtered_pd['avg_intensity'], bins=10, edgecolor='black')
plt.title("Histogram of Average Intensity per Transformer")
plt.xlabel("Average Intensity")
plt.ylabel("Number of Transformers")
display(plt.gcf())
plt.close()

# COMMAND ----------

plt.figure(figsize=(10,6))
plt.hist(filtered_pd['avg_tension'], bins=10, edgecolor='black')
plt.title("Histogram of Average Tension per Transformer")
plt.xlabel("Average Tension")
plt.ylabel("Number of Transformers")
display(plt.gcf())
plt.close()

# COMMAND ----------

import matplotlib.pyplot as plt

# Get distinct transformer IDs
transformer_ids = [row.transformer_id for row in df.select("transformer_id").distinct().collect()]

for t_id in transformer_ids:
    # Filter data for the current transformer
    t_data = df.filter(df.transformer_id == t_id).toPandas()
    
    plt.figure(figsize=(12, 6))
    plt.plot(t_data['timestamp'], t_data['intensity'], label='Intensity')
    plt.plot(t_data['timestamp'], t_data['tension'], label='Tension')
    plt.axhline(y=high_limit_value, color='r', linestyle='--', label='High Limit')
    plt.title(f"Transformer {t_id}: Intensity and Tension Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.legend()
    display(plt.gcf())
    plt.close()


# COMMAND ----------

from pyspark.sql.functions import when, col

# Create binary columns to flag exceedances for both intensity and tension
data = df.withColumn("intensity_exceed", when(col("intensity") > high_limit_value, 1).otherwise(0)) \
           .withColumn("tension_exceed", when(col("tension") > high_limit_value, 1).otherwise(0))
display(data)


# COMMAND ----------

df.display()

# COMMAND ----------

date_interval_df = df.agg(
    min("DATE").alias("first_date"),
    max("DATE").alias("last_date")
)
display(date_interval_df)

# COMMAND ----------

import matplotlib.pyplot as plt

# Get distinct transformer IDs
transformer_ids = [row.ID for row in df.select("ID").distinct().collect()]

for t_id in transformer_ids:
    # Filter data for the current transformer
    t_data = df.filter(df.ID == t_id).toPandas()
    
    plt.figure(figsize=(12, 6))
    plt.plot(t_data['DATE'], t_data['INTENSITY'], label='Intensity')
    plt.plot(t_data['DATE'], t_data['TENSION'], label='Tension')
    plt.title(f"Transformer {t_id}: Intensity and Tension Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.legend()
    display(plt.gcf())
    plt.close()


# COMMAND ----------

from pyspark.sql.functions import when, col

# Create binary columns to flag exceedances for both intensity and tension
data = df.withColumn("intensity_exceed", when(col("INTENSITY") > col("H_LIM_I"), 1).otherwise(0)) \
           .withColumn("tension_exceed", when(col("TENSION") > col("H_LIM_T"), 1).otherwise(0))
display(data)


# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

exceed_df = data.groupBy("ID").agg(
    spark_sum("intensity_exceed").alias("total_intensity_exceed"),
    spark_sum("tension_exceed").alias("total_tension_exceed")
)
display(exceed_df)

# COMMAND ----------

for t_id in transformer_ids:
    t_data = data.filter(data.ID == t_id).toPandas()
    
    plt.figure(figsize=(12, 6))
    plt.plot(t_data['DATE'], t_data['INTENSITY'], label='Intensity')
    plt.fill_between(t_data['DATE'], t_data['H_LIM_I'], t_data['INTENSITY'],
                     where=(t_data['INTENSITY'] > t_data['H_LIM_I']), color='red', alpha=0.3)
    plt.title(f"Transformer {t_id}: Intensity Exceedance Visualization")
    plt.xlabel("Timestamp")
    plt.ylabel("Intensity")
    plt.legend()
    display(plt.gcf())
    plt.close()


# COMMAND ----------

for t_id in transformer_ids:
    t_data = data.filter(data.ID == t_id).toPandas()
    
    plt.figure(figsize=(12, 6))
    plt.plot(t_data['DATE'], t_data['TENSION'], label='Tension')
    plt.fill_between(t_data['DATE'], t_data['H_LIM_T'], t_data['TENSION'],
                     where=(t_data['TENSION'] > t_data['H_LIM_T']), color='red', alpha=0.3)
    plt.title(f"Transformer {t_id}: Tension Exceedance Visualization")
    plt.xlabel("Timestamp")
    plt.ylabel("Tension")
    plt.legend()
    display(plt.gcf())
    plt.close()


# COMMAND ----------



# COMMAND ----------

# MAGIC %pip install tensorflow

# COMMAND ----------

#%restart_python

# COMMAND ----------

import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
from tensorflow.keras.preprocessing.sequence import TimeseriesGenerator

# COMMAND ----------

# Convert PySpark DataFrame to Pandas DataFrame for scaling and sequencing
df = df_try.toPandas()

# Define columns to be used for features and targets
feature_columns = ['TENSION', 'INTENSITY', 'MAVERAGE_2H_T', 'MAVERAGE_2H_I', 'MAVERAGE_1D_I', 'MAVERAGE_1D_T', 
                   'EVENT_COUNT_I', 'EVENT_COUNT_T', 'DAY_OF_WEEK', 'DAY_OF_MONTH', 'DAY_OF_YEAR', 'HOUR_OF_DAY', 'AM_PM']
target_columns = ['TENSION', 'INTENSITY']

# Set the timestep for sequence length (half a day = 2880, 1440, 720 steps, for example)
time_steps = 10  

# Prepare empty lists to hold processed sequences and labels
X_sequences = []
y_sequences = []

# Group by 'ID' and process each group individually
for _, group in df.groupby('ID'):
    # Extract features and targets
    features = group[feature_columns]
    targets = group[target_columns]
    
    # Initialize and fit scalers
    scaler_features = MinMaxScaler()
    scaler_targets = MinMaxScaler()
    
    scaled_features = scaler_features.fit_transform(features)
    scaled_targets = scaler_targets.fit_transform(targets)
    
    # Generate sequences for this equipment ID
    generator = TimeseriesGenerator(scaled_features, scaled_targets, length=time_steps, batch_size=32)
    
    # Append sequences from generator to the main list
    for X, y in generator:
        X_sequences.append(X)
        y_sequences.append(y)

# Convert lists to numpy arrays for model compatibility
import numpy as np
X_final = np.array(X_sequences)
y_final = np.array(y_sequences)

# Check shapes
print(X_final.shape)  # Should print (total_sequences, time_steps, number_of_features)
print(y_final.shape)  # Should print (total_sequences, number_of_targets)

# Now `X_final` and `y_final` are ready to be passed into an LSTM model

# COMMAND ----------

filtered_df = df.filter((col("TIME_OVER_LIMIT_T") == 2) | (col("TIME_OVER_LIMIT_I") == 2))

# Group by ID and count the occurrences for each ID
count_df = filtered_df.groupBy("ID").agg(count("*").alias("count"))

# Show the result
count_df.display()

# COMMAND ----------

# Filter rows where TIME_OVER_LIMIT_T or TIME_OVER_LIMIT_I are greater than 0
df_with_prefix = df.filter((col("TIME_OVER_LIMIT_T") > 0) | (col("TIME_OVER_LIMIT_I") > 0))

# Create a new column with the first 6 characters of the ID
df_with_prefix = df_with_prefix.withColumn("ID_prefix", substring(col("ID"), 1, 6))

# Group by ID and count the occurrences for each ID, but keep the ID_prefix column
count_df = df_with_prefix.groupBy("ID", "ID_prefix").agg(count("*").alias("count"))

# Now, group by ID_prefix and calculate the object count and total count for each prefix
result_df = count_df.groupBy("ID_prefix").agg(
    count("*").alias("object_count"),  # Count of all objects with the same ID_prefix
    sum("count").alias("total_count")  # Sum of the counts for each ID_prefix
)

# Show the result
result_df.display()

# COMMAND ----------

#Remover este

id_prefix_value = 'FSALGW'  # Replace with the actual prefix, e.g., 'ABC123'

# Filter by the specific ID prefix and count distinct IDs
distinct_ids_count = df_with_prefix.filter(col("ID_prefix") == id_prefix_value)
distinct_ids_count = distinct_ids_count.groupBy("ID").count()
distinct_ids_count.display()

# COMMAND ----------

id = 'OSPAAA5509A0'  # Replace with the actual prefix, e.g., 'ABC123'

# Filter by the specific ID prefix and count distinct IDs
distinct_ids_count = df_with_prefix.filter(col("ID") == id)
distinct_ids_count.display()

# COMMAND ----------

# data inicio dados = 2023-11-07 data fim = 2023-12-03

# Specify the equipment ID and metric you want to plot
equipment_id = "FSALGW5506-0"
metric_column = "TENSION"
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
row = df.filter(df["ID"] == equipment_id).select("H_LIM_T").first()
limit_value = row["H_LIM_T"]

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

metric_column = "INTENSITY"
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
row = df.filter(df["ID"] == equipment_id).select("H_LIM_I").first()
limit_value = row["H_LIM_I"]

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
equipment_id = "SPRBB-5505-0"
metric_column = "INTENSITY"
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
row = df.filter(df["ID"] == equipment_id).select("H_LIM_I").first()
limit_value = row["H_LIM_I"]

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
equipment_id = "SPRBB-5505-0"
metric_column = "TENSION"
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
row = df.filter(df["ID"] == equipment_id).select("H_LIM_T").first()
limit_value = row["H_LIM_T"]

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

# MAGIC %restart_python

# COMMAND ----------

df = spark.table("model_table")

# COMMAND ----------

df.display()

# COMMAND ----------

df_model = df.toPandas()

df_model = df_model.sample(frac=0.1, random_state=42)

# COMMAND ----------

import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.model_selection import train_test_split

def preprocess_data(df, time_steps, feature_cols, target_cols):
    # Check if all feature columns and target columns are present
    missing_features = [col for col in feature_cols if col not in df.columns]
    missing_targets = [col for col in target_cols if col not in df.columns]
    if missing_features or missing_targets:
        raise ValueError(f"Missing columns - Features: {missing_features}, Targets: {missing_targets}")

    # Separate features and targets
    features = df[feature_cols].copy()
    targets = df[target_cols].copy()
    
    # One-hot encode categorical features
    categorical_features = ['DAY_OF_WEEK', 'AM_PM']
    one_hot_encoder = OneHotEncoder(sparse=False, drop='first')
    encoded_categorical = one_hot_encoder.fit_transform(features[categorical_features])
    
    # Drop original categorical columns
    features = features.drop(columns=categorical_features)
    
    # Scale numerical features
    scaler = MinMaxScaler()
    features = scaler.fit_transform(features)
    
    # Concatenate encoded categorical features with scaled numerical features
    features = np.hstack((features, encoded_categorical))

    sequences = []
    target_arrays = {col: [] for col in target_cols}
    
    for i in range(len(df) - time_steps):
        sequences.append(features[i:i+time_steps])
        for col in target_cols:
            target_arrays[col].append(targets[col].iloc[i + time_steps])
    
    sequences = np.array(sequences)
    target_arrays = {col: np.array(target_arrays[col]) for col in target_cols}
    
    return sequences, target_arrays

# Define sequences
time_steps = 24  # Example: using past 24 hours as a sequence length

# List of features to include
feature_cols = [
    'INTENSITY', 'TENSION', 
    'H_LIM_I', 'H_LIM_T',
    'MAVERAGE_2H_I', 'MAVERAGE_2H_T', 
    'MAVERAGE_1D_I', 'MAVERAGE_1D_T',
    'EVENT_COUNT_I', 'EVENT_COUNT_T',
    'TIME_OVER_LIMIT_I', 'TIME_OVER_LIMIT_T',
    'DAY_OF_WEEK', 'DAY_OF_MONTH', 'DAY_OF_YEAR', 'HOUR_OF_DAY', 'AM_PM'
]

target_cols = ['TIME_OVER_LIMIT_I', 'TIME_OVER_LIMIT_T']

# Assuming 'df_model' is your prepared pandas dataframe
sequences, targets = preprocess_data(df_model, time_steps, feature_cols, target_cols)

# Convert targets dictionary to a NumPy array
y_targets = np.vstack([targets[col] for col in target_cols]).T

# Split data
X_train, X_test, y_train, y_test = train_test_split(sequences, y_targets, test_size=0.2, shuffle=False)

# Model definition


# COMMAND ----------

from tensorflow.keras.layers import Input, Dense, LSTM, Dropout
from tensorflow.keras.models import Model

# Define the input layer
input_layer = Input(shape=(time_steps, sequences.shape[2]))

# Define LSTM layer
lstm_layer = LSTM(50, activation='tanh')(input_layer)
dropout_layer = Dropout(0.2)(lstm_layer)

# Define two output layers, one for each target
output_i = Dense(4, activation='softmax', name='output_i')(dropout_layer)
output_t = Dense(4, activation='softmax', name='output_t')(dropout_layer)

# Create the model
model = Model(inputs=input_layer, outputs=[output_i, output_t])

# Compile the model
model.compile(optimizer='adam', 
              loss={'output_i': 'sparse_categorical_crossentropy', 
                    'output_t': 'sparse_categorical_crossentropy'},
              metrics={'output_i': 'accuracy', 
                       'output_t': 'accuracy'})

# Train the model
history = model.fit(X_train, 
                    {'output_i': y_train[:, 0], 'output_t': y_train[:, 1]}, 
                    epochs=10, 
                    batch_size=32, 
                    validation_split=0.2)

# Evaluate the model
losses, accuracies = model.evaluate(X_test, 
                                    {'output_i': y_test[:, 0], 'output_t': y_test[:, 1]})
accuracy_i = accuracies[0]  # Accuracy for TIME_OVER_LIMIT_I
accuracy_t = accuracies[1]  # Accuracy for TIME_OVER_LIMIT_T

print(f"Test Accuracy for TIME_OVER_LIMIT_I: {accuracy_i:.2f}")
print(f"Test Accuracy for TIME_OVER_LIMIT_T: {accuracy_t:.2f}")


# COMMAND ----------

# MAGIC %md
# MAGIC Plan, create a single label, comprising of both variables for tension and intensity
# MAGIC Then normalize, then train

# COMMAND ----------

df = df.withColumn('TIME_OVER_LIMIT', greatest(col('TIME_OVER_LIMIT_I'), col('TIME_OVER_LIMIT_T')))
df = df.drop('TIME_OVER_LIMIT_I', 'TIME_OVER_LIMIT_T')
# Show the result
df.display()

# COMMAND ----------

# Filter by the specific ID prefix and count distinct IDs
distinct_ids_count = df.filter(col("TIME_OVER_LIMIT") > 0)
distinct_ids_count.display()

# COMMAND ----------

!pip install tensorflow

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
from pandas import DataFrame , concat
from sklearn.metrics import mean_absolute_error , mean_squared_error
%matplotlib inline
%config InlineBackend.figure_format = 'retina'
from numpy import mean , concatenate
from math import sqrt
from pandas import read_csv
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense,LSTM,Activation
from sklearn.preprocessing import LabelEncoder
#from keras.models import Sequential
#from keras.layers import Dense
#from keras.layers import LSTM

# COMMAND ----------

df

# COMMAND ----------

x_1 = df['dew'].values
x_2 = dataset['temp'].values
x_3 = dataset['press'].values
x_4 = dataset['wnd_spd'].values
x_5 = dataset['wnd_dir'].values
x_6 = dataset['snow'].values
x_7 = dataset['rain'].values
y = dataset['pollution'].values
