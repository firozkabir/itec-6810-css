# Databricks notebook source
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ###### prepare bikeshare data

# COMMAND ----------

# this is the data directory for this spark instance. 
# Can be a volume, local path or a cloud object storage. 
data_dir = "/Volumes/eip_dev/internal/data-files/"

# COMMAND ----------

# MAGIC %md
# MAGIC rest of the code expects you to have untar the `ccs.tar.gz` file into the `{data_dir}`

# COMMAND ----------

df = spark.read.option("inferSchema", "true") \
               .option("header", "true") \
                .csv(f"{data_dir}/css/bike-share/*/*.csv")
#display(df)

# COMMAND ----------

df = df.toDF(*[c.lower().replace(' ', '_') for c in df.columns])
# display(df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df = df.withColumn("start_time", to_timestamp("start_time", "MM/dd/yyyy HH:mm")) \
       .withColumn("end_time", to_timestamp("end_time", "MM/dd/yyyy HH:mm"))
# display(df)

# COMMAND ----------

from pyspark.sql.functions import dayofmonth, month, year, hour, minute

df = df.withColumn("start_day", dayofmonth("start_time")) \
       .withColumn("start_month", month("start_time")) \
       .withColumn("start_year", year("start_time")) \
       .withColumn("start_hour", hour("start_time")) \
       .withColumn("start_minute", minute("start_time"))
# display(df)

# COMMAND ----------

from pyspark.sql.functions import date_format

df = df.withColumn("dteday", date_format("start_time", "yyyy-MM-dd"))
# display(df)

# COMMAND ----------

from pyspark.sql.functions import dayofweek

df = df.withColumn("weekday", dayofweek("dteday"))
# display(df)

# COMMAND ----------

from pyspark.sql.functions import month, when

df = df.withColumn("start_month", month("start_time"))
df = df.withColumn("season", when(df.start_month.isin(3, 4, 5), 1)
                             .when(df.start_month.isin(6, 7, 8), 2)
                             .when(df.start_month.isin(9, 10, 11), 3)
                             .when(df.start_month.isin(12, 1, 2), 4))
# display(df)

# COMMAND ----------

from pyspark.sql.functions import lit

df_holidays = spark.read \
                   .csv(f"{data_dir}/css/holidays/holidays.csv", header=True)

df_holidays = df_holidays.withColumnRenamed("date", "dteday").withColumn("holiday", lit(1))
# display(df_holidays)

# COMMAND ----------

df = df.join(df_holidays, on=["dteday"], how="left")
# display(df)

# COMMAND ----------

from pyspark.sql.functions import coalesce

df = df.withColumn("holiday", coalesce(df["holiday"], lit(0)))
# display(df)

# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn("casual_or_registered", when(df["user_type"] == "Casual Member", "casual").otherwise("registered"))
# display(df)

# COMMAND ----------

df = df.withColumnRenamed("start_year", "yr") \
       .withColumnRenamed("start_month", "mnth") \
       .withColumnRenamed("start_hour", "hr")
# display(df)

# COMMAND ----------

df = df.withColumn("workingday", when((df["holiday"] == 1) | (df["weekday"].isin(0, 1)), 0).otherwise(1))
# display(df)

# COMMAND ----------

df_output = df.select("dteday", "season", "yr", "mnth", "hr", "holiday", "weekday", "workingday", "casual_or_registered", "start_day")
# display(df_output)

# COMMAND ----------

df_pivot = df_output.groupBy("dteday", "season", "yr", "mnth", "hr", "holiday", "weekday", "workingday", "start_day").pivot("casual_or_registered").count()
# display(df_pivot)

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

df_pivot = df_pivot.withColumn("casual", coalesce(df_pivot["casual"], lit(0)))
df_pivot = df_pivot.withColumn("registered", coalesce(df_pivot["registered"], lit(0)))
# display(df_pivot)

# COMMAND ----------

from pyspark.sql.functions import col

df_pivot = df_pivot.withColumn("cnt", col("casual") + col("registered"))
# display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### prepare weather data

# COMMAND ----------

df_weather = spark.read.option("inferSchema", "true") \
                        .option("header", "true") \
                        .csv(f"{data_dir}/css/weather/to-data/*.csv")
# display(df_weather)

# COMMAND ----------

from pyspark.sql.functions import to_date, hour

df_weather = df_weather.withColumn('dteday', to_date(df_weather['time (LST)'], 'yyyy-MM-dd'))
df_weather = df_weather.withColumn('hr', hour(df_weather['time (LST)']))
# display(df_weather)

# COMMAND ----------

from pyspark.sql.functions import coalesce

df_weather = df_weather.withColumn('atemp', coalesce(df_weather['Hmdx'], df_weather['Wind Chill']))
# display(df_weather)

# COMMAND ----------

from pyspark.sql.functions import coalesce

df_weather = df_weather.withColumn('atemp', coalesce(df_weather['Hmdx'], df_weather['Wind Chill']))
# display(df_weather)

# COMMAND ----------

from pyspark.sql.functions import coalesce

df_weather = df_weather.withColumn('atemp', coalesce(df_weather['Temp (°C)'], df_weather['atemp']))
# display(df_weather)

# COMMAND ----------

df_weather = df_weather.withColumnRenamed('Temp (°C)', 'temp').withColumnRenamed('Rel Hum (%)', 'hum').withColumnRenamed('Wind Spd (km/h)', 'windspeed')
display(df_weather)

# COMMAND ----------

df_weather = df_weather.withColumnRenamed("Year", "yr") \
                      .withColumnRenamed("Month", "mnth") \
                      .withColumnRenamed("Day", "start_day") \
                      .withColumnRenamed('Dew Point Temp (°C)', 'dewpoint') \
                      .withColumnRenamed('Precip. Amount (mm)', 'percip') \
                      .withColumnRenamed('Visibility (km)', 'visibility') \
                      .withColumnRenamed('Weather', 'weather')

#df_weather = df_weather.drop('dteday')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### prepare final df

# COMMAND ----------

df = df_pivot.join(df_weather, on=['yr', 'mnth', 'start_day', 'hr'], how='inner')
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

df = df.fillna(0)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### perform multivariate regression analysis

# COMMAND ----------

independent_variables = ['season', 'yr', 'mnth', 'holiday', 'weekday', 'workingday', 'temp', 'hum', 'hr']
dependent_variable = ['cnt']
df_multi = df.select( independent_variables + dependent_variable ) 
for i in df_multi.columns:
        print( "Correlation to CNT for ", i, df_multi.stat.corr('cnt', i))

# COMMAND ----------

print(df_multi.columns)

# COMMAND ----------

display(df_multi)

# COMMAND ----------

# (5) Generate Input Feature Vectors from the Raw Spark DataFrame
#multivariate_feature_columns = ['season', 'yr', 'mnth', 'temp']
multivariate_feature_columns = ['season', 'yr', 'mnth', 'holiday', 'weekday', 'workingday', 'temp', 'hum', 'hr']
multivariate_label_column = 'cnt'
vector_assembler = VectorAssembler(inputCols = multivariate_feature_columns, outputCol = 'features')
df_multi = vector_assembler.transform(df_multi).select(['features', multivariate_label_column])
df_multi.head(10)

# COMMAND ----------

# (6) Split the Raw DataFrame into a Training DataFrame and a Test DataFrame
train_df, test_df = df_multi.randomSplit([0.75, 0.25], seed=12345)
train_df.count(), test_df.count()

# COMMAND ----------

linear_regression = LinearRegression(featuresCol = 'features', labelCol = multivariate_label_column)
linear_regression_model = linear_regression.fit(train_df)

# COMMAND ----------

print("Model Coefficients: " + str(linear_regression_model.coefficients))
print("Intercept: " + str(linear_regression_model.intercept))
training_summary = linear_regression_model.summary
print("RMSE: %f" % training_summary.rootMeanSquaredError)
print("R-SQUARED: %f" % training_summary.r2)
print("TRAINING DATASET DESCRIPTIVE SUMMARY: ")
train_df.describe().show()
print("TRAINING DATASET RESIDUALS: ")
training_summary.residuals.show()

# COMMAND ----------

test_linear_regression_predictions_df = linear_regression_model.transform(test_df)
print("TEST DATASET PREDICTIONS AGAINST ACTUAL LABEL: ")
test_linear_regression_predictions_df.select("prediction", multivariate_label_column, "features").show(10)

# COMMAND ----------

test_summary = linear_regression_model.evaluate(test_df)
print("RMSE on Test Data = %g" % test_summary.rootMeanSquaredError)
print("R-SQUARED on Test Data = %g" % test_summary.r2)

# COMMAND ----------

# Convert predictions to Pandas DataFrame for visualization
predictions_pd = test_linear_regression_predictions_df.select("cnt", "prediction").toPandas()

# Plot the results
plt.figure(figsize=(10, 6))
plt.scatter(predictions_pd["cnt"], predictions_pd["prediction"], color='blue', label='Predictions')
plt.plot([predictions_pd["cnt"].min(), predictions_pd["cnt"].max()], 
        [predictions_pd["cnt"].min(), predictions_pd["cnt"].max()], color='red', linewidth=2, label='Ideal Fit')
plt.xlabel("Actual Count")
plt.ylabel("Predicted Count")
plt.title("Linear Regression Predictions vs Actual")
plt.legend()
plt.show()


# COMMAND ----------

x

# COMMAND ----------

# MAGIC %md
# MAGIC ###### perform univariate regression analysis

# COMMAND ----------

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt

def perform_analysis(feature_columns, prediction_column):
  # Prepare the data for linear regression
  assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
  data = assembler.transform(df).select("features", prediction_column)
  lr = LinearRegression(labelCol=prediction_column, featuresCol="features")
  lr_model = lr.fit(data)

  # Create a Linear Regression model
  lr = LinearRegression(labelCol="cnt", featuresCol="features")

  # Fit the model
  lr_model = lr.fit(data)

  # Display the coefficients and intercept
  coefficients = lr_model.coefficients
  intercept = lr_model.intercept
  pvalues = lr_model.summary.pValues
  r2 = lr_model.summary.r2

  print("Coefficients: ", coefficients)
  print("Intercept: ", intercept)
  print("pvalues: ", pvalues)
  print("r2: ", r2)

  print("Model Coefficients: " + str(lr_model.coefficients))
  print("Intercept: " + str(lr_model.intercept))
  training_summary = lr_model.summary
  print("RMSE: %f" % training_summary.rootMeanSquaredError)
  print("R-SQUARED: %f" % training_summary.r2)

  # Make predictions
  predictions = lr_model.transform(data)
  #display(predictions)

  # Convert predictions to Pandas DataFrame for visualization
  predictions_pd = predictions.select("cnt", "prediction").toPandas()

  # Plot the results
  plt.figure(figsize=(10, 6))
  plt.scatter(predictions_pd["cnt"], predictions_pd["prediction"], color='blue', label='Predictions')
  plt.plot([predictions_pd["cnt"].min(), predictions_pd["cnt"].max()], 
          [predictions_pd["cnt"].min(), predictions_pd["cnt"].max()], color='red', linewidth=2, label='Ideal Fit')
  plt.xlabel("Actual Count")
  plt.ylabel("Predicted Count")
  plt.title("Linear Regression Predictions vs Actual")
  plt.legend()
  plt.show()


# COMMAND ----------

perform_analysis(feature_columns=["percip", "temp", "hum", "season", "workingday"], prediction_column="cnt")

# COMMAND ----------

perform_analysis(feature_columns=["percip", "temp", "season", "workingday"], prediction_column="cnt")

# COMMAND ----------

perform_analysis(feature_columns=["percip", "temp", "workingday"], prediction_column="cnt")


# COMMAND ----------

perform_analysis(feature_columns=["percip", "temp"], prediction_column="cnt")


# COMMAND ----------

perform_analysis(feature_columns=["percip"], prediction_column="cnt")


# COMMAND ----------

perform_analysis(feature_columns=["temp"], prediction_column="cnt")