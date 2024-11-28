# Databricks notebook source
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
display(df.count())

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

# DBTITLE 1,Reading and Renaming Holidays Data Using PySpark
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


# COMMAND ----------

print(df_weather.columns)

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

df_weather_filtered = df_weather.filter(df_weather['Wind Spd (km/h)'].isNotNull())
display(df_weather_filtered)

# COMMAND ----------

df_weather = df_weather.withColumnRenamed('Temp (°C)', 'temp').withColumnRenamed('Rel Hum (%)', 'hum').withColumnRenamed('Wind Spd (km/h)', 'windspeed')
# display(df_weather)

# COMMAND ----------

df_weather = df_weather.withColumn('hum', col('hum')/100)
df_weather = df_weather.withColumn('temp', col('temp')/((39+8)-8))
df_weather = df_weather.withColumn('atemp', col('atemp')/((50+16)-16))

# COMMAND ----------

from pyspark.sql.functions import when, col, lower, regexp_replace

df_weather = df_weather.withColumn('weather_clean', lower(regexp_replace(col('weather'), '[^\w\s]', '')))

df_weather = df_weather.withColumn('weathersit', 
                   when(col('weather_clean').rlike('clear|few clouds|partly cloudy'), 1)
                   .when(col('weather_clean').rlike('mist cloudy|mist broken clouds|mist few clouds|mist'), 2)
                   .when(col('weather_clean').rlike('light snow|light rain thunderstorm scattered clouds|light rain scattered clouds'), 3)
                   .when(col('weather_clean').rlike('heavy rain ice pallets thunderstorm mist|snow fog'), 4)
                   .otherwise(None))

df_weather = df_weather.drop('weather_clean')
# display(df_weather)

# COMMAND ----------

df_weather = df_weather.withColumnRenamed("Year", "yr") \
                       .withColumnRenamed("Month", "mnth") \
                        .withColumnRenamed("Day", "start_day") 

# COMMAND ----------

selected_columns_df = df_weather.select('dteday', 'hr', 'weathersit', 'temp', 'atemp', 'hum', 'windspeed', 'yr', 'mnth', 'start_day')
# display(selected_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### prepare final df

# COMMAND ----------

df_combined = df_pivot.join(selected_columns_df, on=['yr', 'mnth', 'start_day', 'hr'], how='inner')
# display(df_combined)

# COMMAND ----------

df_combined.coalesce(1).write.mode("overwrite") \
                        .csv(f"{data_dir}/css/output/tmp", header=True)



# COMMAND ----------

import os
import shutil

# Get a list of all CSV files in the directory
csv_files = [f for f in os.listdir(f"{data_dir}/css/output/tmp") if f.endswith('.csv')]

# Move the first file to the desired location
if csv_files:
    shutil.move(f"{data_dir}/css/output/tmp/{csv_files[0]}", f"{data_dir}/css/output/hour.csv")