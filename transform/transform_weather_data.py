import json
import os
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, when, sum as sql_sum


def replace_nulls(df):
    string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    filled_df = df.fillna('Unknown', subset=string_columns)
    return filled_df

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

spark = SparkSession.builder \
    .appName("Read BigQuery Table") \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.0') \
    .config('temporaryGcsBucket', config.get('util_bucket'))\
    .getOrCreate()

raw_table = config.get("raw_tables").get("weather_data")
df = spark.read.format("bigquery") \
    .option("table", raw_table) \
    .load()

important_columns = [
    "date", "elevation", "burrough", "lattitude", "longitude", "temperature_2m",
    "relative_humidity_2m", "pressure_msl", "wind_speed_10m", "sunshine_duration"
]
df_selected = df.select(*important_columns)
df_filtered = df_selected.filter(col("date").isNotNull())
df_transformed = df_filtered.withColumn("date", to_date("date")) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date"))

df_transformed = df_transformed.withColumn(
    "temperature_category", when(col("temperature_2m") < 0, "Below Freezing").otherwise("Above Freezing")
).withColumn(
    "humidity_range", when(col("relative_humidity_2m") < 50, "Low Humidity").otherwise("High Humidity")
).withColumn(
    "pressure_condition", when(col("pressure_msl") > 1013.25, "High Pressure").otherwise("Low Pressure")
).withColumn(
    "wind_intensity", when(col("wind_speed_10m") > 20, "High Wind").otherwise("Low Wind")
)

df_transformed = df_transformed.withColumn("year_month_ts", to_date(col("year")))
df_transformed = replace_nulls(df_transformed)
df_transformed.show(10)

prod_table = config.get("prod_tables").get("weather_data")
df_transformed.write.format("bigquery") \
    .option("table", prod_table) \
    .option("temporaryGcsBucket", config.get("util_bucket")) \
    .option("partitionField", "date") \
    .option("partitionType", "MONTH") \
    .mode("overwrite") \
    .save()

spark.stop()