import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, dayofweek, hour, from_unixtime, unix_timestamp, sum as sql_sum


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

# Read data from BigQuery
raw_table = config.get("raw_tables").get("traffic_data")
df = spark.read.format("bigquery") \
    .option("table", raw_table) \
    .load()

# Cast data types
df = df.withColumn("crash_timestamp", to_date(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
df = df.withColumn("speed", col("speed").cast("double"))
df = df.withColumn("travel_time", col("travel_time").cast("integer"))
df = df.filter(col("crash_timestamp").isNotNull())

# Extract year, month, and day from the date
df = df.withColumn("year", year("crash_timestamp")) \
    .withColumn("month", month("crash_timestamp")) \
    .withColumn("day_of_week", dayofweek("crash_timestamp")) \
    .withColumn("hour", hour("crash_timestamp"))\
    .withColumn("year_month_ts", date_format(col("crash_timestamp"), "yyyy-MM-01"))

# Transformations for analysis
speed_stats = df.select(
    mean(col("speed")).alias("mean_speed"),
    stddev(col("speed")).alias("stddev_speed")
).collect()
mean_speed = speed_stats[0]["mean_speed"]
stddev_speed = speed_stats[0]["stddev_speed"]

df = df.withColumn(
    "speed_band",
    when(col("speed") < mean_speed - stddev_speed, "Low").when(
        col("speed") < mean_speed + stddev_speed, "Average").otherwise("High")
)

# Travel Time Variability
travel_time_stats = df.select(
    mean(col("travel_time")).alias("mean_travel_time"),
    stddev(col("travel_time")).alias("stddev_travel_time")
).collect()
mean_travel_time = travel_time_stats[0]["mean_travel_time"]
stddev_travel_time = travel_time_stats[0]["stddev_travel_time"]

df = df.withColumn(
    "travel_time_category",
    when(col("travel_time") < mean_travel_time - stddev_travel_time, "Quick").when(
        col("travel_time") > mean_travel_time + stddev_travel_time, "Slow").otherwise("Normal")
)

# Time Slot Analysis
df = df.withColumn(
    "time_slot",
    when(col("hour").between(0, 5), "Late Night").when(col("hour").between(6, 11), "Morning").when(
        col("hour").between(12, 17), "Afternoon").otherwise("Evening")
)

# Weekend or Weekday
df = df.withColumn(
    "week_part",
    when(col("day_of_week").isin(1, 7), "Weekend").otherwise("Weekday")
)

df_transformed = df.withColumn("year_month_ts", to_date(col("year_month_ts")))
df_transformed = replace_nulls(df_transformed)
df_transformed.show(10)

# Write the transformed data to BigQuery, partitioned by year and month
prod_table = config.get("prod_tables").get("traffic_data")
df_transformed.write.format("bigquery") \
    .option("table", prod_table) \
    .option("temporaryGcsBucket", config.get("util_bucket")) \
    .option("partitionField", "year_month_ts") \
    .option("partitionType", "MONTH") \
    .mode("overwrite") \
    .save()

# Stop the Spark session
spark.stop()