import json
import os
from google.cloud import storage
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

raw_table = config.get("raw_tables").get("persons_data")

df = spark.read.format("bigquery") \
    .option("table", raw_table) \
    .load()

important_columns = [
    "collision_id", "crash_date", "crash_time", "person_type", "person_injury",
    "vehicle_id", "person_age", "ejection", "emotional_status", "bodily_injury",
    "position_in_vehicle", "safety_equipment", "contributing_factor_1", "contributing_factor_2",
    "person_sex"
]

df_selected = df.select(*important_columns)

# Filter out records where crash_date is null
df_filtered = df_selected.filter(col("crash_date").isNotNull())

# Convert crash_date to date type and extract year and month
df_transformed = df_filtered.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS z")) \
    .withColumn("year", year(col("crash_date"))) \
    .withColumn("month", month(col("crash_date")))

df_transformed = df_transformed.withColumn(
    "part_of_day",
    when(hour("crash_time") < 6, "Night")
    .when(hour("crash_time") < 12, "Morning")
    .when(hour("crash_time") < 18, "Afternoon")
    .otherwise("Evening")
)

df_transformed = df_transformed.withColumn(
    "weekday_weekend",
    when(date_format(col("crash_date"), 'E').isin(["Sat", "Sun"]), "Weekend")
    .otherwise("Weekday")
)

df_transformed = df_transformed.withColumn(
    "age_category",
    when(col("person_age").isNull(), "Unknown")
    .when(col("person_age") < 18, "<18")
    .when(col("person_age") < 25, "18-24")
    .when(col("person_age") < 35, "25-34")
    .when(col("person_age") < 45, "35-44")
    .when(col("person_age") < 55, "45-54")
    .when(col("person_age") < 65, "55-64")
    .otherwise("65+")
)

df_transformed = df_transformed.withColumn(
    "injury_severity",
    when(col("person_injury") == "Killed", 5)
    .when(col("person_injury").like("%Injured%"), 3)
    .when(col("person_injury") == "Unspecified", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn("year_month_ts", to_date(col("year")))
df_transformed = replace_nulls(df_transformed)
df_transformed.show(10)

# Write the transformed data to BigQuery, partitioned by year and month
prod_table = config.get("prod_tables").get("persons_data")
df_transformed.write.format("bigquery") \
    .option("table", prod_table) \
    .option("temporaryGcsBucket", config.get("util_bucket")) \
    .option("partitionField", "year_month_ts") \
    .option("partitionType", "DAY") \
    .mode("overwrite") \
    .save()

spark.stop()