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


df = spark.read.format("bigquery") \
    .option("table", "df_raw.df_crashes_data_raw") \
    .load()


selected_columns = df.select(
    "crash_date", "crash_time", "borough", "zip_code",
    "latitude", "longitude", "number_of_persons_injured",
    "number_of_persons_killed", "number_of_pedestrians_injured",
    "number_of_cyclist_injured", "number_of_motorist_injured",
    "contributing_factor_vehicle_1", "vehicle_type_code1", "vehicle_type_code2", "collision_id"
)

cleaned_data = selected_columns.filter(col("zip_code").isNotNull()).filter(col("crash_date").isNotNull())
cleaned_data = cleaned_data.withColumn("crash_timestamp", from_unixtime(unix_timestamp(col("crash_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS z")))

cleaned_data = cleaned_data\
    .withColumn("year", year(col("crash_timestamp"))) \
    .withColumn("month", month(col("crash_timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("crash_timestamp")))\
    .withColumn("hour_of_day", hour(col("crash_timestamp")))

cleaned_data = cleaned_data.withColumn(
    "year_month_ts", to_date(col("year")))

cleaned_data = cleaned_data.withColumn("was_fatal", when(col("number_of_persons_killed") > 0, True).otherwise(False))

for vehicle_col in ["vehicle_type_code1", "vehicle_type_code2"]:
    cleaned_data = cleaned_data.withColumn(
        vehicle_col + "_involved", 
        when(col(vehicle_col).isNotNull(), 1).otherwise(0)
    )

cleaned_data = cleaned_data.withColumn(
    "total_vehicles_involved", col("vehicle_type_code1_involved") + col("vehicle_type_code2_involved")
)
df_transformed = replace_nulls(cleaned_data)
df_transformed.show(10)
# Write the data to a new BigQuery dataset, partitioned by year and month
prod_table = config.get("prod_tables").get("crashes_data")
df_transformed.write.format("bigquery") \
    .option("table", prod_table) \
    .option('temporaryGcsBucket', config.get('util_bucket'))\
    .option("partitionField", "year_month_ts") \
    .option("partitionType", "DAY")\
    .mode("overwrite") \
    .save()


spark.stop()
