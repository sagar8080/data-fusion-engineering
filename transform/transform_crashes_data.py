import json
import os
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    dayofweek,
    hour,
    from_unixtime,
    unix_timestamp,
    sum as sql_sum,
)


def replace_nulls(df):
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    filled_df = df.fillna("Unknown", subset=string_columns)
    return filled_df


with open("config.json", "r") as config_file:
    config = json.load(config_file)


spark = (
    SparkSession.builder.appName("Read BigQuery Table")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.0",
    )
    .config("temporaryGcsBucket", config.get("util_bucket"))
    .getOrCreate()
)

raw_table = "df_raw.df_crashes_data_raw"
df = spark.read.format("bigquery").option("table", raw_table).load()


df = df.select(
    "crash_date",
    "borough",
    "zip_code",
    "latitude",
    "longitude",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_cyclist_injured",
    "number_of_motorist_injured",
    "contributing_factor_vehicle_1",
    "vehicle_type_code1",
    "vehicle_type_code2",
    "collision_id",
)


cleaned_data = df.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd"))
cleaned_data = (
    cleaned_data.withColumn("year", year(col("crash_date")))
    .withColumn("month", month(col("crash_date")))
    .withColumn("day", day(col("crash_date")))
)

cleaned_data = cleaned_data.withColumn(
    "was_fatal", when(col("number_of_persons_killed") > 0, True).otherwise(False)
)

for vehicle_col in ["vehicle_type_code1", "vehicle_type_code2"]:
    cleaned_data = cleaned_data.withColumn(
        vehicle_col + "_involved", when(col(vehicle_col).isNotNull(), 1).otherwise(0)
    )

cleaned_data = cleaned_data.withColumn(
    "total_vehicles_involved",
    col("vehicle_type_code1_involved") + col("vehicle_type_code2_involved"),
)
df_transformed = replace_nulls(cleaned_data)

# Write the data to a new BigQuery dataset, partitioned by year and month
prod_table = config.get("prod_tables").get("crashes_data")
df_transformed.write.format("bigquery").option("table", prod_table).option(
    "temporaryGcsBucket", config.get("util_bucket")
).mode("overwrite").save()

spark.stop()
