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

raw_table = config.get("raw_tables").get("vehicles_data")
df = spark.read.format("bigquery") \
    .option("table", raw_table) \
    .load()

# Select important columns
df_selected = df.select(
    "collision_id", "crash_date", "crash_time", "state_registration", "vehicle_year",
    "vehicle_type", "pre_crash", "contributing_factor_1", "vehicle_damage"
)

# Drop rows where crash_date is null
df_filtered = df_selected.filter(col("crash_date").isNotNull())

# Extract year and month from the crash_date
df_transformed = df_filtered.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd"))
df_transformed = df_transformed.withColumn("year", year(col("crash_date")))\
                               .withColumn("month", month(col("crash_date")))

# Apply transformations
df_transformed = df_transformed.withColumn(
    "part_of_day",
    when(hour("crash_time") < 6, "Night").when(hour("crash_time") < 12, "Morning").when(hour("crash_time") < 18, "Afternoon").otherwise("Evening")
).withColumn(
    "weekend",
    when(dayofweek(from_unixtime(unix_timestamp(col("crash_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS z"))).isin([1, 7]), "Yes").otherwise("No")
).withColumn(
    "vehicle_age_category",
    when(col("vehicle_year").isNull(), "Unknown")
    .when(year(current_date()) - col("vehicle_year") < 5, "0-4 years")
    .when(year(current_date()) - col("vehicle_year") < 10, "5-9 years")
    .otherwise("10+ years")
).withColumn(
    "high_impact_crash",
    when(col("contributing_factor_1").isin(["Lost Consciousness", "Driver Inattention/Distraction"]), "Yes").otherwise("No")
)

df_transformed = df_transformed.withColumn("year_month_ts", to_date(col("year")))
df_transformed = df_transformed.withColumnRenamed("contributing_factor_1", "contributing_factor")
df_transformed = replace_nulls(df_transformed)
df_transformed.show(10)

prod_table = config.get("prod_tables").get("vehicles_data")
df_transformed.write.format("bigquery") \
    .option("table", prod_table) \
    .option("temporaryGcsBucket", config.get("util_bucket")) \
    .option("partitionField", "year_month_ts") \
    .option("partitionType", "DAY") \
    .mode("overwrite") \
    .save()

spark.stop()
