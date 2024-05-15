import json
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import bigquery


bq_client = bigquery.Client()
start_timestamp = datetime.datetime.now()


def replace_nulls(df):
    """
    Replaces null values in string columns of a DataFrame with 'Unknown'.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame with possible null values.

    Returns:
        pyspark.sql.DataFrame: DataFrame with null values in string columns replaced by 'Unknown'.
    """
    # Get the names of all string columns in the DataFrame
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    # Replace null values with 'Unknown' for string columns
    filled_df = df.fillna("Unknown", subset=string_columns)
    return filled_df


def store_func_state(table_id, state_json):
    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print(f"Insert errors: {errors}")


with open("config.json", "r") as config_file:
    config = json.load(config_file)

#################################################### SPARK TRANSFORMATION ####################################################

# Initialize a SparkSession with the necessary configurations for BigQuery
spark = (
    SparkSession.builder.appName("Read BigQuery Table")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.0",
    )
    .config("temporaryGcsBucket", config.get("util_bucket"))
    .getOrCreate()
)

# Load raw vehicle data table from BigQuery
raw_table = config.get("raw_tables").get("vehicles_data")
df = spark.read.format("bigquery").option("table", raw_table).load()

# Select important columns from the DataFrame
df_selected = df.select(
    "collision_id",
    "crash_date",
    "crash_time",
    "state_registration",
    "vehicle_year",
    "vehicle_type",
    "pre_crash",
    "contributing_factor_1",
    "vehicle_damage",
)

# Drop rows where 'crash_date' column is null
df_filtered = df_selected.filter(col("crash_date").isNotNull())

# Transform the DataFrame by extracting year and month from 'crash_date'
df_transformed = df_filtered.withColumn(
    "crash_date", to_date(col("crash_date"), "yyyy-MM-dd")
)
df_transformed = (
    df_transformed.withColumn("year", year(col("crash_date")))
    .withColumn("month", month(col("crash_date")))
    .withColumn("day", day(col("crash_date")))
)

# Apply transformations to create new columns for part of day, weekend, vehicle age category, and high impact crash
df_transformed = (
    df_transformed.withColumn(
        "part_of_day",
        when(hour("crash_time") < 6, "Night")
        .when(hour("crash_time") < 12, "Morning")
        .when(hour("crash_time") < 18, "Afternoon")
        .otherwise("Evening"),
    )
    .withColumn(
        "weekend",
        when(
            dayofweek(
                from_unixtime(
                    unix_timestamp(col("crash_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS z")
                )
            ).isin([1, 7]),
            "Yes",
        ).otherwise("No"),
    )
    .withColumn(
        "vehicle_age_category",
        when(col("vehicle_year").isNull(), "Unknown")
        .when(year(current_date()) - col("vehicle_year") < 5, "0-4 years")
        .when(year(current_date()) - col("vehicle_year") < 10, "5-9 years")
        .otherwise("10+ years"),
    )
    .withColumn(
        "high_impact_crash",
        when(
            col("contributing_factor_1").isin(
                ["Lost Consciousness", "Driver Inattention/Distraction"]
            ),
            "Yes",
        ).otherwise("No"),
    )
)

# Add a column for year-month timestamp
df_transformed = df_transformed.withColumn("year_month_ts", to_date(col("year")))

# Rename column 'contributing_factor_1' to 'contributing_factor'
df_transformed = df_transformed.withColumnRenamed(
    "contributing_factor_1", "contributing_factor"
)

# Replace null values in string columns
df_transformed = replace_nulls(df_transformed)

# Show first 10 rows of the transformed DataFrame
df_transformed.show(10)
rows = df_transformed.count()

# Write the transformed DataFrame back to BigQuery
prod_table = config.get("prod_tables").get("vehicles_data")
df_transformed.write.format("bigquery").option("table", prod_table).option(
    "temporaryGcsBucket", config.get("util_bucket")
).option("partitionField", "year_month_ts").option("partitionType", "DAY").mode(
    "overwrite"
).save()

#################################################### ADD to CATALOG ####################################################
end_timestamp = datetime.datetime.now()
processed_rows = rows
time_taken = end_timestamp - start_timestamp

function_state = {
    "process_name": "transform-traffic-data",
    "process_status": "success",
    "process_start_time": start_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
    "process_end_time": end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
    "time_taken": round(time_taken.seconds, 3),
    "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "processed_rows": processed_rows,
}

catalog_table = config["catalog_table"]
store_func_state(catalog_table, function_state)

# Stop the Spark session
spark.stop()
