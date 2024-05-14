import json
import os
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, StringType

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

# Load configuration settings from the config file
with open("config.json", "r") as config_file:
    config = json.load(config_file)

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

# Load raw weather data table from BigQuery
raw_table = config.get("raw_tables").get("weather_data")
df = spark.read.format("bigquery").option("table", raw_table).load()

# Select important columns from the DataFrame
important_columns = [
    "date",
    "elevation",
    "burrough",
    "lattitude",
    "longitude",
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "rain",
    "snowfall",
    "snow_depth",
    "pressure_msl",
    "wind_speed_10m",
    "sunshine_duration",
    "cloud_cover",
    "wind_gusts_10m",
]
df_selected = df.select(*important_columns)

# Filter rows where 'date' column is not null
df_filtered = df_selected.filter(col("date").isNotNull())

# Transform the DataFrame by renaming and creating new columns
df_transformed = df_filtered.withColumnRenamed("date", "weather_timestamp")\
                            .withColumn("weather_timestamp", to_timestamp("weather_timestamp"))\
                            .withColumn("year", year("weather_timestamp"))\
                            .withColumn("month", month("weather_timestamp"))\
                            .withColumn("day", day(col("weather_timestamp")))\
                            .withColumn("hour_of_day", hour(col("weather_timestamp")))

# Calculate statistical measures for weather parameters
stats = df_transformed.select(
    mean(col("rain")).alias("mean_rain"),
    stddev(col("rain")).alias("stddev_rain"),
    mean(col("snowfall")).alias("mean_snowfall"),
    stddev(col("snowfall")).alias("stddev_snowfall"),
    mean(col("wind_gusts_10m")).alias("mean_wind_gusts"),
    stddev(col("wind_gusts_10m")).alias("stddev_wind_gusts"),
).collect()[0]

mean_rain, stddev_rain = stats.mean_rain, stats.stddev_rain
mean_snowfall, stddev_snowfall = stats.mean_snowfall, stats.stddev_snowfall
mean_wind_gusts, stddev_wind_gusts = stats.mean_wind_gusts, stats.stddev_wind_gusts

# Categorize weather data based on calculated statistics
df_transformed = df_transformed.withColumn(
    "rainfall_category",
    when(col("rain") < (mean_rain - stddev_rain), "Low Rainfall")
    .when(col("rain") > (mean_rain + stddev_rain), "High Rainfall")
    .otherwise("Moderate Rainfall"),
)

df_transformed = df_transformed.withColumn(
    "snowfall_category",
    when(col("snowfall") < (mean_snowfall - stddev_snowfall), "Low Snowfall")
    .when(col("snowfall") > (mean_snowfall + stddev_snowfall), "High Snowfall")
    .otherwise("Moderate Snowfall"),
)

df_transformed = df_transformed.withColumn(
    "wind_gusts_category",
    when(col("wind_gusts_10m") < (mean_wind_gusts - stddev_wind_gusts), "Low Gusts")
    .when(col("wind_gusts_10m") > (mean_wind_gusts + stddev_wind_gusts), "High Gusts")
    .otherwise("Moderate Gusts"),
)

# Round off latitude and longitude values
df_transformed = df_transformed.withColumn(
    "lattitude", round(col("lattitude").cast("float"), 4)
).withColumn("longitude", round(col("longitude").cast("float"), 4))

# Extract numeric values from elevation column and cast to float
df_transformed = df_transformed.withColumn(
    "elevation_m", regexp_extract(col("elevation"), "\d+\.?\d*", 0).cast("float")
)

# Format temperature values to two decimal places
df_transformed = df_transformed.withColumn(
    "temperature_2m", format_number(col("temperature_2m").cast("float"), 2)
)

# Add weather categories based on various conditions
df_transformed = (
    df_transformed.withColumn(
        "temperature_category",
        when(col("temperature_2m") < 0, "Below Freezing").otherwise("Above Freezing"),
    )
    .withColumn(
        "humidity_range",
        when(col("relative_humidity_2m") < 50, "Low Humidity").otherwise(
            "High Humidity"
        ),
    )
    .withColumn(
        "pressure_condition",
        when(col("pressure_msl") > 1013.25, "High Pressure").otherwise("Low Pressure"),
    )
    .withColumn(
        "wind_intensity",
        when(col("wind_speed_10m") > 20, "High Wind").otherwise("Low Wind"),
    )
)

# Add a column for year-month timestamp
df_transformed = df_transformed.withColumn("year_month_ts", to_date(col("year")))

# Replace null values in string columns
df_transformed = replace_nulls(df_transformed)

# Write the transformed DataFrame back to BigQuery
prod_table = config.get("prod_tables").get("weather_data")
df_transformed.write.format("bigquery") \
    .option("table", prod_table) \
    .option("temporaryGcsBucket", config.get("util_bucket")) \
    .mode("overwrite") \
    .save()

# Stop the Spark session
spark.stop()
