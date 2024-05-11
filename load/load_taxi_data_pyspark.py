import json
import datetime
from argparse import ArgumentParser
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = ArgumentParser(description="Arg parser for this dataproc job")
parser.add_argument("--batch-size", type=int, dest="batch_size", default=10)
parser.add_argument("--prefix-path", type=str, dest="prefix_path")

def create_spark_session(config):
    """Create and configure a Spark session."""
    spark = SparkSession.builder\
    .appName("landing_to_raw")\
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.1")\
    .config('temporaryGcsBucket', config.get('util_bucket'))\
    .getOrCreate()
    return spark

def get_config():
    with open('config.json', 'r') as config_file:
        return json.load(config_file)

def list_and_batch_gcs_files(client, bucket_name, prefix, max_batch_size_gb=5):
    max_batch_size_bytes = max_batch_size_gb * 1024 ** 3
    
    try:
        bucket = client.get_bucket(bucket_name)
    except Exception as e:
        print(f"Error accessing bucket: {str(e)}")
        return
    current_batch = []
    current_batch_size = 0
    try:
        blobs = bucket.list_blobs(prefix=prefix)
    except Exception as e:
        print(f"Error listing blobs: {str(e)}")
        return

    # Iterate over each blob
    for blob in blobs:
        if blob.name.endswith('/'):
            continue

        blob_size = blob.size
        # Check if adding this file would exceed the max batch size
        if current_batch_size + blob_size > max_batch_size_bytes:
            if current_batch:
                yield current_batch
            current_batch = []
            current_batch_size = 0
        current_batch.append(blob.name)
        current_batch_size += blob_size

    if current_batch:
        yield current_batch

def generate_file_path(bucket_name, proc_name, stage):
    base_path = f"gs://{bucket_name}/data/{stage}/{proc_name}/"
    if stage == 'processed':
        current_day = datetime.date.today().strftime("%Y-%m-%d")
        current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f"{proc_name}_{current_timestamp}.parquet"
        return f"{base_path}{current_day}/{file_name}"
    return base_path

def cast_dataframe_types(dataframe):
    cast_dict = {
        'VendorID': 'integer',
        'tpep_pickup_datetime': 'timestamp',
        'tpep_dropoff_datetime': 'timestamp',
        'passenger_count': 'integer',
        'trip_distance': 'float',
        'RatecodeID': 'integer',
        'store_and_fwd_flag': 'string',
        'PULocationID': 'integer',
        'DOLocationID': 'integer',
        'payment_type': 'integer',
        'fare_amount': 'float',
        'extra': 'float',
        'mta_tax': 'float',
        'tip_amount': 'float',
        'tolls_amount': 'float',
        'improvement_surcharge': 'float',
        'total_amount': 'float',
        'congestion_surcharge': 'integer',
        'airport_fee': 'integer'
    }

    for column, dtype in cast_dict.items():
        if column in dataframe.columns:
            dataframe = dataframe.withColumn(column, col(column).cast(dtype))
        else:
            print(f"Column {column} not found in DataFrame.")
    return dataframe

def read_batch(spark, file_paths):
    df = spark.read.parquet(file_paths)
    df = cast_dataframe_types(df)
    return df

def move_gcs_files(client, batch, bucket_name):
    bucket = client.bucket(bucket_name)
    for input_filepath in batch:
        parts = input_filepath[5:].split('/')
        blob_name = '/'.join(parts[1:])
        destination_blob_name = blob_name.replace('pre-processed', 'processed')
        source_blob = bucket.blob(blob_name)
        bucket.copy_blob(source_blob, bucket, destination_blob_name)
        source_blob.delete()

def write_data_to_bigquery(dataframe, table_name):
    dataframe.write.format('bigquery') \
        .option('table', table_name) \
        .mode('append') \
        .save()

def main(process_name, config, prefix, batch_size=5):
    spark = create_spark_session(config)
    client = storage.Client()
    bucket_name = config["landing_bucket"]
    try:
        file_batches = list_and_batch_gcs_files(client, bucket_name, prefix, batch_size)
        for batch in file_batches:
            batch_paths = [f"gs://{bucket_name}/{file_name}" for file_name in batch]
            print(f"Currently processing {process_name} and batch: {batch}")
            df = read_batch(spark, batch_paths)
            df.show(10)
            processed_file_path = generate_file_path(bucket_name, process_name, "processed")
            df.coalesce(1).write.parquet(processed_file_path)
            table_name = config["raw_tables"][process_name]
            write_data_to_bigquery(df, table_name)
            move_gcs_files(client, batch_paths, bucket_name)
    except Exception as e:
        print(f"Error processing {process_name}: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    args = parser.parse_args()
    proc_name = "taxi_data"
    batch_size = int(args.batch_size)   
    prefix_path = args.prefix_path
    if prefix_path.lower() == "all" or not prefix_path:
        prefix_path = None
    config = get_config()
    main(proc_name, config, batch_size=batch_size, prefix=prefix_path)
