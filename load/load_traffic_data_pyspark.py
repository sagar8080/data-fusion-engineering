import json
import datetime
from argparse import ArgumentParser
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

parser = ArgumentParser(description="Arg parser for this dataproc job")
parser.add_argument("--batch-size", type=int, dest="batch_size", default=10)
parser.add_argument("--prefix-path", type=str, dest="prefix_path")


def create_spark_session(config):
    """
    Create and configure a Spark session.

    Args:
        config (dict): Configuration parameters.

    Returns:
        spark: SparkSession.
    """
    spark = SparkSession.builder\
    .appName("landing_to_raw")\
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.1")\
    .config('temporaryGcsBucket', config.get('util_bucket'))\
    .getOrCreate()
    return spark

def get_config():
    """
    Load configuration parameters from a JSON file.

    Returns:
        dict: Configuration parameters.
    """
    with open('config.json', 'r') as config_file:
        return json.load(config_file)

def list_and_batch_gcs_files(client, bucket_name, prefix, max_batch_size_gb=5):
    """
    List files in a Google Cloud Storage bucket and batch them based on size.

    Args:
        client: Google Cloud Storage client.
        bucket_name (str): Name of the GCS bucket.
        prefix (str): Prefix for filtering files.
        max_batch_size_gb (int): Maximum batch size in gigabytes.

    Yields:
        list: List of file paths in each batch.
    """
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

    for blob in blobs:
        if blob.name.endswith('/'):
            continue

        blob_size = blob.size
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
    """
    Generate a file path based on the stage of processing.

    Args:
        bucket_name (str): Name of the GCS bucket.
        proc_name (str): Name of the process.
        stage (str): Stage of processing.

    Returns:
        str: Generated file path.
    """
    base_path = f"gs://{bucket_name}/data/{stage}/{proc_name}/"
    if stage == 'processed':
        current_day = datetime.date.today().strftime("%Y-%m-%d")
        current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f"{proc_name}_{current_timestamp}.parquet"
        return f"{base_path}{current_day}/{file_name}"
    return base_path

def read_batch(spark, file_paths):
    """
    Read a batch of JSON files into a Spark DataFrame.

    Args:
        spark: SparkSession.
        file_paths (list): List of file paths.

    Returns:
        dataframe: Spark DataFrame containing the data.
    """
    df = spark.read.json(file_paths)
    return df

def move_gcs_files(client, batch, bucket_name):
    """
    Move a batch of files from one location to another within the same bucket.

    Args:
        client: Google Cloud Storage client.
        batch (list): List of file paths.
        bucket_name (str): Name of the GCS bucket.
    """
    bucket = client.bucket(bucket_name)
    for input_filepath in batch:
        parts = input_filepath[5:].split('/')
        blob_name = '/'.join(parts[1:])
        destination_blob_name = blob_name.replace('pre-processed', 'processed')
        source_blob = bucket.blob(blob_name)
        bucket.copy_blob(source_blob, bucket, destination_blob_name)
        source_blob.delete()

def write_data_to_bigquery(dataframe, table_name):
    """
    Write data from a Spark DataFrame to BigQuery.

    Args:
        dataframe: Spark DataFrame containing the data.
        table_name (str): Name of the BigQuery table.
    """
    dataframe.write.format('bigquery') \
        .option('table', table_name) \
        .mode('append') \
        .save()
     
        
def store_func_state(bq_client, table_id, state_json):
    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
        

def main(process_name, config, prefix, batch_size):
    """
    Main function to process files, transform data, and load it into BigQuery.

    Args:
        process_name (str): Name of the process.
        config (dict): Configuration parameters.
        prefix (str): Prefix for filtering files.
        batch_size (int): Number of files to process in each batch.
    """
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
    start_timestamp = datetime.datetime.now()
    bq_client = bigquery.Client()
    state = "in-progress"
    try:
        args = parser.parse_args()
        proc_name = "traffic-data"
        batch_size = int(args.batch_size)   
        prefix_path = args.prefix_path
        if prefix_path.lower() == "all" or not prefix_path:
            prefix_path = None
        config = get_config()
        count = main(proc_name, config, batch_size=batch_size, prefix=prefix_path)
        state = "Success"
    except Exception as e:
        state = "Failure"
    end_timestamp = datetime.datetime.now()
    time_taken = end_timestamp - start_timestamp
    function_state = {
        "process_name": f"load-{proc_name}",
        "process_status": state,
        "process_start_time": start_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "process_end_time": end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "time_taken": round(time_taken.seconds, 3),
        "rows_processed":  count,
        "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    CATALOG_TABLE_ID = config["catalog_table"]
    store_func_state(bq_client, CATALOG_TABLE_ID, function_state)
    bq_client.close()