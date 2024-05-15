import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    SetupOptions,
    WorkerOptions,
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


def get_config():
    with open("utils/config.json", "r") as f:
        return json.load(f)


def setup_logger():
    logger = logging.getLogger("Load Taxi Data")
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def setup_parser():
    parser = argparse.ArgumentParser(
        description="Apache Beam script to process JSON data and load into BigQuery."
    )
    parser.add_argument("--project", dest="project", help="GCP project ID.", default="")
    parser.add_argument(
        "--region", dest="region", help="GCP region.", default="us-east4"
    )
    parser.add_argument(
        "--job-name",
        dest="job_name",
        help="Name of the Dataflow job.",
        default="taxi-data-landing-to-raw",
    )
    parser.add_argument(
        "--runner",
        dest="runner",
        help="Pipeline runner",
        default="DirectRunner",
        choices=["DirectRunner", "DataflowRunner"],
    )
    parser.add_argument(
        "--staging-location",
        dest="staging_location",
        help="GCS path for staging.",
        default="gs://df-utility/dataflow/staging",
    )
    parser.add_argument(
        "--temp-location",
        dest="temp_location",
        help="GCS path for temp files.",
        default="gs://df-utility/dataflow/temp",
    )
    parser.add_argument(
        "--max-num-workers",
        dest="max_num_workers",
        type=int,
        help="Maximum number of workers.",
        default=3,
    )
    parser.add_argument(
        "--input-pattern",
        dest="input_pattern",
        help="GCS path pattern for input files.",
        default="gs://df-landing-zone/data/pre-processed/taxi_data/*.json",
    )
    return parser


logger = setup_logger()
parser = setup_parser()
args = parser.parse_args()
logger.info("Starting the Apache Beam Pipeline")
proc_name = "taxi_data"
pipeline_options = PipelineOptions()
input_pattern = args.input_pattern
pipeline_options.view_as(GoogleCloudOptions).project = args.project
pipeline_options.view_as(GoogleCloudOptions).region = args.region
pipeline_options.view_as(GoogleCloudOptions).staging_location = args.staging_location
pipeline_options.view_as(GoogleCloudOptions).temp_location = args.temp_location
pipeline_options.view_as(GoogleCloudOptions).job_name = args.job_name
pipeline_options.view_as(WorkerOptions).max_num_workers = args.max_num_workers
pipeline_options.view_as(WorkerOptions).autoscaling_algorithm = "THROUGHPUT_BASED"
pipeline_options.view_as(SetupOptions).save_main_session = True


def get_beam_schema():
    schema = {
        "fields": [
            {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "tpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "RatecodeID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "congestion_surcharge", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "airport_fee", "type": "INTEGER", "mode": "NULLABLE"},
        ]
    }
    return schema


def typecast_data(element):
    import datetime

    cast_functions = {
        "VendorID": int,
        "tpep_pickup_datetime": lambda x: (
            datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
            if isinstance(x, str)
            else datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S")
        ),
        "tpep_dropoff_datetime": lambda x: (
            datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
            if isinstance(x, str)
            else datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S")
        ),
        "passenger_count": int,
        "trip_distance": float,
        "RatecodeID": int,
        "store_and_fwd_flag": str,
        "PULocationID": int,
        "DOLocationID": int,
        "payment_type": int,
        "fare_amount": float,
        "extra": float,
        "mta_tax": float,
        "tip_amount": float,
        "tolls_amount": float,
        "improvement_surcharge": float,
        "total_amount": float,
        "congestion_surcharge": int,
        "airport_fee": int,
    }

    # Cast each field
    for field, cast_func in cast_functions.items():
        if field in element:
            try:
                if element[field]:
                    element[field] = cast_func(element[field])
            except ValueError:
                element[field] = element[field]
    return element


with beam.Pipeline(options=pipeline_options) as p:
    data = (
        p
        | "Read Parquet files" >> beam.io.ReadFromParquet(input_pattern)
        | "Cast data types" >> beam.Map(typecast_data)
        | "Write to BigQuery"
        >> WriteToBigQuery(
            "df_raw.df_taxi_data_raw",
            schema=None,  # Schema is inferred in this case
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )

logger.info("Completed processing.")
