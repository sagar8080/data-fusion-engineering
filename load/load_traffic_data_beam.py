import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions, WorkerOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.pvalue import TaggedOutput
from google.cloud import storage


PROCESSED_FILES = list()

# Define the schema based on your requirements
schema = {
    "fields": [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "speed", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "travel_time", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "status", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "data_as_of", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "link_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "link_points", "type": "STRING", "mode": "NULLABLE"},
        {"name": "encoded_poly_line", "type": "STRING", "mode": "NULLABLE"},
        {"name": "encoded_poly_line_lvls", "type": "STRING", "mode": "NULLABLE"},
        {"name": "owner", "type": "STRING", "mode": "NULLABLE"},
        {"name": "transcom_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "borough", "type": "STRING", "mode": "NULLABLE"},
        {"name": "link_name", "type": "STRING", "mode": "NULLABLE"}
    ]
}

def get_config():
    with open("utils/config.json", "r") as f:
        return json.load(f)
    
def move_gcs_file(input_filepath):
    if not input_filepath.startswith("gs://"):
        raise ValueError("The input filepath must start with 'gs://'")
    parts = input_filepath[5:].split('/')
    bucket_name = parts[0]
    blob_name = '/'.join(parts[1:])
    if 'pre-processed' in blob_name:
        destination_blob_name = blob_name.replace('pre-processed', 'processed')
    else:
        raise ValueError("The input filepath does not contain 'pre-processed'")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    bucket.copy_blob(source_blob, bucket, destination_blob_name)
    print(f"Moved source file: {source_blob} to target file: {destination_blob_name}")
    source_blob.delete()

class ParseJsonFn(beam.DoFn):
    def process(self, element):
        import json
        file_path = element[0]
        data = element[1]
        record = json.loads(data)
        for r in record:
            yield r
        yield TaggedOutput('successful_file', file_path)

class StreamJsonDoFn(beam.DoFn):
    def process(self, element):
        # element is the file path in GCS
        import ijson
        from apache_beam.io.gcp.gcsio import GcsIO
        gcs_io = GcsIO()
        
        filepath = element[1]
        # Open the file stream from GCS
        with gcs_io.open(filepath) as file_stream:
            # Use ijson to parse JSON file as a stream
            parser = ijson.items(file_stream, 'item')
            for record in parser:
                yield record
        yield TaggedOutput('successful_file', element[0])
    
class ParseFileName(beam.DoFn):
    def process(self, element):
        yield element[0]



def setup_logger():
    logger = logging.getLogger('Load Traffic Data')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def get_files_to_delete(file_path):
    PROCESSED_FILES.append(file_path)


def setup_parser():
    parser = argparse.ArgumentParser(description="Apache Beam script to process JSON data and load into BigQuery.")
    parser.add_argument("--project", dest="project", help="GCP project ID.", default="")
    parser.add_argument("--region", dest="region", help="GCP region.", default="us-east4")
    parser.add_argument("--proc-name", dest="proc_name", help="Process Name executing.", default="traffic_data")
    parser.add_argument("--job-name", dest="job_name", help="Name of the Dataflow job.", default="load-traffic-data-from-gcs-to-bq")
    parser.add_argument("--runner", dest="runner", help="Pipeline runner", default="DirectRunner", choices=["DirectRunner", "DataflowRunner"])
    parser.add_argument("--staging-location", dest="staging_location", help="GCS path for staging.", default="gs://df-utility/dataflow/staging")
    parser.add_argument("--temp-location", dest="temp_location", help="GCS path for temp files.", default="gs://df-utility/dataflow/temp")
    parser.add_argument("--max-num-workers", dest="max_num_workers", type=int, help="Maximum number of workers.", default=3)
    parser.add_argument("--input-pattern", dest="input_pattern", help="GCS path pattern for input files.", default="gs://df-landing-zone/data/pre-processed/traffic_data/*.json")
    return parser


logger = setup_logger()
parser = setup_parser()
args = parser.parse_args()
logger.info("Starting the Apache Beam Pipeline")
proc_name = args.proc_name
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

with beam.Pipeline(options=pipeline_options) as p:
    json_data = p | "Read JSON files" >> beam.io.ReadFromTextWithFilename(input_pattern)
    parse_result = json_data | 'Parse JSON' >> beam.ParDo(StreamJsonDoFn()).with_outputs('successful_file', main='parsed_data')
    parsed_data = parse_result.parsed_data
    parsed_data | "print parsed data" >> beam.Map(print)
    successful_file = parse_result.successful_file
    successful_file | "successfully parsed file" >> beam.Map(print)
    parsed_data | 'Write to BigQuery' >> WriteToBigQuery(
        "df_raw.df_traffic_data_raw",
        schema=schema,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=WriteToBigQuery.Method.STREAMING_INSERTS
    )
    successful_file | "Collect Filenames" >> beam.Map(get_files_to_delete)

print(f"The following files were processed: {PROCESSED_FILES}")
for file in PROCESSED_FILES:
    move_gcs_file(file)


logger.info("Completed processing.")