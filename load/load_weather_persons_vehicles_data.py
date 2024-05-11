import json
from argparse import ArgumentParser
from google.cloud import bigquery

parser = ArgumentParser(description="Load data to BQ")
parser.add_argument("--process-name", dest="proc_name")

client = bigquery.Client()

def get_config():
    with open("utils/config.json", "r") as f:
        return json.load(f)

def load_data_to_bigquery(proc_name, landing_bucket, config):
    uri = f"gs://{landing_bucket}/data/pre-processed/{proc_name}/*.csv"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )
    table_id = config.get("raw_tables").get(proc_name)
    try:
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()
        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")


if __name__ == "__main__":    
    args = parser.parse_args()
    proc_name = args.proc_name
    config = get_config()
    landing_bucket = config.get("landing_bucket", None)
    load_data_to_bigquery(proc_name, landing_bucket, config)