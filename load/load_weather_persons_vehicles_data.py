import json
import datetime
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
        rows = int(destination_table.num_rows)
        print("Loaded {} rows.".format(rows))
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
    finally:
        return rows
    
    
def store_func_state(bq_client, table_id, state_json):
    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    


if __name__ == "__main__":
    start_timestamp = datetime.datetime.now()
    state = "in-progress"
    rows = 0
    try:
        args = parser.parse_args()
        proc_name = args.proc_name
        config = get_config()
        landing_bucket = config.get("landing_bucket", None)
        rows += load_data_to_bigquery(proc_name, landing_bucket, config)
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
        "rows_processed":  rows,
        "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    CATALOG_TABLE_ID = config["catalog_table"]
    store_func_state(client, CATALOG_TABLE_ID, function_state)
    client.close()