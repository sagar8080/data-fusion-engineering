import requests
import json
import datetime
import pandas as pd
import urllib.parse

import functions_framework
from google.cloud import storage, bigquery

# Initialize BigQuery and Cloud Storage clients
bq_client = bigquery.Client()
storage_client = storage.Client()

# Load configuration file
f = open("config.json", "r")
config = json.loads(f.read())

# Constants for configuration and API limits
LANDING_BUCKET = config["landing_bucket"]
CATALOG_TABLE_ID = config["catalog_table"]
PROCESS_NAME = "df-ingest-vehicles-data"
LIMIT = 1000000
DAY_DELTA = 60
DEFAULT_START_DATE = "2012-07-01T00:00:00"
DEFAULT_END_DATE = "2012-07-31T23:59:59"
BASE_URL = "https://data.cityofnewyork.us/resource/bm4k-52h4.csv"
BASE_PROC_NAME = "vehicles_data"
BASE_FILE_PATH = f"data/pre-processed/{BASE_PROC_NAME}"


# Fetch the last timestamp loaded from the BigQuery catalog table
def fetch_last_offset(table_id):
    query = f"""
    SELECT last_timestamp_loaded FROM `{table_id}`
    where process_name='{PROCESS_NAME}' and process_status='Success'
    ORDER BY insert_ts desc
    LIMIT 1
    """
    results = bq_client.query(query).result()
    try:
        offset = list(results)[0]["last_timestamp_loaded"]
        return offset if offset else DEFAULT_START_DATE
    except Exception:
        return DEFAULT_START_DATE


# Get start and end dates with a delta of 60 days
def get_dates(input_date):
    if isinstance(input_date, str):
        start_date = datetime.datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S")
    elif isinstance(input_date, datetime.datetime):
        start_date = input_date
    end_date = start_date + datetime.timedelta(days=DAY_DELTA)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")
    return start_date, end_date


# Fetch data from the NYC vehicle dataset API
def fetch_data(start_date, end_date):
    try:
        params = {
            "$limit": f"{LIMIT}",
            "$where": f"crash_date >= '{start_date}' AND crash_date <= '{end_date}'",
        }
        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        api_url = f"{BASE_URL}?{encoded_params}"
        df = pd.read_csv(api_url, low_memory=False)
        return df
    except requests.RequestException as e:
        print(f"Request failed: {e}")
    return None


# Upload the fetched data to Google Cloud Storage
def upload_to_gcs(data):
    current_day = datetime.date.today()
    current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    file_path = f"{BASE_FILE_PATH}/{current_day}"
    file_name = f"{BASE_PROC_NAME}_{current_timestamp}.csv"
    try:
        data.to_csv(f"gs://{LANDING_BUCKET}/{file_path}/{file_name}", index=False)
        return "Success"
    except Exception as e:
        print(e)
        return "Failure"


# Store the state of the function execution in BigQuery
def store_func_state(table_id, state_json):
    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print(f"Insert errors: {errors}")


# Main function to execute the data fetching and storing process
@functions_framework.http
def execute(request):
    state = "started"
    try:
        start_timestamp = datetime.datetime.now()
        last_date_loaded = fetch_last_offset(CATALOG_TABLE_ID)
        start_date, end_date = get_dates(last_date_loaded)
        data = fetch_data(start_date, end_date)
        if len(data) > 0:
            try:
                state = upload_to_gcs(data)
            except Exception as e:
                print("Upload to GCS failed:", e)
                state = "Failed"
        else:
            state = "No Data Found"
        end_timestamp = datetime.datetime.now()
        time_taken = end_timestamp - start_timestamp
        function_state = {
            "process_name": PROCESS_NAME,
            "process_status": state,
            "process_start_time": start_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "process_end_time": end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "time_taken": round(time_taken.seconds, 3),
            "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_timestamp_loaded": end_date,
        }
        store_func_state(CATALOG_TABLE_ID, function_state)
        bq_client.close()
        storage_client.close()
        f.close()
    except Exception as e:
        print(e)
    return state
