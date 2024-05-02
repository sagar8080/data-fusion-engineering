import datetime
import requests
import traceback
import json
from dateutil.relativedelta import relativedelta

import functions_framework
from google.cloud import storage, bigquery


bq_client = bigquery.Client()
storage_client = storage.Client()
f = open("config.json", "r")
config = json.loads(f.read())
DEFAULT_START_DATE = '2015-01-01T00:00:00'
PROCESS_NAME = "df-ingest-taxi-data"
LANDING_BUCKET = config["landing_bucket"]
CATALOG_TABLE_ID = config["catalog_table"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def fetch_last_date(table_id):
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
    

def get_start_end_date(input_date):
    if isinstance(input_date, str):
        start_date = datetime.datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S")
    elif isinstance(input_date, datetime.datetime):
        start_date = input_date
    end_date = start_date + relativedelta(months=+1)
    start_date = start_date.strftime("%Y-%m")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")
    return start_date, end_date


def generate_file_url(date):
    return f"{BASE_URL}/yellow_tripdata_{date}.parquet"


def upload_to_gcs(file_url):
    current_day = datetime.date.today()
    file_path = f"data/pre-processed/taxi_data/{current_day}"
    try:
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            file_name = file_url.split('/')[-1]
            bucket = storage_client.bucket(LANDING_BUCKET)
            blob = bucket.blob(f"{file_path}/{file_name}")
            blob.upload_from_string(response.content, content_type='application/octet-stream')
            print(f"File {file_name} uploaded successfully to GCS.")
    except Exception as e:
        print(e)


def store_func_state(table_id, state_json):
    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


@functions_framework.http
def execute(request):
    state = "Started"
    try:
        start_timestamp = datetime.datetime.now()
        last_date_loaded = fetch_last_date(CATALOG_TABLE_ID)
        start_date, end_date = get_start_end_date(last_date_loaded)
        file_url = generate_file_url(start_date)
        if file_url:
            try:
                upload_to_gcs(file_url=file_url)
                state = "Success"
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
        traceback.print_exc()
    return state