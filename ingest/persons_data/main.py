import requests
import json
import datetime

import functions_framework
from google.cloud import storage, bigquery


bq_client = bigquery.Client()
storage_client = storage.Client()
f= open("config.json", "r")
config = json.loads(f.read())
LANDING_BUCKET = config["landing_bucket"]
CATALOG_TABLE_ID = config["catalog_table"]
LIMIT = 200000
PROCESS_NAME = "df-ingest-person-data"


def fetch_last_offset(table_id):
    """
    Fetches the last offset from BIGQUERY dataset which the data
    """
    query = f"""
    SELECT last_offset_fetched FROM `{table_id}`
    where process_name='{PROCESS_NAME}' and process_status='Success'
    ORDER BY insert_ts desc
    LIMIT 1
    """
    results = bq_client.query(query).result()
    try:
        offset = int(list(results)[0]["last_offset_fetched"])
        return offset
    except IndexError:
        return 0


def fetch_data(last_offset):
    base_url = "https://data.cityofnewyork.us/resource/f55k-p6yu.json"
    api_url = base_url + f"?$order=crash_date,crash_time&$limit={LIMIT}&$offset={last_offset}"
    response = requests.get(api_url)
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except Exception as e:
            print(e)
            return None
    else:
        return None


def upload_to_gcs(data):
    current_day = datetime.date.today()
    current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    file_path = f"data/pre-processed/persons_data/{current_day}"
    file_name = f"persons_data_{current_timestamp}.json"
    try:
        data_bytes = bytes(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(f"{file_path}/{file_name}")
        blob.upload_from_string(data_bytes, content_type="application/json")
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
    try:
        start_timestamp = datetime.datetime.now()
        last_offset = fetch_last_offset(CATALOG_TABLE_ID)
        data = fetch_data(last_offset)
        state = "Started"
        if data:
            try:
                upload_to_gcs(data)
                state = "Success"
            except Exception as e:
                print(e)
                state = "Failed"
        else:
            state = "Error"
        end_timestamp = datetime.datetime.now()
        time_taken = end_timestamp - start_timestamp
        function_state = {
            "process_name": PROCESS_NAME,
            "process_status": state,
            "process_start_time": start_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "process_end_time": end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "time_taken": round(time_taken.seconds, 3),
            "last_offset_fetched": last_offset + LIMIT,
            "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        store_func_state(CATALOG_TABLE_ID, function_state)
    except Exception as e:
        print(e)
    return state
