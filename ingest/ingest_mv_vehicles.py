import requests
import json
import datetime

import functions_framework
from google.cloud import storage, bigquery


bq_client = bigquery.Client()
storage_client = storage.Client()
LIMIT = 200000
PROCESS_NAME = "mv-collisions-vehicles-ingest"


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


def fetch_last_offset(table_id):
    query = f"""
    SELECT last_offset_fetched FROM `{table_id}`
    where process_name='{PROCESS_NAME}' and process_status='Success'
    ORDER BY insert_ts desc
    LIMIT 1
    """
    results = bq_client.query(query).result()
    offset = int(list(results)[0]["last_offset_fetched"])
    return offset


def fetch_data(last_offset):
    api_url = f"https://data.cityofnewyork.us/resource/bm4k-52h4.json?$limit={LIMIT}&$offset={last_offset}"
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
    file_path = f"data/pre-processed/traffic_data/{current_day}"
    file_name = f"traffic_data_{current_timestamp}.json"
    try:
        data_bytes = bytes(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        bucket = storage_client.bucket("df-landing-zone")
        blob = bucket.blob(f"{file_path}/{file_name}")
        blob.upload_from_string(data_bytes)
    except Exception as e:
        print(e)


def get_table_id():
    dataset_name = "test_dataset"
    table_name = "test_table"
    dataset_id = f"{bq_client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    table_id = f"{bq_client.project}.{dataset.dataset_id}.{table_name}"
    return table_id


def store_func_state(table_id, state_json):

    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


@functions_framework.http
def execute(request):
    start_timestamp = datetime.datetime.now()
    table_id = get_table_id()
    last_offset = fetch_last_offset(table_id)
    data = fetch_data(last_offset)
    state = None
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
    store_func_state(table_id, function_state)
