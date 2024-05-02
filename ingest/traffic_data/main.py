import requests
import json
import datetime
import time

import functions_framework
from google.cloud import storage, bigquery


bq_client = bigquery.Client()
storage_client = storage.Client()
LIMIT = 10
DAY_DELTA = 30
DEFAULT_START_DATE = '2015-01-01T00:00:00'
DEFAULT_END_DATE = '2015-01-31T23:59:59'
PROCESS_NAME = "df-ingest-traffic-data"

f = open("config.json", "r")
config = json.loads(f.read('config.json'))
LANDING_BUCKET = config["landing_bucket"]
CATALOG_TABLE_ID = config["catalog_table"]


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
        return offset
    except IndexError:
        return DEFAULT_START_DATE
    

def get_dates(input_date):
    if isinstance(input_date, str):
        start_date = datetime.datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S")
        end_date = start_date + datetime.timedelta(days=DAY_DELTA)
    elif isinstance(input_date, datetime.datetime):
        end_date = input_date + datetime.timedelta(days=DAY_DELTA)
    start_date = input_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")
    return start_date, end_date


def fetch_data(start_date, end_date):
    """
    Fetches data from the API as per the limit and from the last offset fetched
    """
    api_url = f"https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit={LIMIT}&$where=data_as_of >= '{start_date}' AND data_as_of <= '{end_date}'"
    response = requests.get(api_url)
    print(response)
    if response.status_code == 200:
        try:
            data = response.json()
            print(data)
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
        bucket = storage_client.bucket(LANDING_BUCKET)
        blob = bucket.blob(f"{file_path}/{file_name}")
        blob.upload_from_string(data_bytes)
    except Exception as e:
        print(e)


def store_func_state(table_id, state_json):

    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


# @functions_framework.http
def execute(request):
    try:
        start_timestamp = datetime.datetime.now()
        last_date_loaded = fetch_last_offset(CATALOG_TABLE_ID)
        start_date, end_date = get_dates(last_date_loaded)
        data = fetch_data(start_date=start_date, end_date=end_date)
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
            "last_timestamp_loaded": end_date,
            "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        # store_func_state(CATALOG_TABLE_ID, function_state)
        bq_client.close()
        storage_client.close()
        f.close()
    except Exception as e:
        print(e.with_traceback())
    return state

execute(None)