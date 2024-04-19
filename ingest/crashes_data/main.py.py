import requests
import json
import datetime

import functions_framework
from google.cloud import storage, bigquery


bq_client = bigquery.Client()
storage_client = storage.Client()

LIMIT = 200000
PROCESS_NAME = "df-ingest-collision-data"

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts datetime objects to their ISO format string representation."""

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)

def fetch_last_offset(table_id):
    """
    Query BigQuery to retrieve the last fetched offset for a given table.

    Args:
        table_id (str): Full identifier for the BigQuery table.

    Returns:
        int: The last offset that was successfully fetched and processed.
    """
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
    """
    Fetch data from the NYC Open Data API starting from the specified offset.

    Args:
        last_offset (int): The offset to start fetching data from.

    Returns:
        list or None: Parsed JSON data from the API response or None if an error occurs.
    """
    api_url = f"https://data.cityofnewyork.us/resource/h9gi-nx95.json?$order=collision_id&$limit={LIMIT}&$offset={last_offset}"
    response = requests.get(api_url)
    if response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            print(f"JSON error: {e}")
            return None
    else:
        print(f"HTTP error: {response.status_code}")
        return None

def upload_to_gcs(data):
    """
    Upload JSON data to a Google Cloud Storage bucket.

    Args:
        data (list): The data to be serialized to JSON and uploaded.

    Raises:
        Exception: If an error occurs during data serialization or uploading.
    """
    current_day = datetime.date.today()
    current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    file_path = f"data/pre-processed/collision_data/{current_day}"
    file_name = f"collision_data_{current_timestamp}.json"
    try:
        data_bytes = bytes(json.dumps(data, cls=DateTimeEncoder), encoding="utf-8")
        bucket = storage_client.bucket("df-landing-zone")
        blob = bucket.blob(f"{file_path}/{file_name}")
        blob.upload_from_string(data_bytes)
    except Exception as e:
        print(f"Storage error: {e}")

def get_table_id():
    """
    Construct the fully qualified BigQuery table ID using predefined dataset and table names.

    Returns:
        str: Fully qualified BigQuery table ID.
    """
    dataset_name = "accidents_dataset"
    table_name = "collision_data"
    dataset_id = f"{bq_client.project}.{dataset_name}"
    table_id = f"{dataset_id}.{table_name}"
    return table_id

def store_func_state(table_id, state_json):
    """
    Insert a new row into BigQuery to log the state of the function execution.

    Args:
        table_id (str): The BigQuery table identifier where the log will be stored.
        state_json (dict): A dictionary containing details about the function's execution state.

    Raises:
        Exception: If an error occurs during the row insertion.
    """
    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print(f"Insert errors: {errors}")

@functions_framework.http
def execute(request):
    """
    Main function to orchestrate fetching, processing, and storing collision data.

    This function is triggered via HTTP request, processes data from NYC Open Data,
    and logs its execution status in BigQuery and stores the data in GCS.

    Args:
        request: The request object that triggered this function.

    Returns:
        str, int: A message indicating the function executed successfully and an HTTP status code.
    """
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
            print(f"Upload failure: {e}")
            state = "Failed"
    else:
        state = "Error"

    end_timestamp = datetime.datetime.now()
    time_taken = end_timestamp - start_timestamp
    function_state = {
        "process_name": PROCESS_NAME,
        "process_status": state,
        "process_start_time": start_timestamp.isoformat(),
        "process_end_time": end_timestamp.isoformat(),
        "time_taken": round(time_taken.total_seconds(), 3),
        "last_offset_fetched": last_offset + LIMIT,
        "insert_ts": datetime.datetime.now().isoformat(),
    }
    store_func_state(table_id, function_state)
    return 'Function executed', 200
