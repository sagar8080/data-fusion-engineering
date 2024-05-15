import requests
import json
import datetime
import traceback
import pandas as pd
import urllib.parse

import functions_framework
from google.cloud import storage, bigquery


bq_client = bigquery.Client()
storage_client = storage.Client()
f = open("config.json", "r")
config = json.loads(f.read())

LIMIT = 200000
PROCESS_NAME = "df-ingest-crashes-data"
BASE_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
BASE_FILE_PATH = "data/pre-processed/crashes_data"
BASE_PROC_NAME = "crashes_data"
LANDING_BUCKET = config["landing_bucket"]
CATALOG_TABLE_ID = config["catalog_table"]
LIMIT = 1000000
DAY_DELTA = 60
DEFAULT_START_DATE = "2012-07-01T00:00:00"
DEFAULT_END_DATE = "2012-07-31T23:59:59"


def fetch_last_offset(table_id):
    """
    Fetches the last timestamp loaded from a specified table.

    Args:
        table_id (str): ID of the table from which to fetch the last timestamp.

    Returns:
        str: last timestamp loaded from the table or the default start date if no timestamp is found.

    """
    query = f"""
    SELECT last_timestamp_loaded FROM `{table_id}`
    where process_name='{PROCESS_NAME}' and process_status='Success'
    ORDER BY insert_ts desc
    LIMIT 1
    """
    # Execute the query and retrieve results
    results = bq_client.query(query).result()
    try:
        offset = list(results)[0]["last_timestamp_loaded"]
        # Return the extracted offset if it exists, otherwise return the default start date
        return offset if offset else DEFAULT_START_DATE
    except Exception:

        return DEFAULT_START_DATE


def get_dates(input_date):
    """
    Converts the input date to a start date and an end date.

    Args:
        input_date (datetime): The input date to be converted.

    Returns:
        tuple: A tuple containing the start date and end date.
    """
    if isinstance(input_date, str):
        # If input_date is a string, convert it to a datetime object
        start_date = datetime.datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S")
    elif isinstance(input_date, datetime.datetime):
        # If input_date is already a datetime object, use it as is
        start_date = input_date

    # Calculate the end date by adding the specified number of days
    end_date = start_date + datetime.timedelta(days=DAY_DELTA)

    # Convert start_date and end_date to ISO 8601 format
    start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    return start_date, end_date


def fetch_data(start_date, end_date):
    """
    Fetches data from an API based on the specified date range.

    Args:
        start_date (str): The start date of the data range in ISO 8601 format ("%Y-%m-%dT%H:%M:%S").
        end_date (str): The end date of the data range in ISO 8601 format ("%Y-%m-%dT%H:%M:%S").

    Returns:
        DataFrame or None: A DataFrame containing the fetched data if successful, otherwise None.
    """
    try:
        # Construct parameters for API request
        params = {
            "$limit": f"{LIMIT}",
            "$where": f"crash_date >= '{start_date}' AND crash_date <= '{end_date}'",
        }
        # Encode parameters for URL and construct API URL
        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        api_url = f"{BASE_URL}?{encoded_params}"

        # Read data from API into a DataFrame
        df = pd.read_csv(api_url, low_memory=False)
        return df
    except requests.RequestException as e:
        print(f"Request failed: {e}")

    # Return None if there was an error fetching the data
    return None


def upload_to_gcs(data):
    """
    Uploads data to Google Cloud Storage in CSV format.

    Args:
        data (Data Frame): The data to be uploaded.

    Returns:
        str: A string indicating the status of the upload operation. Possible values are "Success" or "Failure".

    """
    # Get the current day and timestamp
    current_day = datetime.date.today()
    current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    # Define the file path and name
    file_path = f"{BASE_FILE_PATH}/{current_day}"
    file_name = f"{BASE_PROC_NAME}_{current_timestamp}.csv"

    try:
        # Upload the data to GCS
        data.to_csv(f"gs://{LANDING_BUCKET}/{file_path}/{file_name}", index=False)
        return "Success"
    except Exception as e:
        # Print the error message if an exception occurs during the upload
        print(e)
        return "Failure"


def store_func_state(table_id, state_json):
    """
    Stores function state JSON data into a specified BigQuery table.

    Args:
        table_id (str): The ID of the BigQuery table where the data will be stored.
        state_json (dict): The function state JSON data to be stored.

    """
    # Prepare data for insertion into BigQuery table
    rows_to_insert = [state_json]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)

    # Check if insertion was successful or not
    if not errors:
        print("New rows have been added.")
    else:
        # Print any insertion errors
        print(f"Insert errors: {errors}")


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
