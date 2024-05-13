import requests
import json
import datetime
import pandas as pd
import urllib.parse

import functions_framework
from google.cloud import storage, bigquery


bq_client = bigquery.Client()
storage_client = storage.Client()
f= open("config.json", "r")
config = json.loads(f.read())
LANDING_BUCKET = config["landing_bucket"]
CATALOG_TABLE_ID = config["catalog_table"]
PROCESS_NAME = "df-ingest-persons-data"
LIMIT = 1000000
DAY_DELTA = 60
DEFAULT_START_DATE = '2012-07-01T00:00:00'
DEFAULT_END_DATE = '2012-07-31T23:59:59'
BASE_URL = "https://data.cityofnewyork.us/resource/f55k-p6yu.csv"
BASE_FILE_PATH = "data/pre-processed/persons_data"
BASE_PROC_NAME = "persons_data"


def fetch_last_offset(table_id):
    """
    Fetches the last timestamp loaded from a specified table in BigQuery.

    Args:
        table_id (str): The ID of the table in BigQuery from which to fetch the last timestamp.

    Returns:
        str: The last timestamp loaded, or the default start date if no timestamp is found.
        
    """
    # Construct SQL query to select the last loaded timestamp from the specified table
    query = f"""
    SELECT last_timestamp_loaded  FROM `{table_id}`
    WHERE process_name='{PROCESS_NAME}' AND process_status='Success'
    ORDER BY insert_ts DESC
    LIMIT 1
    """

    # Execute the query and retrieve results
    results = bq_client.query(query).result()

    try:
        # Extract the last timestamp loaded from the query results
        offset = list(results)[0]["last_timestamp_loaded"]
        # If offset is None or empty, return default start date
        return offset if offset else DEFAULT_START_DATE
    except Exception:
        return DEFAULT_START_DATE

    

def get_dates(input_date):
    """
    Generates start and end dates based on the input date.

    Args:
        input_date (datetime): The input date. If a string, it should be in this format: "%Y-%m-%dT%H:%M:%S".

    Returns:
        tuple: A tuple containing the start date and end date in this format: "%Y-%m-%dT%H:%M:%S".

    """
    # If input_date is a string, convert it to a datetime object
    if isinstance(input_date, str):
        start_date = datetime.datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S")
    # If input_date is already a datetime object, use it directly
    elif isinstance(input_date, datetime.datetime):
        start_date = input_date

    # Calculate the end date by adding DAY_DELTA days to the start date
    end_date = start_date + datetime.timedelta(days=DAY_DELTA)

    # Format start_date and end_date as strings in the required format
    start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    return start_date, end_date


def fetch_data(start_date, end_date):
    """
    Fetches data from an API endpoint within a specified date range.

    Args:
        start_date (str): The start date of the date range in the format 'YYYY-MM-DD'.
        end_date (str): The end date of the date range in the format 'YYYY-MM-DD'.

    Returns:
        pandas.DataFrame or None: A DataFrame containing the fetched data, or None if the request fails.

    """
    try:
        # Define parameters for the API request
        params = {
            '$limit': f"{LIMIT}",
            '$where': f"crash_date >= '{start_date}' AND crash_date <= '{end_date}'"
        }
        
        # Encode the parameters for use in the URL
        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        
        # Construct the API URL with the encoded parameters
        api_url = f"{BASE_URL}?{encoded_params}"
        
        # Read data from the API URL into a DataFrame
        df = pd.read_csv(api_url, low_memory=False)
        
        # Return the DataFrame
        return df
    except requests.RequestException as e:
        # If the request fails, print an error message and return None
        print(f"Request failed: {e}")
    return None


def upload_to_gcs(data):
    """
    Uploads a DataFrame to Google Cloud Storage (GCS).

    Args:
        data (pandas.DataFrame): The DataFrame to be uploaded to GCS.

    Returns:
        str: "Success" if the upload is successful, "Failure" otherwise.

    Raises:
        None
    """
    # Get the current date and timestamp
    current_day = datetime.date.today()
    current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    
    # Define the file path and name for the uploaded file
    file_path = f"{BASE_FILE_PATH}/{current_day}"
    file_name = f"{BASE_PROC_NAME}_{current_timestamp}.csv"
    
    try:
        # Upload the DataFrame to GCS using a URL like gs://<bucket_name>/<file_path>/<file_name>
        data.to_csv(f"gs://{LANDING_BUCKET}/{file_path}/{file_name}", index=False)
        # Return "Success" if the upload is successful
        return "Success"
    except Exception as e:
        # If an exception occurs during the upload, print the error message and return "Failure"
        print(e)
        return "Failure"


def store_func_state(table_id, state_json):
    """
    Stores the state of a function in a specified BigQuery table.

    Args:
        table_id (str): The ID of the BigQuery table where the function state will be stored.
        state_json (dict): The JSON object representing the state of the function.

    Returns:
        None

    Raises:
        None
    """
    # Prepare the data to be inserted into the BigQuery table
    rows_to_insert = [state_json]
    
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    
    # Check if there are any errors during insertion
    if not errors:
        print("New rows have been added.")
    else:
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