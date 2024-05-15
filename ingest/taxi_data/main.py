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
    """
    Fetch the last successful timestamp from a specified table.

    Parameters:
    - table_id (str): The ID of the BigQuery table from which to fetch the data.

    Returns:
    - datetime or similar: The last successful timestamp loaded into the table or a default start date if no successful records exist.

    """
    # SQL query to find the last timestamp from the table where the process was successful
    query = f"""
    SELECT last_timestamp_loaded FROM `{table_id}`
    where process_name='{PROCESS_NAME}' and process_status='Success'
    ORDER BY insert_ts desc
    LIMIT 1
    """

    # Execute the query and retrieve the results
    results = bq_client.query(query).result()

    try:
        offset = list(results)[0]["last_timestamp_loaded"]
        # Return the fetched timestamp or the default start date if the timestamp is None
        return offset if offset else DEFAULT_START_DATE
    except Exception:
        # Return the default start date if any errors occur during data retrieval
        return DEFAULT_START_DATE
    

def get_start_end_date(input_date):
    """
    Calculate the start and end dates based on the input date.

    Parameters:
    - input_date (str or datetime.datetime): The date from which to calculate the month period. 

    Returns:
    - tuple: A tuple containing two strings, the start date in "YYYY-MM" format and the end date in
             "YYYY-MM-DDTHH:MM:SS" format.

    """
    # Determine the type of input_date and convert it to a datetime object if necessary
    if isinstance(input_date, str):
        start_date = datetime.datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S")
    elif isinstance(input_date, datetime.datetime):
        start_date = input_date
    else:
        raise ValueError("input_date must be a string in ISO format or a datetime.datetime object")

    # Calculate the end_date by adding one month to the start_date
    end_date = start_date + relativedelta(months=+1)

    # Format the start_date as "YYYY-MM" and the end_date as "YYYY-MM-DDTHH:MM:SS"
    start_date = start_date.strftime("%Y-%m")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    # Return the formatted start_date and end_date
    return start_date, end_date


def generate_file_url(date):
    return f"{BASE_URL}/yellow_tripdata_{date}.parquet"


def upload_to_gcs(file_url):
    """
    Uploads a file from a specified URL to Google Cloud Storage (GCS).
    This function downloads a file from the given URL and uploads it to a Google Cloud Storage bucket.

    Parameters:
    - file_url (str): The URL of the file to be downloaded and uploaded to GCS.

    """
    # Get the current date to create a file path in the GCS bucket
    current_day = datetime.date.today()
    file_path = f"data/pre-processed/taxi_data/{current_day}"

    try:
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            # Extract the file name from the URL
            file_name = file_url.split('/')[-1]
            # Initialize the GCS bucket and blob object
            bucket = storage_client.bucket(LANDING_BUCKET)
            blob = bucket.blob(f"{file_path}/{file_name}")
            # Upload the file content as 'application/octet-stream'
            blob.upload_from_string(response.content, content_type='application/octet-stream')
            print(f"File {file_name} uploaded successfully to GCS.")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
    except Exception as e:
        # Print any errors that occur during the file upload process
        print(e)


def store_func_state(table_id, state_json):
    """
    Inserts a new row into a BigQuery table with the provided JSON data.

    Parameters:
    - table_id (str): The ID of the BigQuery table where the data will be inserted.
    - state_json (dict): A dictionary representing the JSON data to be inserted into the table.

    """
    # Prepare the data as a list of dictionaries (rows) for insertion
    rows_to_insert = [state_json]

    # Try to insert the row into the specified BigQuery table
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)

    # Check if there were any errors during the insertion
    if not errors:
        print("New rows have been added.")
    else:
        # Print the errors if the insertion was unsuccessful
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