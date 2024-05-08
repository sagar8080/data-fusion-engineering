import openmeteo_requests
import datetime
import requests_cache
import pandas as pd
from retry_requests import retry

import functions_framework
from google.cloud import bigquery
import json

f= open("config.json", "r")
config = json.loads(f.read())
LANDING_BUCKET = config["landing_bucket"]
CATALOG_TABLE_ID = config["catalog_table"]

LOCATION = {
    "Manhattan": {"lattitude": 40.71, "longitude": -74.00},
    "Brooklyn": {"lattitude": 40.67, "longitude": -73.97},
    "Queens": {
        "lattitude": 40.71,
        "longitude": -73.80,
    },
    "Bronx": {"lattitude": 40.85, "longitude": -73.93},
    "Staten Island": {"lattitude": 40.57, "longitude": -74.07},
}

DEFAULT_DATE = "2009-01-01"
DAY_DELTA = 60
PROCESS_NAME = "df-ingest-weather-data"

cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)
bq_client = bigquery.Client()


def get_dates(input_date):
    if isinstance(input_date, str):
        try:
            start_date = datetime.datetime.strptime(input_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("input_date string must be formatted as 'YYYY-MM-DD'")
    elif isinstance(input_date, datetime.datetime):
        start_date = input_date
    else:
        raise TypeError("input_date must be either a string formatted as 'YYYY-MM-DD' or a datetime object")
    end_date = start_date + datetime.timedelta(days=DAY_DELTA)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    return start_date_str, end_date_str


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
        return offset if offset else DEFAULT_DATE
    except IndexError:
        return DEFAULT_DATE


# openmeteo = openmeteo_requests.Client()


def fetch_data(burrough, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    lat, lon = LOCATION.get(burrough, {}).get("lattitude", None), LOCATION.get(
        burrough, {}
    ).get("longitude", None)
    print(
        f"Currently processing data for {burrough} with start date of {start_date} and end date of {end_date}"
    )
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "snowfall",
            "snow_depth",
            "weather_code",
            "pressure_msl",
            "surface_pressure",
            "cloud_cover",
            "wind_speed_10m",
            "wind_gusts_10m",
            "is_day",
            "sunshine_duration",
        ],
        "timezone": "America/New_York",
    }

    responses = openmeteo.weather_api(url, params=params)
    return responses


def get_table_id():
    dataset_name = "data_catalog"
    table_name = "df_process_catalog"
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


def process_responses(responses, burrough):
    response = responses[0]
    lattitude = LOCATION.get(burrough).get("lattitude")
    longitude = LOCATION.get(burrough).get("longitude")
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
    hourly_rain = hourly.Variables(5).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(6).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(7).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(8).ValuesAsNumpy()
    hourly_pressure_msl = hourly.Variables(9).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(10).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(11).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(12).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(13).ValuesAsNumpy()
    hourly_is_day = hourly.Variables(14).ValuesAsNumpy()
    hourly_sunshine_duration = hourly.Variables(15).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }

    hourly_data["elevation"] = f"{response.Elevation()} m asl"
    hourly_data["burrough"] = burrough
    hourly_data["lattitude"] = lattitude
    hourly_data["longitude"] = longitude
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["rain"] = hourly_rain
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["weather_code"] = hourly_weather_code
    hourly_data["pressure_msl"] = hourly_pressure_msl
    hourly_data["surface_pressure"] = hourly_surface_pressure
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
    hourly_data["is_day"] = hourly_is_day
    hourly_data["sunshine_duration"] = hourly_sunshine_duration
    hourly_dataframe = pd.DataFrame(data=hourly_data)

    return hourly_dataframe


def upload_to_gcs(data, burrough):
    current_day = datetime.date.today()
    current_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    file_path = f"data/pre-processed/weather_data/{current_day}"
    file_name = f"{burrough}_{current_timestamp}.csv"
    try:
        data.to_csv(f"gs://{LANDING_BUCKET}/{file_path}/{file_name}")
        return "Success"
    except Exception as e:
        print(e)
        return "Failure"


@functions_framework.http
def execute(request):
    start_timestamp = end_timestamp = datetime.datetime.now()
    last_date_loaded = fetch_last_offset(CATALOG_TABLE_ID)
    start_date, end_date = get_dates(last_date_loaded)
    try:
        for burrough in LOCATION.keys():
            hourly_data = fetch_data(burrough, start_date, end_date)
            processed_data = process_responses(hourly_data, burrough)
            if len(processed_data) > 0:
                state = upload_to_gcs(processed_data, burrough)
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
            "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        store_func_state(CATALOG_TABLE_ID, function_state)
        bq_client.close()
    except Exception as e:
        print(e)
    return state