from google.cloud import bigquery


ROOT_PREFIX = "df"
STAGE_SUFFIX = "stg"
RAW_SUFFIX = "raw"
PROD_SUFFIX = "prd"

# bigquery info
STG_DATASET = f"{ROOT_PREFIX}_{STAGE_SUFFIX}"
RAW_DATASET = f"{ROOT_PREFIX}_{RAW_SUFFIX}"
PRD_DATASET = f"{ROOT_PREFIX}_{PROD_SUFFIX}"
CATALOG_DATASET = "data_catalog"
TRAFFIC_TABLE = "traffic_data"
CRASHES_TABLE = "crashes_data"
VEHICLES_TABLE = "vehicles_data"
PERSONS_TABLE = "persons_data"
TLC_TRIP_TABLE = "taxi_data"
WEATHER_TABLE = "weather_data"
CATALOG_TABLE = f"{ROOT_PREFIX}_process_catalog"


# bucket level info
LANDING_BUCKET = f"{ROOT_PREFIX}-landing-zone"
CODE_BUCKET = f"{ROOT_PREFIX}-code-bucket"
UTILITY_BUCKET = f"{ROOT_PREFIX}-utility"
DATAPROC_BUCKET = f"{ROOT_PREFIX}-dataproc"

catalog_schema = [
    bigquery.SchemaField("process_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("process_status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("process_start_time", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("process_end_time", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("time_taken", "DECIMAL", mode="NULLABLE"),
    bigquery.SchemaField("last_offset_fetched", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("last_timestamp_loaded", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("processed_rows", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("insert_ts", "DATETIME", mode="REQUIRED"),
]