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

# Define the schema fields for df-traffic-data
traffic_data_schema = [
    bigquery.SchemaField("ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("SPEED", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("TRAVEL_TIME", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("STATUS", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DATA_AS_OF", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("LINK_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("LINK_POINTS", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ENCODED_POLY_LINE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ENCODED_POLY_LINE_LVLS", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("OWNER", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("TRANSCOM_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("BOROUGH", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("LINK_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("insert_ts", "DATETIME", mode="REQUIRED"),
]


mv_crashes_schema = [
    bigquery.SchemaField("COLLISION_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ACCIDENT_DATE", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ACCIDENT_TIME", "TIME", mode="REQUIRED"),
    bigquery.SchemaField("BOROUGH", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ZIP_CODE", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("LATITUDE", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("LONGITUDE", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("LOCATION", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ON_STREET_NAME", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CROSS_STREET_NAME", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("OFF_STREET_NAME", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_PERSONS_INJURED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_PERSONS_KILLED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_PEDESTRIANS_INJURED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_PEDESTRIANS_KILLED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_CYCLIST_INJURED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_CYCLIST_KILLED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_MOTORIST_INJURED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("NUMBER_OF_MOTORIST_KILLED", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_VEHICLE_1", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_VEHICLE_2", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_VEHICLE_3", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_VEHICLE_4", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_VEHICLE_5", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VEHICLE_TYPE_CODE_1", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VEHICLE_TYPE_CODE_2", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VEHICLE_TYPE_CODE_3", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VEHICLE_TYPE_CODE_4", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VEHICLE_TYPE_CODE_5", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("insert_ts", "DATETIME", mode="REQUIRED"),
]


mv_persons_schema = [
    bigquery.SchemaField("UNIQUE_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("COLLISION_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ACCIDENT_DATE", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ACCIDENT_TIME", "TIME", mode="REQUIRED"),
    bigquery.SchemaField("VICTIM_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VICTIM_TYPE", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VICTIM_INJURY", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VEHICLE_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VICTIM_AGE", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("EJECTION", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("EMOTIONAL_STATUS", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("BODILY_INJURY", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("POSITION_IN_VEHICLE", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("SAFETY_EQUIPMENT", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("PED_LOCATION", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("PED_ACTION", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("COMPLAINT", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VICTIM_ROLE", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_1", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_2", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("VICTIM_SEX", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("insert_ts", "DATETIME", mode="REQUIRED"),
]

mv_vehicles_schema = [
    bigquery.SchemaField("UNIQUE_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("COLLISION_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ACCIDENT_DATE", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("ACCIDENT_TIME", "TIME", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("STATE_REGISTRATION", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_TYPE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_MAKE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_MODEL", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_YEAR", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("TRAVEL_DIRECTION", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_OCCUPANTS", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DRIVER_SEX", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DRIVER_LICENSE_STATUS", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DRIVER_LICENSE_JURISDICTION", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("PRE_ACDNT_ACTION", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("POINT_OF_IMPACT", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_DAMAGE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_DAMAGE_1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_DAMAGE_2", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VEHICLE_DAMAGE_3", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("PUBLIC_PROPERTY_DAMAGE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("PUBLIC_PROPERTY_DAMAGE_TYPE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CONTRIBUTING_FACTOR_2", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("insert_ts", "DATETIME", mode="REQUIRED"),
]


tlc_trip_data_schema = [
    bigquery.SchemaField("VendorID", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("passenger_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("trip_distance", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("RatecodeID", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("PULocationID", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("DOLocationID", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("payment_type", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("fare_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("extra", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("mta_tax", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("tip_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("tolls_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("insert_ts", "DATETIME", mode="REQUIRED"),
]
