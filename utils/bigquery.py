import logging
from google.cloud import bigquery


ROOT_PREFIX = "df"
STAGE_SUFFIX = "stg"
RAW_SUFFIX = "raw"
PROD_SUFFIX = "prd"
TRAFFIC_TABLE = "dot_traffic_speeds"
CRASHES_TABLE = "mv_collision_crashes"
VEHICLES_TABLE = "mv_collision_vehicles"
PERSONS_TABLE = "mv_collision_persons"
TLC_TRIP_TABLE = "tlc_trip_nyc"
STG_DATASET = f"{ROOT_PREFIX}_{STAGE_SUFFIX}"
RAW_DATASET = f"{ROOT_PREFIX}_{RAW_SUFFIX}"
PRD_DATASET = f"{ROOT_PREFIX}_{PROD_SUFFIX}"

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
]

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DF-GCP-TABLES")

# Construct a BigQuery client object
client = bigquery.Client()


def create_dataset(dataset_name=None):
    try:
        dataset_id = f"{client.project}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        return dataset
    except Exception:
        return False


def create_table(dataset_name, table_name, schema):
    try:
        dataset_id = f"{client.project}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        table_id = f"{client.project}.{dataset.dataset_id}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        return table
    except Exception:
        return False


def delete_table(dataset_name, table_name):
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    table_id = f"{client.project}.{dataset.dataset_id}.{table_name}"
    table = bigquery.Table(table_id)
    client.delete_table(table)
    logger.warning(
        f"Deleted Table: {table.project}.{table.dataset_id}.{table.table_id}"
    )


def delete_dataset(dataset_name):
    client.delete_dataset(dataset_name)


def create_datasets():
    stage_dataset = create_dataset(STG_DATASET)
    if stage_dataset:
        logger.warning(
            f"Created STG dataset {client.project}.{stage_dataset.dataset_id}"
        )
    else:
        logger.warning("Failed to create stage dataset")

    raw_dataset = create_dataset(RAW_DATASET)
    if raw_dataset:
        logger.warning(
            f"Created RAW dataset {client.project}.{raw_dataset.dataset_id}"
        )
    else:
        logger.warning("Failed to create raw dataset")

    prod_dataset = create_dataset(PRD_DATASET)
    if prod_dataset:
        logger.warning(
            f"Created PRD dataset {client.project}.{prod_dataset.dataset_id}"
        )
    else:
        logger.warning("Failed to create prod dataset")

    catalog_dataset = create_dataset("data_catalog")
    if catalog_dataset:
        logger.warning(
            f"Created CATALOG dataset {client.project}.{catalog_dataset.dataset_id}"
        )
    else:
        logger.warning("Failed to create catalog dataset")


def create_tables():
    tables = {
        TRAFFIC_TABLE: traffic_data_schema,
        CRASHES_TABLE: mv_crashes_schema,
        VEHICLES_TABLE: mv_vehicles_schema,
        PERSONS_TABLE: mv_persons_schema,
        TLC_TRIP_TABLE: tlc_trip_data_schema,
    }

    catalog_table = create_table("data_catalog", f"{ROOT_PREFIX}_process_catalog", catalog_schema)
    if catalog_table:
        logger.warning("Created catalog table")
    else:
        logger.warning("Error while creating catalog table")

    for table_name, schema in tables.items():
        stage_table = create_table(
            STG_DATASET, f"{ROOT_PREFIX}_{table_name}_{STAGE_SUFFIX}", schema
        )
        if stage_table:
            logger.warning(
                f"Created table {stage_table.project}.{stage_table.dataset_id}.{stage_table.table_id}"
            )
        else:
            logger.warning(
                f"Error while creating table: {ROOT_PREFIX}_{table_name}_{STAGE_SUFFIX}"
            )

        raw_table = create_table(
            RAW_DATASET, f"{ROOT_PREFIX}_{table_name}_{RAW_SUFFIX}", schema
        )
        if raw_table:
            logger.warning(
                f"Created table {raw_table.project}.{raw_table.dataset_id}.{raw_table.table_id}"
            )
        else:
            logger.warning(
                f"Error while creating table: {ROOT_PREFIX}_{table_name}_{RAW_SUFFIX}"
            )

        prd_table = create_table(
            PRD_DATASET, f"{ROOT_PREFIX}_{table_name}_{PROD_SUFFIX}", schema
        )
        if stage_table:
            logger.warning(
                f"Created table {prd_table.project}.{prd_table.dataset_id}.{prd_table.table_id}"
            )
        else:
            logger.warning(
                f"Error while creating table: {ROOT_PREFIX}_{table_name}_{PROD_SUFFIX}"
            )


def main():
    create_datasets()
    create_tables()


main()
