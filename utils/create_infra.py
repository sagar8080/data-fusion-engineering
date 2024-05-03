import logging
import os
import json
import shutil
import uuid
import hashlib
from argparse import ArgumentParser

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from constants import *


args = ArgumentParser()
args.add_argument("-o", dest="operation")
args.add_argument("-p", dest="project_id", required=False)
args.add_argument("-bucket", dest="config_bucket", required=False)

parser = args.parse_args()
operation_type = parser.operation
config_bucket = parser.config_bucket if parser.config_bucket else None
project_id = parser.project_id if parser.project_id else None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DF-GCP-LOGS")

if project_id:
    bigquery_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)
else:
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()


CONFIG = dict()


def generate_hashed_id():
    unique_string = str(uuid.uuid4())
    hashed = hashlib.sha256(unique_string.encode()).hexdigest()
    short_id = int(hashed[:6], 16) % 1000000
    return str(short_id).zfill(6)


def dataset_exists(dataset_id):
    dataset_ref = bigquery_client.dataset(dataset_id)
    try:
        return bigquery_client.get_dataset(dataset_ref).dataset_id
    except NotFound:
        return False


def table_exists(dataset_id, table_id):
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    try:
        return bigquery_client.get_table(table_ref)
    except NotFound:
        return False


def create_dataset(dataset_name=None):
    try:
        dataset_id = dataset_exists(dataset_name)
        if not dataset_id:
            dataset_id = f"{bigquery_client.project}.{dataset_name}"
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset = bigquery_client.create_dataset(dataset)
            logger.warning(f"creating dataset: {dataset_id}")
            return dataset
        else:
            logger.warning(f"Dataset {dataset_id} already exists.")
            return dataset_id
    except Exception:
        return False


def create_table(dataset_name, table_name, schema):
    try:
        table_id = table_exists(dataset_id=dataset_name, table_id=table_name)
        if not table_id:
            dataset_id = f"{bigquery_client.project}.{dataset_name}"
            dataset = bigquery.Dataset(dataset_id)
            table_id = f"{bigquery_client.project}.{dataset.dataset_id}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)
            table = bigquery_client.create_table(table)
            logger.warning(f"creating TABLE: {table_id}")
            return table
        else:
            logger.warning(f"TABLE {table_id} already exists.")
            return table_id
    except Exception:
        return False


def delete_table(dataset_name, table_name):
    dataset_id = f"{bigquery_client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    table_id = f"{bigquery_client.project}.{dataset.dataset_id}.{table_name}"
    table = bigquery.Table(table_id)
    bigquery_client.delete_table(table)
    logger.warning(f"Deleted Table: {table.project}.{table.dataset_id}.{table.table_id}")


def delete_dataset(dataset_name):
    bigquery_client.delete_dataset(dataset_name)


def create_datasets(config):
    stage_dataset = create_dataset(STG_DATASET)
    if stage_dataset:
        config["stage_dataset"] = STG_DATASET
    else:
        logger.warning("Failed to create stage dataset")

    raw_dataset = create_dataset(RAW_DATASET)
    if raw_dataset:
        config["raw_dataset"] = RAW_DATASET
    else:
        logger.warning("Failed to create raw dataset")

    prod_dataset = create_dataset(PRD_DATASET)
    if prod_dataset:
        config["prod_dataset"] = PRD_DATASET
    else:
        logger.warning("Failed to create prod dataset")

    catalog_dataset = create_dataset(CATALOG_DATASET)
    if catalog_dataset:
        config["catalog_dataset"] = CATALOG_DATASET
    else:
        logger.warning("Failed to create catalog dataset")


def create_tables(config):
    tables = {
        TRAFFIC_TABLE: traffic_data_schema,
        CRASHES_TABLE: mv_crashes_schema,
        VEHICLES_TABLE: mv_vehicles_schema,
        PERSONS_TABLE: mv_persons_schema,
        TLC_TRIP_TABLE: tlc_trip_data_schema,
    }
    config["stage_tables"] = {}
    config["raw_tables"] = {}
    config["prod_tables"] = {}
    catalog_table = create_table(CATALOG_DATASET, CATALOG_TABLE, catalog_schema)
    if catalog_table:
        config["catalog_table"] = (
            f"{catalog_table.project}.{catalog_table.dataset_id}.{catalog_table.table_id}"
        )
    else:
        logger.warning(f"Failed to create TABLE: {catalog_table}")

    for table_name, schema in tables.items():
        stage_table = create_table(
            STG_DATASET, f"{ROOT_PREFIX}_{table_name}_{STAGE_SUFFIX}", schema
        )
        if stage_table:
            config["stage_tables"][
                table_name
            ] = f"{stage_table.project}.{stage_table.dataset_id}.{stage_table.table_id}"
        else:
            logger.warning(
                f"Failed to create TABLE: {ROOT_PREFIX}_{table_name}_{STAGE_SUFFIX}"
            )

        raw_table = create_table(
            RAW_DATASET, f"{ROOT_PREFIX}_{table_name}_{RAW_SUFFIX}", schema
        )
        if raw_table:
            config["raw_tables"][
                table_name
            ] = f"{raw_table.project}.{raw_table.dataset_id}.{raw_table.table_id}"
        else:
            logger.warning(
                f"Failed to create TABLE: {ROOT_PREFIX}_{table_name}_{RAW_SUFFIX}"
            )

        prod_table = create_table(
            PRD_DATASET, f"{ROOT_PREFIX}_{table_name}_{PROD_SUFFIX}", schema
        )
        if prod_table:
            config["prod_tables"][
                table_name
            ] = f"{prod_table.project}.{prod_table.dataset_id}.{prod_table.table_id}"
        else:
            logger.warning(
                f"Failed to create TABLE: {ROOT_PREFIX}_{table_name}_{PROD_SUFFIX}"
            )


def bucket_exists(bucket_name):
    try:
        storage_client.get_bucket(bucket_name)
        return True
    except NotFound:
        return False


def create_buckets(config):
    try:
        buckets = {
            "landing_bucket": LANDING_BUCKET,
            "code_bucket": CODE_BUCKET,
            "util_bucket": UTILITY_BUCKET,
            "dataproc_bucket": DATAPROC_BUCKET,
        }
        for key, b in buckets.items():
            bucket_name = f"{b}-{generate_hashed_id()}"
            config[key] = bucket_name
            if not bucket_exists(bucket_name):
                bucket = storage_client.bucket(bucket_name)
                new_bucket = storage_client.create_bucket(bucket, location="US")
                new_bucket.acl.save_predefined("private")
                logger.warning(f"Bucket {bucket_name} created with private ACL.")
            else:
                logger.warning(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Error occured: {e}")


def upload_cfn_code(folder_path, config):
    code_bucket_name = config["code_bucket"]
    code_bucket = storage_client.bucket(code_bucket_name)
    for foldername in os.listdir(folder_path):
        folder_abs_path = os.path.join(folder_path, foldername)
        if os.path.isdir(folder_abs_path):
            zip_path = f"{os.getcwd()}/zipped/{foldername}"
            shutil.make_archive(zip_path, "zip", folder_abs_path)
            blob = code_bucket.blob(f"scripts/ingest-{foldername}.zip")
            logger.warning(f"uploading {folder_abs_path}")
            blob.upload_from_filename(f"{zip_path}.zip")


def add_config_to_ingest(base_path, config):
    try:
        if not os.path.isdir(base_path):
            logger.error(f"The path {base_path} is not a valid directory.")
            return

        for item in os.listdir(base_path):
            item_path = os.path.join(base_path, item)
            if os.path.isdir(item_path):
                config_file_path = os.path.join(item_path, "config.json")
                with open(config_file_path, "w+") as config_file:
                    json.dump(config, config_file, indent=4)
                logger.warning(f"Written config.json to {item_path}")
                add_config_to_ingest(item_path, config)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to encode config to JSON: {e}")
    except OSError as e:
        logger.error(f"Failed to write to file system: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def upload_to_cloud(code_bucket_name, config):
    code_bucket = storage_client.bucket(code_bucket_name)
    blob = code_bucket.blob("config/config.json")
    config_string = json.dumps(config)
    blob.upload_from_string(config_string)
    logger.warning(f"Uploaded config file to {code_bucket_name}")


def write_config(config):
    filename = "config.json"
    filepath = os.path.join(os.getcwd(), "utils", filename)
    code_bucket_name = config["code_bucket"]

    try:
        with open(filepath, "w") as f:
            json.dump(config, f, indent=4)
        upload_to_cloud(code_bucket_name, config)

    except json.JSONDecodeError as e:
        logger.error(f"Failed to encode config to JSON: {e}")
    except OSError as e:
        logger.error(f"Failed to write to file system: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        try:
            upload_to_cloud(code_bucket_name, config)
        except Exception as e:
            logger.error(f"Failed to upload config file to Google Cloud Storage: {e}")


def upload_dataproc_jobs(filepath, config):
    code_bucket_name = config["code_bucket"]
    code_bucket = storage_client.bucket(code_bucket_name)
    for filename in os.listdir(filepath):
        if filename.endswith('.py'):
            local_file_path = os.path.join(filepath, filename)
            destination_blob_name = f"scripts/{filename}"
            blob = code_bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            logger.warning(f"Uploaded {filename} to GCS bucket {code_bucket_name} successfully.")


def read_config(config_bucket):
    filename = "config.json"
    filepath = os.path.join(os.getcwd(), f"utils/{filename}")
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            configuration = json.load(f)
        return configuration
    else:
        code_bucket = storage_client.bucket(config_bucket)
        blob = code_bucket.blob(f"config/config.json")
        configuration = json.loads(blob.download_as_string())
        return configuration
    

def check_config_in_utils():
    config_path = os.path.join('utils', 'config.json')
    if os.path.exists(config_path):
        return True
    else:
        return False


def delete_buckets(conf):
    pass


def delete_datasets(conf):
    pass


def delete_tables(conf):
    pass


def main():
    if operation_type.lower() == "create":
        
        if check_config_in_utils:
            logger.warning("Config found, skipping resource creation, updating cloud function")
            f = open("utils/config.json", "r")
            config = json.loads(f.read())
            write_config(config)
            add_config_to_ingest(f"{os.getcwd()}/ingest/", config)
            upload_cfn_code(f"{os.getcwd()}/ingest/", config)
            upload_dataproc_jobs(f"{os.getcwd()}/data_pipelines/", config)
        
        else:
            logger.warning("Config not found, creating GCP resources")
            config = dict()
            create_buckets(config)
            create_datasets(config)
            create_tables(config)
            write_config(config)
            add_config_to_ingest(f"{os.getcwd()}/ingest/", config)
            upload_cfn_code(f"{os.getcwd()}/ingest/", config)
            upload_dataproc_jobs(f"{os.getcwd()}/data_pipelines/", config)
    
    elif operation_type.lower() == "delete":
        conf = read_config(config_bucket)
        delete_buckets(conf)
        delete_datasets(conf)
        delete_tables(conf)


main()
