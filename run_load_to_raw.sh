#!/bin/bash

# Environment Variables
# Get the current Google Cloud project ID
CONFIG_PATH="utils/config.json"
PROJECT_ID=$(gcloud config get-value project)

# Check if a project ID is set
if [ -z "$PROJECT_ID" ]; then
  echo "No Google Cloud project is set. Please set a project using 'gcloud config set project PROJECT_ID'."
  exit 1
fi

echo "Using project: $PROJECT_ID"
CLUSTER_NAME="data-fusion"
REGION="us-east4"
CODE_BUCKET=$(jq -r '.code_bucket' "$CONFIG_PATH")
DATAPOC_BUCKET="df-dataproc"
TRAFFIC_DATA_FILE="gs://${CODE_BUCKET}/scripts/taxi_data_landing_to_raw_pyspark.py"
TAXI_DATA_FILE="gs://${CODE_BUCKET}/scripts/taxi_data_landing_to_raw_pyspark.py"
CONFIG_FILE="gs://${CODE_BUCKET}/config/config.json"
TRAFFIC_PYSPARK_JOB_NAME="load-traffic-raw-$(printf "%04d" $((RANDOM % 10000)))"
TAXI_PYSPARK_JOB_NAME="load-traffic-raw-$(printf "%04d" $((RANDOM % 10000)))"
BATCH_SIZE=2 # Maximum size in GB to be processed in one go, can be increased based on cluster config
TRAFFIC_DATA_PREFIX_PATH="data/pre-processed/traffic_data"
TAXI_DATA_PREFIX_PATH="data/pre-processed/traffic_data"

start_cluster(){
    gcloud dataproc clusters start $CLUSTER_NAME --region=$REGION
    echo "Starting the Dataproc cluster."
}

describe_cluster(){
    gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION
}

load_crashes_data(){
python data_pipelines/load/direct_data_load_bigquery.py --process-name crashes_data
}

load_persons_data(){
python data_pipelines/load/direct_data_load_bigquery.py --process-name persons_data
}

load_vehicles_data(){
python data_pipelines/load/direct_data_load_bigquery.py --process-name vehicles_data
}

load_weather_data(){
python data_pipelines/load/direct_data_load_bigquery.py --process-name weather_data
}

load_traffic_data(){
gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID --id=$TRAFFIC_PYSPARK_JOB_NAME --files=$CONFIG_FILE --bucket=$DATAPROC_BUCKET $TRAFFIC_DATA_FILE -- --batch-size $BATCH_SIZE --prefix-path $TRAFFIC_DATA_PREFIX_PATH
echo "Submitted PySpark job to Dataproc cluster."
}

load_taxi_data(){
gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID --id=$TAXI_PYSPARK_JOB_NAME --files=$CONFIG_FILE --bucket=$DATAPROC_BUCKET $TAXI_DATA_FILE -- --batch-size $BATCH_SIZE --prefix-path $TAXI_DATA_PREFIX_PATH
echo "Submitted PySpark job to Dataproc cluster."
}

stop_cluster(){
    gcloud dataproc clusters stop $CLUSTER_NAME --region=$REGION
    echo "Stopping the Dataproc cluster."
}

# Get user input for process selection
echo "Select a process to run:"
echo "1. Load Weather data"
echo "2. Load Crashes data"
echo "3. Load Persons data"
echo "4. Load Vehicles data"
echo "5. Load Traffic data"
echo "6. Load Taxi data"
read -p "Enter your choice (1-6): " choice

case $choice in
    1) load_weather_data ;;
    2) load_crashes_data ;;
    3) load_persons_data ;;
    4) load_vehicles_data ;;
    5)  submit_traffic_data_job
        stop_cluster
        exit 0
        ;;
    6) submit_taxi_data_job
        stop_cluster
        exit 0
        ;;
    *) echo "Invalid choice. Exiting."; exit 1 ;;
esac
