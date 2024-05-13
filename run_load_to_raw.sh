#!/bin/bash

# Set environment variables
CONFIG_PATH="utils/config.json"
PROJECT_ID=$(gcloud config get-value project)

# Check if a Google Cloud project ID is set
if [ -z "$PROJECT_ID" ]; then
  echo "No Google Cloud project is set. Please set a project using 'gcloud config set project PROJECT_ID'."
  exit 1
fi

CLUSTER_NAME="data-fusion"
REGION="us-east4"
CODE_BUCKET=$(jq -r '.code_bucket' "$CONFIG_PATH")
DATAPROC_BUCKET=$(jq -r '.dataproc_bucket' "$CONFIG_PATH")

# PySpark job files and names
declare -A PYSPARK_JOBS=(
  [traffic]="load_traffic_data_pyspark.py"
  [taxi]="load_taxi_data_pyspark.py"
  [crashes]="load_crashes_data_pyspark.py"
)

BATCH_SIZE=2  # Maximum size in GB to be processed in one go

start_cluster() {
    echo "Starting the Dataproc cluster..."
    gcloud dataproc clusters start $CLUSTER_NAME --region=$REGION
}

stop_cluster() {
    echo "Stopping the Dataproc cluster..."
    gcloud dataproc clusters stop $CLUSTER_NAME --region=$REGION
}

submit_job() {
  local job_type=$1
  local job_file="gs://${CODE_BUCKET}/scripts/load/$2"
  local job_name="load-$1-data-raw-$(printf "%04d" $((RANDOM % 10000)))"
  local prefix_path="data/pre-processed/${1}_data"
  echo "$job_file"
  echo "Submitting PySpark job for $job_type..."
  gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID \
      --id=$job_name --files=$CONFIG_PATH --bucket=$DATAPROC_BUCKET $job_file -- --batch-size $BATCH_SIZE --prefix-path $prefix_path
}

direct_load_to_bigquery() {
  local process_name=$1
  echo "Loading $process_name to BIGQUERY directly"
  python load/load_weather_persons_vehicles_data.py --process-name $process_name
}

load_data() {
    direct_load_to_bigquery "weather_data"
    direct_load_to_bigquery "persons_data"
    direct_load_to_bigquery "vehicles_data"
    start_cluster
    for type in "${!PYSPARK_JOBS[@]}"; do
        submit_job $type ${PYSPARK_JOBS[$type]}
    done
    stop_cluster
}

load_data
