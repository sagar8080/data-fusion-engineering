#!/bin/bash

################################# TO SET THIS ON A SCHEDULE; DO THIS: ###################################
#   crontab -e                                                                                          #
#   0 19 * * 1-5 path/to/script  -- Runs once daily                                                     #
#########################################################################################################


# Environment Variables
CONFIG_PATH="utils/config.json"
PROJECT_ID=$(gcloud config get-value project)

# Check if a project ID is set
if [ -z "$PROJECT_ID" ]; then
  echo "No Google Cloud project is set. Please set a project using 'gcloud config set project PROJECT_ID'."
  exit 1
fi

echo "Using project: Data Fusion"

CLUSTER_NAME="data-fusion"
REGION="us-east4"
CODE_BUCKET=$(jq -r '.code_bucket' "$CONFIG_PATH")
DATAPROC_BUCKET=$(jq -r '.dataproc_bucket' "$CONFIG_PATH")

# Script Paths
TRANSFORM_PERSONS_SCRIPT="gs://${CODE_BUCKET}/scripts/transform/transform_persons_data.py"
TRANSFORM_CRASHES_SCRIPT="gs://${CODE_BUCKET}/scripts/transform/transform_crashes_data.py"
TRANSFORM_VEHICLES_SCRIPT="gs://${CODE_BUCKET}/scripts/transform/transform_vehicles_data.py"
TRANSFORM_TRAFFIC_SCRIPT="gs://${CODE_BUCKET}/scripts/transform/transform_traffic_data.py"
TRANSFORM_WEATHER_SCRIPT="gs://${CODE_BUCKET}/scripts/transform/transform_weather_data.py"

# Job IDs
PERSONS_JOB_ID="transform-persons-data-$(printf "%04d" $((RANDOM % 10000)))"
CRASHES_JOB_ID="transform-crashes-data-$(printf "%04d" $((RANDOM % 10000)))"
VEHICLES_JOB_ID="transform-vehicles-data-$(printf "%04d" $((RANDOM % 10000)))"
TRAFFIC_JOB_ID="transform-traffic-data-$(printf "%04d" $((RANDOM % 10000)))"
WEATHER_JOB_ID="transform-weather-data-$(printf "%04d" $((RANDOM % 10000)))"

start_cluster(){
    gcloud dataproc clusters start $CLUSTER_NAME --region=$REGION
    echo "Starting the Dataproc cluster."
}

describe_cluster(){
    gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION
}

transform_data(){
    local script_path=$1
    local job_id=$2
    gcloud dataproc jobs submit pyspark $script_path \
        --cluster=$CLUSTER_NAME \
        --region=$REGION \
        --project=$PROJECT_ID \
        --id=$job_id \
        --files=$CONFIG_PATH \
        --bucket=$DATAPROC_BUCKET
    echo "Submitted PySpark job to Dataproc cluster with ID $job_id."
}

stop_cluster(){
    gcloud dataproc clusters stop $CLUSTER_NAME --region=$REGION
    echo "Stopping the Dataproc cluster."
}

start_cluster
transform_data $TRANSFORM_WEATHER_SCRIPT $WEATHER_JOB_ID
transform_data $TRANSFORM_CRASHES_SCRIPT $CRASHES_JOB_ID
transform_data $TRANSFORM_PERSONS_SCRIPT $PERSONS_JOB_ID
transform_data $TRANSFORM_VEHICLES_SCRIPT $VEHICLES_JOB_ID
transform_data $TRANSFORM_TRAFFIC_SCRIPT $TRAFFIC_JOB_ID    
stop_cluster ;;

