# #!/bin/bash

# figlet -f slant "Data Fusion"
# # Environment Variables
# # Get the current Google Cloud project ID
# CONFIG_PATH="utils/config.json"
# PROJECT_ID=$(gcloud config get-value project)

# # Check if a project ID is set
# if [ -z "$PROJECT_ID" ]; then
#   echo "No Google Cloud project is set. Please set a project using 'gcloud config set project PROJECT_ID'."
#   exit 1
# fi

# # echo "Using project: $PROJECT_ID"
# CLUSTER_NAME="data-fusion"
# REGION="us-east4"
# CODE_BUCKET=$(jq -r '.code_bucket' "$CONFIG_PATH")
# DATAPOC_BUCKET=$(jq -r '.dataproc_bucket' "$CONFIG_PATH")

# TRANSFORM_CRASHES_SCRIPT="gs://df-code-bucket/scripts/transform/transform_crashes_data.py"
# TRANSFORM_PERSONS_SCRIPT="gs://df-code-bucket/scripts/transform/transform_persons_data.py"
# TRANSFORM_TRAFFIC_SCRIPT="gs://df-code-bucket/scripts/transform/transform_traffic_data.py"
# TRANSFORM_VEHICLES_SCRIPT="gs://df-code-bucket/scripts/transform/transform_vehicles_data.py"
# TRANSFORM_WEATHER_SCRIPT="gs://df-code-bucket/scripts/transform/transform_weather_data.py"

# PERSONS_JOB_ID="transform-persons-data-$(printf "%04d" $((RANDOM % 10000)))"
# CRASHES_JOB_ID="transform-crashes-data-$(printf "%04d" $((RANDOM % 10000)))"
# VEHICLES_JOB_ID="transform-vehicles-data-$(printf "%04d" $((RANDOM % 10000)))"
# TRAFFIC_JOB_ID="transform-traffic-data-$(printf "%04d" $((RANDOM % 10000)))"
# WEATHER_JOB_ID="transform-weather-data-$(printf "%04d" $((RANDOM % 10000)))"

# BATCH_SIZE=2 # Maximum size in GB to be processed in one go, can be increased based on cluster config


# start_cluster(){
#     gcloud dataproc clusters start $CLUSTER_NAME --region=$REGION
#     echo "Starting the Dataproc cluster."
# }

# describe_cluster(){
#     gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION
# }


# transform_persons_data(){
# gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID --id=$PERSONS_JOB_ID --files=$CONFIG_FILE --bucket=$DATAPROC_BUCKET $TRANSFORM_PERSONS_SCRIPT
# echo "Submitted PySpark job to Dataproc cluster."
# }

# transform_vehicles_data(){
# gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID --id=$VEHICLES_JOB_ID --files=$CONFIG_FILE --bucket=$DATAPROC_BUCKET $TRANSFORM_PERSONS_SCRIPT
# echo "Submitted PySpark job to Dataproc cluster."
# }

# transform_weather_data(){
# gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID --id=$VEHICLES_JOB_ID --files=$CONFIG_FILE --bucket=$DATAPROC_BUCKET $TRANSFORM_PERSONS_SCRIPT
# echo "Submitted PySpark job to Dataproc cluster."
# }

# transform_traffic_data(){
# gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID --id=$TRAFFIC_PYSPARK_JOB_NAME --files=$CONFIG_FILE --bucket=$DATAPROC_BUCKET $TRAFFIC_DATA_FILE -- --batch-size $BATCH_SIZE --prefix-path $TRAFFIC_DATA_PREFIX_PATH
# echo "Submitted PySpark job to Dataproc cluster."
# }

# transform_taxi_data(){
# echo "Submitted PySpark job to Dataproc cluster."
# }

# transform_crashes_data(){
# gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID --id=$CRASHES_PYSPARK_JOB_NAME --files=$CONFIG_FILE --bucket=$DATAPROC_BUCKET $CRASHES_DATA_FILE -- --batch-size $BATCH_SIZE --prefix-path $CRASHES_DATA_PREFIX_PATH
# echo "Submitted PySpark job to Dataproc cluster."
# }

# stop_cluster(){
#     gcloud dataproc clusters stop $CLUSTER_NAME --region=$REGION
#     echo "Stopping the Dataproc cluster."
# }

# # Get user input for process selection
# echo "Select a process to run:"
# echo "1. Load Weather data"
# echo "2. Load Crashes data"
# echo "3. Load Persons data"
# echo "4. Load Vehicles data"
# echo "5. Load Traffic data"
# echo "6. Load Taxi data"
# read -p "Enter your choice (1-6): " choice

# case $choice in
#     1) load_weather_data ;;
#     2)  start_cluster
#         load_crashes_data
#         stop_cluster
#         exit 0
#         ;;
#     3) load_persons_data ;;
#     4) load_vehicles_data ;;
#     5)  start_cluster
#         load_traffic_data
#         stop_cluster
#         exit 0
#         ;;
#     6)  start_cluster
#         load_taxi_data
#         stop_cluster
#         exit 0
#         ;;
#     *) echo "Invalid choice. Exiting."; exit 1 ;;
# esac

#!/bin/bash

figlet -f slant "Data Fusion"

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

# Get user input for process selection
echo "Select a process to run:"
echo "1. Transform Weather data"
echo "2. Transform Crashes data"
echo "3. Transform Persons data"
echo "4. Transform Vehicles data"
echo "5. Transform Traffic data"
echo "6. Transform Taxi data"
read -p "Enter your choice (1-6): " choice

case $choice in
    1)  start_cluster
        transform_data $TRANSFORM_WEATHER_SCRIPT $WEATHER_JOB_ID
        stop_cluster ;;
    2)  start_cluster
        transform_data $TRANSFORM_CRASHES_SCRIPT $CRASHES_JOB_ID
        stop_cluster ;;
    3)  start_cluster 
        transform_data $TRANSFORM_PERSONS_SCRIPT $PERSONS_JOB_ID
        stop_cluster ;;
    4)  start_cluster
        transform_data $TRANSFORM_VEHICLES_SCRIPT $VEHICLES_JOB_ID
        stop_cluster ;;
    5)  start_cluster
        transform_data $TRANSFORM_TRAFFIC_SCRIPT $TRAFFIC_JOB_ID
        stop_cluster ;;
    6)  start_cluster
        
        stop_cluster ;;
    *) echo "Invalid choice. Exiting."; exit 1 ;;
esac
