
# Analyzing the Impact of Weather, Traffic, and Taxi Usage on Road Safety in NYC

## Table of Contents
- [Directory Structure](#directory-structure)
- [Background](#background)
- [Application Architecture](#application-architecture)
- [Data Engineering Roadmap](#data-engineering-roadmap)
- [Ingest](#ingest)
- [Transformation](#transformation)
- [Storage](#storage)
- [Analysis](#analysis)
- [Management](#management)
- [Screenshots](#screenshots)


## Directory Structure

The repository is organized into several directories, each with a specific purpose in the overall project architecture. 
Here is an overview of the top-level structure and its contents:

```
.
├── README.md
├── dag
│   └── dag.py
├── data_pipelines
│   ├── landing_to_stage.py
│   ├── prod_to_analysis.py
│   └── stage_to_raw_and_prod.py
├── dir_structure.txt
├── first_steps.md
├── ingest
│   ├── crashes_data
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── persons_data
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── traffic_data
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── vehicles_data
│   │   ├── main.py
│   │   └── requirements.txt
│   └── weather_data
│       ├── main.py
│       └── requirements.txt
├── main.tf
├── test.py
├── utils
│   ├── bigquery.py
│   ├── dataproc.py
│   └── zip_files.py
├── variables.tf
```

- `README.md`: The comprehensive guide documenting the purpose, structure, and usage of this repository.
- `dag/`: Contains the `dag.py` script, which orchestrates the data processing workflows as Directed Acyclic Graphs (DAGs).
- `data_pipelines/`: Scripts for data processing pipelines are located here, including the landing to staging (`landing_to_stage.py`), staging to raw and production (`stage_to_raw_and_prod.py`), and production to analysis (`prod_to_analysis.py`).
- `dir_structure.txt`: A text file containing the directory structure of the project, for quick reference or documentation purposes.
- `first_steps.md`: A markdown file that contains instructions or a guide for initial steps in setting up or understanding the project.
- `ingest/`: This directory contains subdirectories for each data source, such as crashes, persons, traffic, vehicles, and weather data. Each subdirectory includes a `main.py` script for ingesting data from its respective source and a `requirements.txt` file specifying the Python package dependencies. This is essential for cloud functions to be deployed and run appropriately individually.
- `main.tf`: The main Terraform configuration file that defines the infrastructure as code for provisioning resources on Google Cloud.
- `utils/`: Utility scripts such as `bigquery.py` for interacting with BigQuery, `dataproc.py` for working with DataProc services, and `zip_files.py`, which can be used for compressing data files.
- `variables.tf`: Terraform configuration file that defines variables used across the Terraform files in the project.

## Background

Understanding the dynamics of road safety in New York City is crucial for implementing effective traffic management strategies and ensuring public safety. This project integrates multiple datasets to analyze the relationship between weather, traffic, taxi usage, and road safety in NYC. The objective is to uncover patterns, correlations, and potential causal relationships that can inform stakeholders and aid in risk mitigation associated with adverse conditions.

### Business Problem

The interplay between weather conditions, traffic patterns, and taxi usage plays a significant role in road safety incidents. This project seeks to provide stakeholders with insights into these factors to potentially reduce risks and enhance public safety measures.

### Project Description

* This project aims to design a comprehensive data system that integrates multiple datasets to analyze the relationship between weather, traffic, taxi usage, and road safety in NYC. 
* The datasets utilized include DOT Traffic Speeds, Motor Vehicle Collisions (Persons), Motor Vehicle Collisions (Vehicles), weather data from OpenWeatherMap API, and TLC trip data from the NYC TLC Trip Records. 
* By merging these datasets, we seek to investigate how various weather conditions, traffic densities, and taxi activities impact road safety metrics such as the frequency and severity of motor vehicle collisions. 
* Through rigorous data analysis and visualization techniques, we aim to uncover patterns, correlations, and potential causal relationships.

### Datasets:
- DoT Traffic Speeds- https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds-NBE/i4gi-tjb9/about_data
- Motor Vehicle Collisions (Persons) - https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu/about_data
- Motor Vehicle Collisions (Vehicles) - https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4/about_data
- Motor Vehicle Collisions (Crashes) - https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data
- Weather Data -  https://openweathermap.org/api
- TLC trip data - https://registry.opendata.aws/nyc-tlc-trip-records-pds/

## Application Architecture
![Alt text](./GCP%20-%20data%20fusion%20engineering.jpg)

## Data Engineering Roadmap

### Initial Cloud Setup

The process begins by running the file `setup.sh`. In order to reproduce this architecture on GCP make the script executable by performing `chmod +x setup.sh` 
and run the file as `./setup.sh <GCP-PROJECT-ID> <GCP-SERVICE-ACCOUNT>` in the current working directory. Here is a breakdown of he detailed shell script operations that provides a complete view of the automated deployment and management process that supports the data architecture.

- Initial Setup and Installation:
    - The script takes in 2 arguments: `GCP project id` which needs to be created prior installation and a `service account name` which may or may not be created prior to execution.
    - The script begins by installing jq on Ubuntu or WSL (Windows Subsystem for Linux) to handle JSON files, crucial for reading configuration files.
    - Validates if the gcloud CLI is installed, and installs it if absent, ensuring tools necessary for interacting with Google Cloud services are available.
- Google Cloud Authentication and Configuration:
    - Establishes service accounts and sets up application default credentials to ensure seamless authentication and authorization with Google Cloud services.
    - Specifically creates a new service account, automating permissions and credential setup necessary for the cloud resources. 
    - If this script is run multiple times, it 
- Environment Setup for Development:
    - Ensures Python and virtualenv are installed, setting up a controlled and consistent development environment.
    - Automatically installs necessary Python libraries from requirements.txt, ensuring all dependencies are satisfied for the scripts to run.
- Infrastructure Setup and Management:
    - Automates the creation of BigQuery datasets, tables, and GCS buckets. It also handles the generation of a global configuration and zips and uploads Cloud Functions (CFN) code to a GCS code bucket dynamically.
    - If a config file is already generated in previous installation, it uses that config file and skips the creation of resources.
    - All the GCP buckets created will have a `6 digit  unique id` in order to maintain bucket naming standards which states that names should be globally unique
    - Config file generated will vary from user to user
    - Ensures Terraform is installed, facilitating infrastructure as code deployments that are reproducible and consistent.
    - Exports necessary environment variables for Terraform, enabling it to manage cloud resources based on the defined configurations.
- Resource Management:
    - Optionally clears existing resources to ensure a clean state for deployments, which can be crucial for managing cloud costs and avoiding configuration drift.
    - Executes Terraform scripts to create cloud functions and cloud schedulers for each of the APIs, automating the deployment of the entire cloud infrastructure.
- Cleanup and Configuration Management:
    - Removes configuration files from the ingest location to prevent repetition and maintain security by ensuring sensitive information is not left in accessible locations.
    - Persists Terraform variables in the bash environment to ensure they are available across sessions, enhancing the usability of the script in persistent environments.
- Cleanup Bytecode Files: 
    - Removes Python bytecode files (pycache and .pyc files), keeping the workspace clean and ensuring that stale bytecode does not interfere with development.
- Continuous Integration and Continuous Deployment (CI/CD): 
    - The entire process can be part of a CI/CD pipeline, ensuring that updates to the codebase in the Git repository automatically trigger re-deployments, maintaining the sync between the code and the deployed infrastructure.

### PROCESS FLOW
The provided data architecture diagram and process flow outline a comprehensive data engineering solution designed to handle data ingestion, processing, and analysis within a Google Cloud Platform (GCP) environment. This solution leverages a variety of GCP services such as:

1. `CLOUD FUNCTIONS` - for batch extraction of data from APIs
2. `CLOUD SCHEDULER` - for executing cloud functions at pre-defined intervals using a cron expression
3. `CLOUD STORAGE BUCKETS` - for storing the data extracted from APIs serving as a landing zone layer
3. `BIGQUERY` - for storing the metadata, raw, staging, and production data
4. `DATAPROC` - for processing data in cloud storage buckets and loading into bigquery tables
5. `CLOUD COMPOSER (AIRFLOW)` - for orchestrating spark jobs that perform transformation and aggregation of raw data
6. `LOOKER` - BI tool to build dashboards

Here's a detailed exploration and explanation of the data engineering roadmap based on the outlined processes:

- Data Ingestion:
    - `External APIs to Cloud Storage:` The process begins with the ingestion of data from six different external APIs, each potentially representing different data domains such as traffic, crashes, vehicles, persons, weather, and TLC trip data. Cloud Functions are used to fetch data from these APIs periodically, triggered by Cloud Scheduler instances. This setup ensures that data fetching is automated and occurs at regular intervals.
    - `Automation and Deployment:` The deployment of Cloud Functions is automated using a shell script that pulls the latest code from a Git repository and uses Terraform for infrastructure provisioning and management. This approach ensures that any updates to the function logic or configurations can be centrally managed and version-controlled.

- Data Processing Orchestration:
    - `Dataproc and Spark Jobs:` A Directed Acyclic Graph (DAG)-based workflow orchestrates the creation of dynamic Dataproc clusters and the execution of three Apache Spark jobs. This setup leverages the scalability and flexibility of Dataproc for processing large datasets efficiently.

- Staging and Raw Data Management:
    - `Spark Job 1:` The first Spark job processes the data fetched from the landing zone in Cloud Storage and writes it into a staging table in BigQuery. This step might include preliminary cleaning and structuring of the data.
    - `Spark Job 2:` The second Spark job transfers data from the staging table to a raw table in BigQuery. This raw table acts as a static source of truth and includes transformations necessary to prepare the data for downstream analysis and reporting.

- Production Data Handling:
    - `Spark Job 3:` This job is responsible for transforming data from the raw tables into a production-ready format. It loads the data into an unified data model in BigQuery, performing aggregations, metric calculations, and any other necessary data transformations to support analytical needs.

- Data Analysis and Reporting:
    - `Business Intelligence Tools:` Data stored in BigQuery is then used as a source for BI tools such as Looker or Apache Superset. These tools are used to create interactive dashboards and reports that provide insights into the data, supporting business decisions and operational efficiency.

- Metadata Management and Incremental Loading:
    - `Data Catalogs:` Metadata for each execution, including job performance metrics and the last offset fetched from each API, is stored in a data catalog. This catalog not only helps in maintaining an audit trail of data operations but also supports incremental data loading by storing the last state of data fetched. This mechanism reduces redundancy, improves efficiency, and ensures that only new or changed data is fetched in subsequent runs.


## Ingest

The ingest stage involves retrieving data from the following external sources:
- DOT Traffic Speeds API
- NYC Open Data APIs for Motor Vehicle Collisions (Persons, Vehicles, Crashes)
- OpenWeatherMap API for weather data
- TLC trip data

The ingest process is automated via Python scripts using standard libraries like `requests` for API calls. The scripts are deployed on Google Cloud using Cloud Functions and trigger on a schedule that ensures data is refreshed every hour.

## Transformation

Data from all sources is transformed into a cohesive data model using DataProc and PySpark. The transformation occurs bi-hourly, dovetailing with the ingest timing to ensure a balance between data freshness and system efficiency. During this stage, data is prepared for analysis, conforming to a relational schema that supports complex queries.

## Storage

We use BigQuery as our primary storage technology, chosen for its seamless integration with DataProc and excellent support for SQL queries on large datasets. The database is structured to logically represent our data model, with separate tables for each data source that relate to one another through shared keys.

## Analysis

Our analytical framework consists of predefined SQL queries to explore various hypotheses about traffic congestion, driver behavior, and the impact of weather on taxi demand. Each query corresponds to a potential insight or pattern that could be derived from the combined data.

## Data Analysis:
- **Traffic Congestion Analysis**: 
  - Utilize taxi trip data to identify areas with the highest congestion during different weather conditions and times of day. 
  - Analyze how traffic speeds correlate with the volume of taxi trips and the severity of collisions.
- **Driver Behavior and Road Safety**: 
  - Uncover how weather conditions influence taxi driver behavior and its impact on road safety and correlate them with collision data.
- **Impact of Inclement Weather on Taxi Demand**: 
  - Examine how changes in weather conditions affect taxi demand across different parts of NYC. 
  - Analyze whether there's a correlation between precipitation, temperature, and the frequency of taxi trips.
- **Route Optimization and Traffic Predictions**: 
  - Develop predictive models to forecast future traffic congestion based on weather forecasts and historical taxi patterns.

## Management

Our GitHub repository follows best practices for code management. Commits are small, logical, and contain clear messages. All team members contribute meaningful commits. Code is organized into folders that correspond to each project dimension (Ingest, Transformation, Storage, Analysis).

## Screenshots
* To be updated