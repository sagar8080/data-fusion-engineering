
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
![Alt text](./architecture_diagram.svg)

1. **Data Ingestion via Cloud Functions**:
   - **Triggering Cloud Functions**: Leveraging *event-driven architecture* and *serverless computing*, Cloud Functions are scheduled by Cloud Scheduler to fetch data from various external APIs (e.g., Weather Data, Crashes Data, Persons Data, Vehicles Data, Taxi Data, Traffic Data).
   - **Storing in Landing Zone**: Ingested data is stored as CSV or JSON files in a pre-processed path within the landing zone in Cloud Storage, implementing the *data lake* paradigm.

2. **Data Storage**:
   - **Landing to RAW**: Using *distributed computing* and *in-memory processing*, PySpark scripts which are stored in the GCS buckets are referenced by shell scripts in the Code Bucket, run transformation and loading jobs. This process is automated using `CRONTAB` file available on all Linux Distros.
   - **Raw Dataset Storage**: Processed data is stored in a raw dataset in Cloud Storage, maintaining the *schema-on-read* approach.

3. **Data Transformation and Loading**:
   - **RAW to Prod Dataset**: Transformed data is loaded from the raw dataset to the production dataset, implementing *ETL (Extract, Transform, Load)* processes. This process is automated using `CRONTAB` file available on all Linux Distros.

4. **Data Monitoring and Management**:
   - **Data Catalog**: Utilizing *metadata management* and *data governance*, the Data Catalog monitors processes, tracks last fetched timestamps for incremental loading, and ensures data quality and consistency.

5. **Data Visualization**:
   - **Superset Dashboards**: *Data visualization* tools like Superset were used to create dashboards and charts, which are auto-refreshed at periodic intervals to provide analysis and insights into the data.

## Initial Setup

**Setup and Execution:**
- Make the script executable: 
```
chmod +x setup.sh
```
- Run the script: 
```
./setup.sh <GCP-PROJECT-ID> <GCP-SERVICE-ACCOUNT>
```

**Script Operations:**

**Initial Setup and Installation:**
- Takes two arguments: GCP project ID and service account name.
- Installs `jq` for handling JSON files on Ubuntu or WSL.
- Validates and installs `gcloud` CLI if not already installed.

**Google Cloud Authentication and Configuration:**
- Establishes service accounts and sets up default credentials.
- Creates a new service account with necessary permissions.
- Reuses previously generated credentials if the script is rerun.

**Environment Setup for Development:**
- Ensures Python and `virtualenv` are installed.
- Installs required Python libraries from `requirements.txt`.

**Infrastructure Setup and Management:**
- Automates creation of BigQuery datasets, tables, and GCS buckets.
- Generates a global configuration and uploads Cloud Functions code to GCS.
- Ensures Terraform is installed for infrastructure as code deployments.
- Exports necessary environment variables for Terraform.

**Resource Management:**
- Optionally clears existing resources for a clean state.
- Executes Terraform scripts to create cloud functions and schedulers for APIs.

**Cleanup and Configuration Management:**
- Removes configuration files from ingest location for security.
- Persists Terraform variables in the bash environment.

**Cleanup Bytecode Files:**
- Removes Python bytecode files (`pycache` and `.pyc`) to keep workspace clean.

**Continuous Integration and Continuous Deployment (CI/CD):**
- The process can be integrated into a CI/CD pipeline to trigger automatic re-deployments upon updates to the codebase.

![Setup process](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/6657d1d9-9637-4208-b0a7-4f42c87a5025)
|:--:|
| The infrastructure automation done using the `setup.sh` script |

![Python Dependencies on Local](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/50161218-3ae1-4421-a33e-01a83f68fe0b)
|:--:|
| It fullfills the standard system dependencies required to run the modules |

![Cloud Function Creation](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/adc1054c-1d98-49e8-a39e-3ac705bface9)
|:--:|
| Cloud Functions Creation |

![Cloud Function Setup](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/9f5cba9d-5423-4b93-b8ab-8332665620f1)
|:--:|
| Cloud Functions Fully Set Up |

![Cloud Scheduler Setup](./screenshots/cloud_scheduler.png)
|:--:|
| Cloud Schedulers are up as well |

![code_bucket](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/93015060-fd38-40fe-9d3a-81a1b37a543d)
|:--:|
| Code Bucket Creation |

![code_bucket_contents](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/3e0c39bd-4849-4189-803f-ef3997476188)
|:--:|
| Code Bucket Contents |

* `Note`: Other GCP infrastructure can also be automated using Terraform but we thought it would be better to create it manually so that we don't forget to shut it down.

## Ingest

These scripts are designed to run as a Google Cloud Function that automates the process of ingesting data from the APIs, processing it, and storing the results in Google Cloud Storage and BigQuery.

1. **Initialization**: The script begins by importing necessary libraries and loading configuration details from a JSON file. It initializes Google Cloud Storage and BigQuery clients and sets up constants for the process.

2. **Fetch Last Offset**: The `fetch_last_offset` function queries BigQuery to determine the most recent timestamp of successfully processed data, which helps in fetching new data incrementally from the API.

3. **Date Handling**: The `get_dates` function converts the retrieved timestamp into start and end dates for data extraction, ensuring that data is fetched in defined time increments (60 days by default).

4. **Data Fetching and Uploading**: The `fetch_data` function constructs an API call to retrieve crash data within the specified date range, while `upload_to_gcs` uploads the fetched data as a CSV file to Google Cloud Storage, handling potential errors gracefully.

5. **State Management and Execution**: The main `execute` function coordinates the entire process, from fetching data to uploading it, and storing the function's state in BigQuery. It captures the execution state, timestamps, and logs any errors, ensuring robust and trackable data processing.

![cloud_bucket_creation](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/2d50b988-904a-4e1e-9912-083e37903c39)
|:--:|
| Created GCS Cloud Buckets |

![landing_zone](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/ada93002-317f-46e9-ac64-e22a946888cb)

![landing_zone_contents - 1](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/4c9627b8-54b0-4e49-9f63-a2ff51c0882e)

![landing_zone_contents - 2](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/2dd53244-6959-42a9-840f-ee86e626bf9a)

![landing_zone_contents - 3](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/7f8af8c6-d24f-470f-b961-b97b74b61656)
|:--:|
| Data Ingestion into the Landing Zone |



## Load and Transform

Data from all sources is transformed into a cohesive data model using DataProc and PySpark. The transformation occurs bi-hourly, dovetailing with the ingest timing to ensure a balance between data freshness and system efficiency. During this stage, data is prepared for analysis, conforming to a relational schema that supports complex queries.



![Setting up DataProc Cluster](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/23fbb913-3a74-492b-9e3b-8edc95866cef)
|:--:|
| DataProc Cluster Set Up --- Configuration --- Leader Node: 1 N2-Standard-4 4CPU 16GB --- 2 Worker Nodes: 2 E2-Standard-4 4VCPU 16GB|

![Setting up Virtual Environment](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/7de095bd-479c-42e4-934c-80f8a0db6f14)
|:--:|
| Virtual Environment (Compute Engine) Set Up |

![DataProc Jobs Created](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/d801a323-a65b-4aa9-8d63-c4cb7d3ca664)
|:--:|
| Creating DataProc Jobs |

![Running Load to Raw Shell Script on VM](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/12dda142-4b48-4a6c-82a0-cf9878b60566)
|:--:|
| We intended to run "Landing Zone to Raw Zone Script" also on Virtual Machine (Compute Engine) using a CRON expression |

![Running Transformation Shell Script on VM](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/9d1f70e1-b21a-46ba-ad17-1ca1af67a97b)
|:--:|
| We inteded to run the "Transformation Script" on Virtual Machine (Compute Engine) using CRON expression |

![Transformation Job Successful](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/f75e6c6d-d377-41aa-88ea-36b6df0f0e66)
|:--:|
| Transformation Job Successful on Virtual Machine (Compute Engine) |

![cmd - run load to raw](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/d3b90923-52c2-47ff-934f-58c0f59e01b0)
|:--:|
| Running "Landing Zone to Raw Zone Script" on the Local Environment (Command Line) |

![cmd - run transformations](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/af36f240-4217-4509-9125-40d35ce94d7b)
| Running "Transformation Script" on the Local Environment (Command Line) |

![PYSPARK jobs](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/4adc64ee-f8b0-4296-bf1e-b9f9a9176ac2)
|:--:|
| PySpark Job Creation |

![pyspark execution](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/4f70cca7-5a5a-463b-a7ac-5dc23b484551)
|:--:|
| PySpark Job Execution Details |


## Storage

We use BigQuery as our primary storage technology, chosen for its seamless integration with DataProc and excellent support for SQL queries on large datasets. The database is structured to logically represent our data model, with separate tables for each data source that relate to one another through shared keys.

![pre-processed to processed](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/ee6be573-b3d8-4170-a580-3815afd4a3c3)
|:--:|
| Data Storage from Landing Zone (Pre-Processed) to Raw Zone (Processed) |

![bigquery_tables](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/b40667c9-0910-4c01-86de-e832df9ce6a1)
|:--:|
| BigQuery Tables |

## Analysis

- **Weather Statistics of NYC**
  - Analyzes weather statistics across different boroughs in NYC from 2009 - 2024

- **Fatality in High Impact Crashes**
  - Examines the frequency and distribution of fatalities in high impact crashes across different boroughs from 2016 to 2024.

- **Vehicles Involved in a Crash Year Over Year (YOY)**
  - Tracks the number of vehicles involved in crashes on a yearly basis.

- **Fatal Crashes in Weather Conditions**
  - Analyzes the distribution of fatal crashes under different weather conditions.

- **Body Injury and Status Post Crash**
  - Investigates the relationship between bodily injuries, emotional statuses, and age categories of individuals involved in crashes.

- **High Yielding Factors of Crashes**
  - Identifies the most common contributing factors leading to crashes.

- **Number of Persons Killed YoY**
  - Calculates the year-over-year count of persons killed in crashes across different boroughs.

- **Occurrence of High Impact Crashes**
  - Tracks the monthly occurrence of high impact crashes from 2016 to 2024.

- **Number of Persons Injured**
  - Computes the monthly count of persons injured in crashes from 2012 to 2024.

- **Contributing Factor for Crashes while Turning**
  - Identifies the number of crashes occurring while making various types of turns.

- **Categorizing Persons Involved in Crashes**
  - Categorizes individuals involved in crashes by their person type and age category.

- **Analyzing Traffic Speeds in NYC on Weekdays and Weekends**
  - Analyzes traffic speed patterns in NYC.

- **Trend of High Impact Crashes YoY**
  - Examines the yearly trend of high impact crashes across different boroughs.

- **Understanding Pre Crash Condition and Vehicle Damage Afterwards**
  - Explores the relationship between pre-crash conditions and the extent of vehicle damage post-crash.

- **State-wise Distribution of Crashes**
  - Analyzes the distribution of high impact crashes based on the state of vehicle registration from 2023 to 2024.

## Management

- Our GitHub repository follows best practices for code management. 
- Commits are small, logical, and contain clear messages. 
- All team members contribute meaningful commits. 
- Code is organized into folders that correspond to each project dimension (Ingest, Transformation, Storage, Analysis).

## Dashboard
![Alt text](./data-fusion-dashboard.jpg)
