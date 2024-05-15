
# Analyzing the Impact of Weather, Traffic, and Taxi Usage on Road Safety in NYC

```
    ____          __            ______              _             
   / __ \ ____ _ / /_ ____ _   / ____/__  __ _____ (_)____   ____ 
  / / / // __ `// __// __ `/  / /_   / / / // ___// // __ \ / __ \
 / /_/ // /_/ // /_ / /_/ /  / __/  / /_/ /(__  )/ // /_/ // / / /
/_____/ \__,_/ \__/ \__,_/  /_/     \__,_//____//_/ \____//_/ /_/ 

- In the storm of data, we fuse insights for safer paths.
```

## Table of Contents

- [Background](#background)
- [Data Architecture](#data-architecture)
- [Initial Setup](#initial-setup)
- [Ingest](#ingest)
- [Load and Transform](#load-and-transform)
- [Storage](#storage)
- [Analysis](#analysis)
- [Management](#management)
- [Dashboard](#dashboard)
- [Key Takeaways](#key-takeaways)


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
- Weather Data -  https://open-meteo.com/
- TLC trip data - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Data Architecture
![Alt text](./architecture_diagram.svg)

1. **Data Ingestion via Cloud Functions**:
   - **Triggering Cloud Functions**: Leveraging *event-driven architecture* and *serverless computing*, Cloud Functions are scheduled by Cloud Scheduler to fetch data from various external APIs (e.g., Weather Data, Crashes Data, Persons Data, Vehicles Data, Taxi Data, Traffic Data).
   - **Storing in Landing Zone**: Ingested data is stored as CSV or JSON files in a pre-processed path within the landing zone in Cloud Storage, implementing the *data lake* paradigm.

2. **Data Storage**:
   - **Landing to RAW**: Using *distributed computing* and *in-memory processing*, PySpark scripts which are stored in the GCS buckets are referenced by shell scripts in the Code Bucket, run transformation and loading jobs. This process is automated using `CRONTAB` file available on all Linux Distros.
   - **Raw Dataset Storage**: Processed data is stored in a raw dataset in Cloud Storage, maintaining the *schema-on-read* approach.

3. **Data Transformation and Loading**:
   - **RAW to Prod Dataset**: Transformed data is loaded from the raw dataset to the production dataset, implementing *ETL (Extract, Transform, Load)* processes. This process is automated using `CRONTAB` file available on all Linux Distros.

4. **Scheduling**: 
    - The cloud functions are scheduled by cloud schedulers to run every hour between 9AM to 5PM EST from Monday to Friday.
    - The LANDING to RAW shell script is scheduled by crontab and cron expressions on Compute Engine to run at 6 PM EST from Monday to Friday.
    - The RAW to PROD shell script is scheduled by crontab and cron expressions on Compute Engine to run at 7 PM EST from Monday to Friday.

5. **Data Monitoring and Management**:
   - **Data Catalog**: Utilizing *metadata management* and *data governance*, the Data Catalog monitors processes, tracks last fetched timestamps for incremental loading, and ensures data quality and consistency.

6. **Data Visualization**:
   - We leveraged open-source `Apache Superset` installing it on compute engine and local systems, to create dashboards and charts, which are auto-refreshed at periodic intervals.

## Initial Setup

**Setup and Execution:**
- Make the script executable: 
```
chmod +x setup.sh
```
- Run the script: 
```
./setup.sh <GCP-PROJECT-ID> <GCP-SERVICE-ACCOUNT-NAME>
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
- Generates an unique global configuration and uploads Cloud Functions code to GCS.
- Ensures Terraform is installed for infrastructure as code deployments.
- Exports necessary environment variables for Terraform.

**Resource Management:**
- Optionally clears existing resources for a clean state.
- Executes Terraform scripts to create cloud functions and schedulers for APIs.

**Cleanup and Configuration Management:**
- Removes configuration files from ingest location for security.
- Persists Terraform variables in the bash environment.


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

1. **Data Proc Initialization**: Initialize a DataProc cluster within the Google Cloud (GCP) environment to enable effective communication between data nodes and facilitate batch processing.

2. **Virtual Machine Setup**: Set up a Google Compute Engine (GCE) VM equipped with necessary compute resources to host and execute data loading and transformation scripts, utilizing CRON expressions for robust scheduling across any infrastructure.

3. **Batch Loading**: Utilize the `run_load_to_raw.sh`  shell script for moving data from the landing zone into the raw data zone. This script manages the initialization of the cluster, batch loads data into the code bucket and BigQuery, and terminates the cluster post-successful execution. This script is also tested locally to ensure consistent execution across various environments. This Batch Loading Script is run once daily on a defined schedule using using `CRONTAB` file available on all Linux Distros.

4. **PySpark Job Creation**: Develop and deploy PySpark scripts on the DataProc cluster to handle more intricate data transformations and processing tasks.

5. **Transformation to Production Data**: The `run_transformation.sh` shell script is used to refine raw data into production-ready data sets. This script coordinates the execution of PySpark scripts and, similar to other scripts, is also tested locally to validate its performance in different environments. The Transformation Script is run once daily on a defined schedule `CRONTAB` file available on all Linux Distros.

6. **Monitoring & Optimization**: Continuously monitor the performance of both the DataProc cluster and the Compute Engine VM, as well as script executions, using GCP's monitoring tools. This monitoring assists in adjusting compute resources and optimizing script execution to enhance processing speed and resource efficiency.

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

1. **Data Structure Design**: The data within BigQuery is organized logically to represent the data model effectively. Each data source, such as crashes, traffic, persons, taxis, and vehicles, has separate tables which are further categorized as follows:

   **Landing Zone (Pre-Processed)**: 
   Initial raw data from various sources is stored directly as it is ingested. This includes separate folders for each type of data (e.g., crashes, persons, traffic) with a daily partitioning scheme, as seen with folders dated by each day (e.g., 2024-05-08, 2024-05-07, etc.). Data is segmented into daily batches within the `df_raw` bucket, allowing for effective date-based management and querying.

   **Processed Data Zone**:
   After initial ingestion and any required preprocessing, data is moved to the `df_prod` bucket, where it is ready for use in production environments. This processed data is still categorized by data type but optimized for performance and query efficiency. This data is further used for detailed analytics.

2. **Data Catalog**: Maintaining a process catalog (`df_process_catalog`).0 helps in managing metadata and ensuring that data governance and lineage are traceable.

3. **Automation & Scripting**: Automated scripts facilitate the data migration from the landing zone to the raw and processed zones as needed. Additionally, Python scripts automate direct data loading into BigQuery for weather, vehicles, and persons data, while PySpark scripts handle datatype casting. For traffic data, initially received in JSON format and requiring extra processing, we evaluated both Apache Beam and Apache Spark, ultimately choosing Spark as the more suitable solution for our needs.

![pre-processed to processed](https://github.com/sagar8080/data-fusion-engineering/assets/74659975/ee6be573-b3d8-4170-a580-3815afd4a3c3)
|:--:|
| Data Storage from Landing Zone (Pre-Processed) to Raw Zone (Processed) |

![bigquery_tables](./screenshots/prod_tables.png)
|:--:|
| BigQuery Tables structured in raw and production datasets |

![unified model preview](./screenshots/unified_crash_model.png)
|:--:|
| Unified Crash Data Table Preview |

## Analysis

### Combined Data Model

- The `df_unified_crashes_model` table aggregates crash data, persons data, and vehicles data into a unified schema.
- This includes specific transformations for data normalization and consolidation.

### Data Standardization
- In constructing the df_unified_crashes_model, we have created a robust data model that integrates and standardizes various datasets to provide a comprehensive view of vehicle crashes across New York City. 

- This model standardizes borough names to ensure consistency across datasets, facilitating accurate city-wide analyses.  

- Furthermore, the creation of additional views linking this unified crashes model to weather, traffic, and taxi data extends its utility, enabling complex queries that can assess the impact of environmental and traffic conditions on crash events. 

----

### In-Depth Analytics Performed

#### Weather Statistics of NYC
- **Question:** What are the average weather conditions in each borough of NYC between 2009 and 2024?
- **Answer:** The top section of the dashboard provides weather statistics across different boroughs, including temperature range, snowfall, rainfall, and humidity for the specified period.

#### Fatality in High Impact Crashes
- **Question:** How has the fatality rate in high impact crashes varied across different boroughs from 2016 to 2024?
- **Answer:** The line graph under the "Fatality in High Impact Crashes" section shows a trend of fatalities in high impact crashes across various boroughs, indicating a general decrease over the years.

#### Vehicles Involved in a Crash Year Over Year (YOY)
- **Question:** What is the year-over-year change in the number of vehicles involved in crashes?
- **Answer:** The "Number of Persons Killed YoY" graph also shows the number of vehicles involved in crashes, with a trend line illustrating yearly variations.

#### Fatal Crashes in Weather Conditions
- **Question:** How do weather conditions like rain, snow, and fog correlate with the incidence of fatal crashes?
- **Answer:** The colorful donut chart titled "Fatal Crashes in Weather Conditions" visually represents the correlation between different weather conditions and fatal crashes.

#### Body Injury and Status Post Crash
- **Question:** What patterns exist between the type of bodily injuries, emotional status, and age groups of crash victims?
- **Answer:** The network graph titled "Body Injury and Status Post Crash" visualizes the relationships between bodily injuries, emotional statuses, and age categories of individuals involved in crashes.

#### High Yielding Factors of Crashes
- **Question:** What are the top five contributing factors to crashes in the past five years?
- **Answer:** The diagram lists "High Yielding Factors of Crashes" like aggressive driving, failure to yield right-of-way, and driver inattention/distraction, showing their relative frequencies.

#### Number of Persons Killed YoY
- **Question:** What is the trend in the number of persons killed in vehicle crashes from year to year in each borough?
- **Answer:** The green line graph displays the year-over-year count of persons killed in crashes across different boroughs, showing a slight increase in recent months.

#### Occurrence of High Impact Crashes
- **Question:** How does the monthly occurrence of high impact crashes fluctuate from 2016 to 2024?
- **Answer:** A significant numeric display indicates "46 high impact crashes in May 2024," noting a dramatic 94.6% decrease over the past 12 months.

#### Number of Persons Injured
- **Question:** What is the monthly trend in the number of persons injured in crashes from 2012 to 2024?
- **Answer:** A similar green line graph to the fatalities shows the monthly count of persons injured in crashes from 2012 to 2024, noting an 8.1% increase in the last 12 months.

#### Contributing Factor for Crashes while Turning
- **Question:** What are the most common contributing factors for crashes that occur during turning maneuvers?
- **Answer:** The large number "47,557" under "High Impact Crashes While Turning" identifies the number of crashes that occurred during turning maneuvers.

#### Categorizing Persons Involved in Crashes
- **Question:** How are the demographics (such as age and person type) distributed among those involved in crashes?
- **Answer:** A bar graph categorizes individuals involved in crashes by their person type and age category, highlighting predominant age groups and person types.

#### Analyzing Traffic Speeds in NYC on Weekdays and Weekends
- **Question:** What are the differences in average traffic speeds between weekdays and weekends in NYC?
- **Answer:** The area graph titled "Traffic Speed in NYC on Weekdays and Weekends" analyzes differences in traffic speeds, clearly indicating variations between these times.

#### Trend of High Impact Crashes YoY
- **Question:** Is there a noticeable trend in high impact crashes year over year across different boroughs?
- **Answer:** A graph shows the yearly trend of high impact crashes across different boroughs, with some peaks and valleys over the years.

#### Understanding Pre Crash Condition and Vehicle Damage Afterwards
- **Question:** Is there a correlation between pre-crash conditions and the severity of vehicle damage following a crash?
- **Answer:** The circular network graph at the bottom of the dashboard could suggest the relationship between pre-crash conditions and the extent of vehicle damage following a crash.

#### State-wise Distribution of Crashes
- **Question:** How are high impact crashes distributed across different states based on vehicle registration from 2023 to 2024?
- **Answer:** A pie chart shows the state-wise distribution of high impact crashes, focusing on the state of vehicle registration from 2023 to 2024.

## Management

- Our GitHub repository follows best practices for code management. 
- Commits are small, logical, and contain clear messages. 
- All team members contribute meaningful commits. 
- Code is organized into folders that correspond to each project dimension (Ingest, Transformation, Storage, Analysis).

## Dashboard
![Alt text](./data-fusion-dashboard.jpg)

## Key Takeaways

- **BigQuery**: Enabled powerful and scalable analytics on large datasets. Also, seamlessly allowed us to perform direct load operations for some of the datasets, saving time.
- **Cloud Storage**: Provided secure and durable storage for our data without incurring a lot of cost.
- **DataProc**: Facilitated efficient data processing with SPARK and also provided a JUPYTERLAB interface to perform some quick testing off the shelf.
- **Cloud Functions**: Managed serverless operations helped us to extract data from the APIs as quickly as possible.
- **Cloud Schedulers**: Automated our cloud functions to run once every hour on weekdays between 9 to 5.

- **Opting for Simplicity in Scheduling**: Initially, we considered using Cloud Composer to orchestrate raw and prod loading. However, we realized that for our project's scale and complexity, Shell Scripts and Crontabs provided a more straightforward and equally effective solution. This approach allowed for precise control and scheduling flexibility without the overhead of managing an additional orchestration tool.

- **Intelligent Data Batching**: To manage system resources efficiently and avoid overloading, we implemented intelligent data batching in chunks of 2-3 GB. This strategy ensured smooth and uninterrupted data processing.

- **Choosing PySpark Over BEAM**: We opted for PySpark over BEAM due to its superior in-memory processing speed, which significantly boosted our data handling capabilities, especially when processing large JSON files. BEAM could be a better option if we intended to use a streaming data source, but for batch workloads, SPARK reigns supreme.

- **Data Cataloging Improvement**: We noticed frequent timeouts with traffic data despite a 600-second timeout limit. This was due to inefficient paging and fetching offsets from the API. By modifying our ingestion code to fetch data based on timestamps (30-40 days at a time), we improved our system's performance and successfully fetched high volumes of data. Here are the stats for our processes:

  1. `ingest-traffic-data`: 95.71 seconds
  2. `ingest-vehicles-data`: 51.30 seconds
  3. `ingest-persons-data`: 28.48 seconds
  4. `ingest-crashes-data`: 8.55 seconds
  5. `ingest-taxi-data`: 6.22 seconds
  6. `ingest-weather-data`: 3.31 seconds

- **Holistic Data Integration**: By integrating diverse datasets — weather, traffic, taxi, and crash data — we were able to gain comprehensive insights into the factors influencing road safety in NYC. This integration helped us uncover important trends and correlations.

- **Insightful Visualizations with Dynamic Dashboards**: We leveraged Superset to create dynamic dashboards primarily due to its ease of features and quick integration with Bigquery. This allowed us to build a pretty dashboard for our MBA friends to get wowed about.
