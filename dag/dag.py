from airflow import DAG
from airflow.providers.google.cloud.dataproc import (
    DataprocClusterCreateOperator, DataprocClusterDeleteOperator, 
    DataprocClusterStartOperator, DataprocClusterStopOperator, DataProcSparkOperator
    )
from airflow.utils.dates import days_ago
from datetime import timedelta

# Set default arguments
default_args = {
    'owner': 'sagar.das',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dataproc_spark_jobs',
    default_args=default_args,
    description='A DAG to run data fusion Spark jobs on Dataproc',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['dataproc']
)

# Define cluster configuration
cluster_config = {
    'master': {
        'num_instances': 1,
        'machine_type': 'n1-standard-2',
        'disk_size_gb': 30
    },
    'worker': {
        'num_instances': 2,
        'machine_type': 'n1-standard-2',
        'disk_size_gb': 30
    }
}

# Create Dataproc cluster
create_cluster = DataprocClusterCreateOperator(
    task_id='create_cluster',
    cluster_name='df-dataproc-cluster',
    project_id='project-id',
    region='us-central1',
    num_workers=2,
    storage_bucket='df-dataproc-utils',
    zone='us-central1-a',
    autoscaling_policy=None,
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    init_actions_uris=None,
    image_version=None,
    metadata=None,
    properties=None,
    tags=None,
    google_cloud_conn_id='google_cloud_default',
    dag=dag
)

# Start Dataproc cluster
start_cluster = DataprocClusterStartOperator(
    task_id='start_cluster',
    project_id='your-project-id',
    region='us-central1',
    cluster_name='dataproc-cluster',
    dag=dag
)

# Define Spark jobs
spark_job_1 = DataProcSparkOperator(
    task_id='spark_job_1',
    main_class='your.main.class.1',
    arguments=['arg1', 'arg2'],
    job_name='spark-job-1',
    cluster_name='dataproc-cluster',
    region='us-central1',
    dataproc_spark_properties=None,
    dataproc_spark_jars=None,
    dataproc_spark_files=None,
    dataproc_spark_jars_pypi=None,
    dataproc_spark_jars_extras=None,
    dataproc_pyspark_python=None,
    dataproc_pyspark_jars=None,
    dataproc_pyspark_files=None,
    dataproc_pyspark_dist_files=None,
    dataproc_pyspark_dist_archives=None,
    gcp_conn_id='google_cloud_default',
    delegate_to=None,
    dag=dag
)

spark_job_2 = DataProcSparkOperator(
    task_id='spark_job_2',
    main_class='your.main.class.2',
    arguments=['arg1', 'arg2'],
    job_name='spark-job-2',
    cluster_name='dataproc-cluster',
    region='us-central1',
    dataproc_spark_properties=None,
    dataproc_spark_jars=None,
    dataproc_spark_files=None,
    dataproc_spark_jars_pypi=None,
    dataproc_spark_jars_extras=None,
    dataproc_pyspark_python=None,
    dataproc_pyspark_jars=None,
    dataproc_pyspark_files=None,
    dataproc_pyspark_dist_files=None,
    dataproc_pyspark_dist_archives=None,
    gcp_conn_id='google_cloud_default',
    delegate_to=None,
    dag=dag
)

spark_job_3 = DataProcSparkOperator(
    task_id='spark_job_3',
    main_class='your.main.class.3',
    arguments=['arg1', 'arg2'],
    job_name='spark-job-3',
    cluster_name='dataproc-cluster',
    region='us-central1',
    dataproc_spark_properties=None,
    dataproc_spark_jars=None,
    dataproc_spark_files=None,
    dataproc_spark_jars_pypi=None,
    dataproc_spark_jars_extras=None,
    dataproc_pyspark_python=None,
    dataproc_pyspark_jars=None,
    dataproc_pyspark_files=None,
    dataproc_pyspark_dist_files=None,
    dataproc_pyspark_dist_archives=None,
    gcp_conn_id='google_cloud_default',
    delegate_to=None,
    dag=dag
)

# Stop Dataproc cluster
stop_cluster = DataprocClusterStopOperator(
    task_id='stop_cluster',
    project_id='your-project-id',
    region='us-central1',
    cluster_name='dataproc-cluster',
    dag=dag
)

# Delete Dataproc cluster
delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_cluster',
    project_id='your-project-id',
    region='us-central1',
    cluster_name='dataproc-cluster',
    dag=dag
)

# DAG workflow
create_cluster >> start_cluster >> spark_job_1 >> spark_job_2 >> spark_job_3 >> stop_cluster >> delete_cluster
