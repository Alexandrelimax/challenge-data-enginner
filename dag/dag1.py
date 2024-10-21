from datetime import datetime

from airflow.decorators import dag, task
from include.taskgroups.csv_to_parquet_group import CSVToParquetTaskGroup
from include.taskgroups.load_bq_task_group import SaveToBigQueryTaskGroup
from include.data.csv_files import csv_files


@dag(
    dag_id="csv_to_bq_with_gcs_pandas",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
)


def csv_to_bq_dag():

    transform_task_group = CSVToParquetTaskGroup(
        group_id="transform_csv_to_parquet", 
        csv_files=csv_files, 
        gcs_bucket_name="my-gcs-bucket"
    )
    
    save_to_bq_task_group = SaveToBigQueryTaskGroup(
        group_id="save_to_bq", 
        files=transform_task_group.upload_tasks,
        gcs_bucket_name="my-gcs-bucket"
    )



csv_to_bq_dag()