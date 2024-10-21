from datetime import datetime

from airflow.decorators import dag
from include.taskgroups.api_fetch_task_group import APIFetchTaskGroup
from include.taskgroups.load_bq_task_group import SaveToBigQueryTaskGroup
from include.data.datasets_info import datasets_info


@dag(
    dag_id="api_to_parquet_gcs_bq_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
)


def csv_to_bq_dag():

    api_fetch_task_group = APIFetchTaskGroup(
        group_id="fetch_api_data_to_parquet_and_gcs",
        datasets_info=datasets_info,
        gcs_bucket_name="my-gcs-bucket"
    )

    load_to_bq_task_group = SaveToBigQueryTaskGroup(
        group_id="load_to_bq",
        parquet_files=api_fetch_task_group.save_tasks,
        gcs_bucket_name="my-gcs-bucket"
    )


csv_to_bq_dag()
