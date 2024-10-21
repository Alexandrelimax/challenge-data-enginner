from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from include.handlers.bigquery_handler import BigQueryLoader
from include.handlers.storage_handler import StorageManager


class SaveToBigQueryTaskGroup(TaskGroup):
    def __init__(self, group_id, files, gcs_bucket_name, **kwargs):
        super().__init__(group_id=group_id, **kwargs)

        @task(task_group=self)
        def save_parquet_to_bq(file):

            table_id = file['table_id']
            parquet_file = file['gcs_path']
            project_id = table_id.split('.')[0]

            loader = BigQueryLoader(project_id)
            loader.load_parquet_to_bigquery(parquet_file, table_id, gcs_bucket_name)
            return f"Successfully loaded {parquet_file} to {table_id}"


        self.save_tasks = [save_parquet_to_bq(parquet) for parquet in files]
