import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from include.handlers.storage_handler import StorageManager


class CSVToParquetTaskGroup(TaskGroup):
    def __init__(self, group_id, csv_files, gcs_bucket_name, **kwargs):
        super().__init__(group_id=group_id, **kwargs)

        @task(task_group=self)
        def transform_csv(file):
            df = pd.read_csv(file['csv_path'])
            df.to_parquet(file['parquet_path'])

            return file

        @task(task_group=self)
        def upload_to_gcs(file):
            uploader = StorageManager(gcs_bucket_name)
            uploader.upload_file(file['parquet_path'], file['parquet_path'])
            return file


        parquet_files = [transform_csv(csv) for csv in csv_files]
        self.upload_tasks = [upload_to_gcs(parquet) for parquet in parquet_files]
