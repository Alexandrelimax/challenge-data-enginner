import asyncio
import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from include.api.api_fetch import APIFetch
from include.handlers.storage_handler import StorageManager


class APIFetchTaskGroup(TaskGroup):
    def __init__(self, group_id, datasets_info, gcs_bucket_name, **kwargs):
        super().__init__(group_id=group_id, **kwargs)

        base_url = "https://api.exemplo.com"
        api_fetcher = APIFetch(base_url)
        gcs_uploader = StorageManager(gcs_bucket_name)


        @task(task_group=self)
        def fetch_data_async(dataset):
            async def fetch_data(dataset):
                data = await api_fetcher.fetch_data(dataset["endpoint"])

                return {
                    "data": data,
                    "parquet_path": dataset["parquet_path"],
                    "table_id": dataset["table_id"],
                    "endpoint": dataset["endpoint"]
                }

            return asyncio.run(fetch_data(dataset))


        @task(task_group=self)
        def save_to_parquet_and_gcs(data_info):

            parquet_file_path = data_info['parquet_path']

            df = pd.DataFrame(data_info['data'])
            df.to_parquet(parquet_file_path, index=False)

            gcs_uploader.upload_file(parquet_file_path, parquet_file_path)

            return {
                'parquet_path': parquet_file_path,
                'table_id': data_info['table_id'],
                'endpoint': data_info['endpoint']
            }

        fetch_tasks = [fetch_data_async(dataset) for dataset in datasets_info]
        self.save_tasks = [save_to_parquet_and_gcs(fetch) for fetch in fetch_tasks]
