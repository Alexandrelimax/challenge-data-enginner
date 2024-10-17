from google.cloud.bigquery import Client, LoadJobConfig, SourceFormat

class BigQueryLoader:
    def __init__(self, project_id):
        self.bigquery_client = Client(project=project_id)

    def load_parquet_to_bigquery(self, gcs_parquet_path, table_id, gcs_bucket_name):

        job_config = LoadJobConfig(source_format=SourceFormat.PARQUET)
        
        uri = f"gs://{gcs_bucket_name}/{gcs_parquet_path}"
        load_job = self.bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
        
        load_job.result()  # Waits for the job to complete
        print(f"Loaded {gcs_parquet_path} into {table_id} in BigQuery")
