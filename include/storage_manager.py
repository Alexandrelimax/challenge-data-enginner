import pandas as pd
from google.cloud import storage
import os

class GCSUploader:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)

    def upload_to_gcs(self, source_file_path: str, destination_blob_name: str) -> None:
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)


    def convert_csv_to_parquet(self, csv_file_path: str, parquet_file_path: str) -> None:
        df = pd.read_csv(csv_file_path)
        df.to_parquet(parquet_file_path, index=False)

