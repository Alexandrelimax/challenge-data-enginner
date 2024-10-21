from google.cloud import storage

class StorageManager:
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_file(self, local_file_path, destination_blob_name):

        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"File {local_file_path} uploaded to {destination_blob_name}.")
