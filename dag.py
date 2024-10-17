import os
from datetime import datetime

from airflow.decorators import task, task_group

from include.storage_manager import GCSUploader
from include.bigquery_manager import BigQueryLoader
from config import CSV_CONFIG


@dag(schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False)
def process_csv_pipeline_with_task_groups():


    @task
    def upload_csv_to_gcs(csv_filename):
        gcs_uploader = GCSUploader(gcs_bucket_name="nome_do_bucket")
        csv_file_path = f"caminho/dos/csvs/{csv_filename}"
        parquet_file_path = csv_file_path.replace('.csv', '.parquet')
        
        parquet_file = gcs_uploader.convert_csv_to_parquet(csv_file_path, parquet_file_path)
        
        gcs_path = gcs_uploader.upload_to_gcs(parquet_file, os.path.basename(parquet_file))
        
        return gcs_path


    @task
    def load_parquet_to_bigquery(gcs_parquet_path, table_id):
        bigquery_loader = BigQueryLoader(project_id="id_do_projeto")

        bigquery_loader.load_parquet_to_bigquery(gcs_parquet_path, table_id, "nome_do_bucket")
    

    @task_group
    def process_all_csvs():
        gcs_upload_customers = upload_csv_to_gcs(CSV_CONFIG['olist_customers']['file_csv'])
        load_parquet_to_bigquery(gcs_upload_customers, CSV_CONFIG['olist_customers']['table_id'])

        gcs_upload_orders = upload_csv_to_gcs(CSV_CONFIG['olist_orders']['file_csv'])
        load_parquet_to_bigquery(gcs_upload_orders, CSV_CONFIG['olist_orders']['table_id'])

        gcs_upload_geolocation = upload_csv_to_gcs(CSV_CONFIG['olist_geolocation']['file_csv'])
        load_parquet_to_bigquery(gcs_upload_geolocation, CSV_CONFIG['olist_geolocation']['table_id'])

        gcs_upload_products = upload_csv_to_gcs(CSV_CONFIG['olist_products']['file_csv'])
        load_parquet_to_bigquery(gcs_upload_products, CSV_CONFIG['olist_products']['table_id'])

        gcs_upload_order_items = upload_csv_to_gcs(CSV_CONFIG['olist_order_items']['file_csv'])
        load_parquet_to_bigquery(gcs_upload_order_items, CSV_CONFIG['olist_order_items']['table_id'])

        gcs_upload_order_payments = upload_csv_to_gcs(CSV_CONFIG['olist_order_payments']['file_csv'])
        load_parquet_to_bigquery(gcs_upload_order_payments, CSV_CONFIG['olist_order_payments']['table_id'])

        gcs_upload_order_reviews = upload_csv_to_gcs(CSV_CONFIG['olist_order_reviews']['file_csv'])
        load_parquet_to_bigquery(gcs_upload_order_reviews, CSV_CONFIG['olist_order_reviews']['table_id'])


    process_all_csvs()

# Instanciando o DAG
dag = process_csv_pipeline_with_task_groups()
