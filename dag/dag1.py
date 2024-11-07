from datetime import datetime
from airflow.decorators import dag
from include.taskgroups.csv_to_parquet_group import CSVToParquetTaskGroup
from include.taskgroups.load_bq_task_group import SaveToBigQueryTaskGroup
from include.taskgroups.dataform_task_groups import DataformWorkflowTaskGroup
from include.config.csv_files import csv_files

@dag(
    dag_id="pipeline_medallion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
)
def csv_to_bq_dag():
    # TaskGroup para transformar CSV em Parquet e fazer upload para o GCS
    transform_task_group = CSVToParquetTaskGroup(
        group_id="transform_csv_to_parquet", 
        csv_files=csv_files, 
        gcs_bucket_name="my-gcs-bucket"
    )
    
    # TaskGroup para salvar os Parquet no BigQuery
    save_to_bq_task_group = SaveToBigQueryTaskGroup(
        group_id="save_to_bq", 
        files=transform_task_group.upload_tasks,
        gcs_bucket_name="my-gcs-bucket"
    )
    
    # TaskGroup para executar o Dataform
    dataform_workflow_group = DataformWorkflowTaskGroup(
        group_id="dataform_workflow",
        project_id="",
        region="us-central1",
        repository_id="your_dataform_repo_id"
    )

    # Encadeamento das tarefas
    save_to_bq_task_group >> dataform_workflow_group

csv_to_bq_dag()
