from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

# Definindo o DAG com o TaskFlow API
@dag(schedule_interval='@daily', start_date=datetime(2024, 10, 17), catchup=False)
def etl_taskflow():

    # Função de extração
    @task
    def extract():
        # Simulando extração de dados
        return {"data": "dados extraídos"}

    # Funções de transformação (dentro do Task Group)
    @task
    def transform_1(data):
        # Transformação 1
        return f"Transformação 1 com {data}"

    @task
    def transform_2(data):
        # Transformação 2
        return f"Transformação 2 com {data}"

    @task
    def transform_3(data):
        # Transformação 3
        return f"Transformação 3 com {data}"

    # Função para carregar os dados no BigQuery
    @task
    def load_to_bq(transformed_data_1, transformed_data_2, transformed_data_3):
        # Simulando carregamento no BigQuery com dados transformados
        print(f"Carregando no BigQuery: {transformed_data_1}, {transformed_data_2}, {transformed_data_3}")
    
    # Executando a extração de dados
    extracted_data = extract()

    # Task Group de transformações em paralelo
    with TaskGroup("transformations") as transformations:
        transform_task_1 = transform_1(extracted_data)
        transform_task_2 = transform_2(extracted_data)
        transform_task_3 = transform_3(extracted_data)

    # Carregar os dados no BigQuery após todas as transformações
    load_to_bq(transform_task_1, transform_task_2, transform_task_3)

# Instanciando o DAG
etl_taskflow_dag = etl_taskflow()
