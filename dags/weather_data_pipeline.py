from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'weather-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='Pipeline tự động call weather API, transform và upload S3',
    schedule_interval='*/5 * * * *', 
    start_date=datetime(2025, 10, 12),
    catchup=False,
) as dag:

    call_api = BashOperator(
        task_id='call_weather_api',
        bash_command='python /opt/airflow/api_ingestion/call_weather_api.py'
    )

    transform_upload = BashOperator(
        task_id='transform_and_upload',
        bash_command='python /opt/airflow/data_transform/transform_and_up_s3.py'
    )

    call_api >> transform_upload
