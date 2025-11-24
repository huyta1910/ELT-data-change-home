
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='incremental_hoadon_chitiethanghoa_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily', # Run once a day
    catchup=False,
    tags=['bigquery', 'incremental'],
) as dag:
    
    run_incremental_ingestion = BashOperator(
        task_id='run_incremental_ingestion',
        # This command runs the Python script from within the Airflow container
        # The script is available because we mounted the `scripts` folder in docker-compose.yml
        bash_command='python /opt/airflow/scripts/ingest_incremental.py'
    )

    