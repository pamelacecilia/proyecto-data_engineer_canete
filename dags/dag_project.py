from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
from main import iniciar

dag_path = os.getcwd()
default_args = {
    'start_date': datetime(2024, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

first_dag = DAG(
    dag_id='ingestion_data',
    default_args=default_args,
    description='Add last 1000 blocks from BCS',
    start_date=datetime(2024,6,1),
    schedule_interval='@daily',
    catchup=False
)


task_1 = BashOperator(
    task_id='log_start',
    bash_command='echo Starting...'
)

task_2 = PythonOperator(
    task_id='main_task',
    python_callable=iniciar,
    dag=first_dag,
)

task_3 = BashOperator(
    task_id='log_end',
    bash_command='echo Process Complete...'
)

task_1 >> task_2 >> task_3