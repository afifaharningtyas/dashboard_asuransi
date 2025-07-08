from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def print_hello():
    return 'Hello from Airflow!'

with DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:
    start = DummyOperator(
        task_id='start',
    )

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> hello_task >> end