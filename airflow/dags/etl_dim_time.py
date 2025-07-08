from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from config import db_config
import os

def etl_dim_time(**kwargs):
    # Tidak ada operasi interval, hanya pastikan format tanggal benar
    print('ETL dim_time dijalankan')
    # Contoh: jalankan SQL dari file, atau gunakan pandas/sqlalchemy

with DAG(
    'etl_dim_time',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='ETL harian untuk dim_time',
    schedule_interval='0 1 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dim', 'time'],
) as dag:
    etl_task = PythonOperator(
        task_id='etl_dim_time_task',
        python_callable=etl_dim_time,
        provide_context=True,
    )
