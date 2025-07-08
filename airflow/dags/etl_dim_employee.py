import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# Import konfigurasi database
from config import db_config
from config.db_config import SOURCE_DB_CONN_ID, DW_DB_CONN_ID, SQL_QUERIES_DIR

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_employee_data():
    """
    Extract employee data from source database
    """
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    sql = """
    SELECT 
        employee_id, 
        nama, 
        tanggal_lahir, 
        alamat, 
        jabatan, 
        department,
        tanggal_masuk,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, tanggal_lahir)) AS umur,
        (CURRENT_DATE - tanggal_masuk)::int AS masa_kerja
    FROM employee
    """
    df = source_hook.get_pandas_df(sql)
    batch_id = f"EMP_LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df['valid_from'] = datetime.now().date()
    df['valid_to'] = None
    df['is_current'] = True
    df['batch_id'] = batch_id
    return df

def load_dim_employee(df):
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    batch_id = df['batch_id'].iloc[0]
    for _, row in df.iterrows():
        cursor.execute(
            "SELECT employee_key FROM dim_employee WHERE employee_id = %s AND is_current = TRUE",
            (row['employee_id'],)
        )
        existing = cursor.fetchone()
        if existing:
            cursor.execute(
                """
                SELECT COUNT(*) FROM dim_employee 
                WHERE employee_id = %s AND is_current = TRUE AND
                (
                    nama != %s OR
                    tanggal_lahir != %s OR
                    alamat != %s OR
                    jabatan != %s OR
                    department != %s OR
                    tanggal_masuk != %s
                )
                """,
                (row['employee_id'], row['nama'], row['tanggal_lahir'], row['alamat'], row['jabatan'], 
                 row['department'], row['tanggal_masuk'])
            )
            has_changes = cursor.fetchone()[0] > 0
            if has_changes:
                cursor.execute(
                    """
                    UPDATE dim_employee 
                    SET is_current = FALSE, valid_to = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE employee_id = %s AND is_current = TRUE
                    """,
                    (row['valid_from'], row['employee_id'])
                )
                cursor.execute(
                    """
                    INSERT INTO dim_employee (
                        employee_id, nama, tanggal_lahir, alamat, jabatan, department, tanggal_masuk,
                        umur, masa_kerja, valid_from, valid_to, is_current, batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['employee_id'], row['nama'], row['tanggal_lahir'], row['alamat'], 
                        row['jabatan'], row['department'], row['tanggal_masuk'],
                        row['umur'], row['masa_kerja'], row['valid_from'], row['valid_to'], 
                        row['is_current'], row['batch_id']
                    )
                )
        else:
            cursor.execute(
                """
                INSERT INTO dim_employee (
                    employee_id, nama, tanggal_lahir, alamat, jabatan, department, tanggal_masuk,
                    umur, masa_kerja, valid_from, valid_to, is_current, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['employee_id'], row['nama'], row['tanggal_lahir'], row['alamat'], 
                    row['jabatan'], row['department'], row['tanggal_masuk'],
                    row['umur'], row['masa_kerja'], row['valid_from'], row['valid_to'], 
                    row['is_current'], row['batch_id']
                )
            )
    conn.commit()
    conn.close()
    return f"Loaded {len(df)} employee records with batch ID: {batch_id}"

def etl_dim_employee():
    df = extract_employee_data()
    result = load_dim_employee(df)
    return result

with DAG(
    'etl_dim_employee',
    default_args=default_args,
    description='ETL process for employee dimension',
    schedule_interval='0 1 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dimension', 'employee'],
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl_dim_employee',
        python_callable=etl_dim_employee
    )
