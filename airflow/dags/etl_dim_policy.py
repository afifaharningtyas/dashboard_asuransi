import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np

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

def extract_policy_data():
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    sql = """
    SELECT 
        policy_id, 
        customer_id, 
        product_id, 
        tanggal_mulai, 
        tanggal_berakhir,
        (tanggal_berakhir - tanggal_mulai)::int AS durasi_polis
    FROM policy
    """
    df = source_hook.get_pandas_df(sql)
    return df

def transform_policy_data(df):
    # Status polis: Active, Expired, Future
    now = datetime.now().date()
    df['tanggal_mulai'] = pd.to_datetime(df['tanggal_mulai']).dt.date
    df['tanggal_berakhir'] = pd.to_datetime(df['tanggal_berakhir']).dt.date
    
    conditions = [
        (df['tanggal_mulai'] > now),
        (df['tanggal_mulai'] <= now) & (df['tanggal_berakhir'] >= now),
        (df['tanggal_berakhir'] < now)
    ]
    choices = ['Future', 'Active', 'Expired']
    df['status_polis'] = np.select(conditions, choices, default='Unknown')
    
    batch_id = f"POLICY_LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df['valid_from'] = now
    df['valid_to'] = None
    df['is_current'] = True
    df['batch_id'] = batch_id
    return df

def load_dim_policy(df):
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    batch_id = df['batch_id'].iloc[0]
    for _, row in df.iterrows():
        cursor.execute(
            "SELECT policy_key FROM dim_policy WHERE policy_id = %s AND is_current = TRUE",
            (row['policy_id'],)
        )
        existing = cursor.fetchone()
        if existing:
            cursor.execute(
                """
                SELECT COUNT(*) FROM dim_policy 
                WHERE policy_id = %s AND is_current = TRUE AND
                (
                    customer_id != '%s' OR
                    product_id != '%s' OR
                    tanggal_mulai != %s OR
                    tanggal_berakhir != %s
                )
                """,
                (row['policy_id'], row['customer_id'], row['product_id'], row['tanggal_mulai'], row['tanggal_berakhir'])
            )
            has_changes = cursor.fetchone()[0] > 0
            if has_changes:
                cursor.execute(
                    """
                    UPDATE dim_policy 
                    SET is_current = FALSE, valid_to = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE policy_id = %s AND is_current = TRUE
                    """,
                    (row['valid_from'], row['policy_id'])
                )
                cursor.execute(
                    """
                    INSERT INTO dim_policy (
                        policy_id, customer_id, product_id, tanggal_mulai, tanggal_berakhir,
                        durasi_polis, status_polis, valid_from, valid_to, is_current, batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['policy_id'], row['customer_id'], row['product_id'], row['tanggal_mulai'], row['tanggal_berakhir'],
                        row['durasi_polis'], row['status_polis'], row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                    )
                )
        else:
            cursor.execute(
                """
                INSERT INTO dim_policy (
                    policy_id, customer_id, product_id, tanggal_mulai, tanggal_berakhir,
                    durasi_polis, status_polis, valid_from, valid_to, is_current, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['policy_id'], row['customer_id'], row['product_id'], row['tanggal_mulai'], row['tanggal_berakhir'],
                    row['durasi_polis'], row['status_polis'], row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                )
            )
    conn.commit()
    conn.close()
    return f"Loaded {len(df)} policy records with batch ID: {batch_id}"


def etl_dim_policy():
    try:
        print("Starting ETL process...")
        df = extract_policy_data()
        print(f"Extracted {len(df)} records")
        
        transformed_df = transform_policy_data(df)
        print(f"Transformed {len(transformed_df)} records")
        
        result = load_dim_policy(transformed_df)
        print(f"ETL completed: {result}")
        return result
    except Exception as e:
        print(f"ETL failed: {str(e)}")
        raise

with DAG(
    'etl_dim_policy',
    default_args=default_args,
    description='ETL process for policy dimension',
    schedule_interval='0 1 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dimension', 'policy'],
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl_dim_policy',
        python_callable=etl_dim_policy
    )
