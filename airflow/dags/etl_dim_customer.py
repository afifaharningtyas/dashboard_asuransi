import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
import uuid

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

def extract_customer_data():
    """
    Extract customer data from source database
    """
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    
    # Extract customer data
    sql = """
    SELECT 
        customer_id, 
        nama, 
        tanggal_lahir, 
        alamat, 
        referral_code, 
        tanggal_daftar,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, tanggal_lahir)) AS umur,
        (CURRENT_DATE - tanggal_daftar)::int AS lama_menjadi_pelanggan
    FROM customer
    """
    
    df = source_hook.get_pandas_df(sql)
    
    # Generate batch ID
    batch_id = f"CUST_LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Add DW metadata
    df['valid_from'] = datetime.now().date()
    df['valid_to'] = None
    df['is_current'] = True
    df['batch_id'] = batch_id
    
    return df

def load_dim_customer(df):
    """
    Load customer dimension using SCD Type 2 approach
    """
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    # Generate batch ID
    batch_id = df['batch_id'].iloc[0]
    
    # Iterate through each row and apply SCD Type 2 logic
    for _, row in df.iterrows():
        # Check if customer already exists
        cursor.execute(
            "SELECT customer_key FROM dim_customer WHERE customer_id = '%s' AND is_current = TRUE",
            (row['customer_id'],)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Compare to see if anything changed
            cursor.execute(
                """
                SELECT COUNT(*) FROM dim_customer 
                WHERE customer_id = '%s' AND is_current = TRUE AND
                (
                    nama != %s OR
                    tanggal_lahir != %s OR
                    alamat != %s OR
                    COALESCE(referral_code, '') != COALESCE(%s, '') OR
                    tanggal_daftar != %s
                )
                """,
                (row['customer_id'], row['nama'], row['tanggal_lahir'], 
                 row['alamat'], row['referral_code'], row['tanggal_daftar'])
            )
            has_changes = cursor.fetchone()[0] > 0
            
            if has_changes:
                # Update existing record (expire it)
                cursor.execute(
                    """
                    UPDATE dim_customer 
                    SET is_current = FALSE, valid_to = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE customer_id = '%s' AND is_current = TRUE
                    """,
                    (row['valid_from'], row['customer_id'])
                )
                
                # Insert new record
                cursor.execute(
                    """
                    INSERT INTO dim_customer (
                        customer_id, nama, tanggal_lahir, alamat, referral_code, tanggal_daftar,
                        umur, lama_menjadi_pelanggan, valid_from, valid_to, is_current, batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['customer_id'], row['nama'], row['tanggal_lahir'], 
                        row['alamat'], row['referral_code'], row['tanggal_daftar'],
                        row['umur'], row['lama_menjadi_pelanggan'], 
                        row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                    )
                )
        else:
            # Insert new customer
            cursor.execute(
                """
                INSERT INTO dim_customer (
                    customer_id, nama, tanggal_lahir, alamat, referral_code, tanggal_daftar,
                    umur, lama_menjadi_pelanggan, valid_from, valid_to, is_current, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['customer_id'], row['nama'], row['tanggal_lahir'], 
                    row['alamat'], row['referral_code'], row['tanggal_daftar'],
                    row['umur'], row['lama_menjadi_pelanggan'], 
                    row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                )
            )
    
    # Commit the transaction
    conn.commit()
    conn.close()
    
    return f"Loaded {len(df)} customer records with batch ID: {batch_id}"

def etl_dim_customer():
    """
    ETL process for customer dimension
    """
    # Extract
    df = extract_customer_data()
    
    # No complex transformation needed here
    
    # Load
    result = load_dim_customer(df)
    
    return result

# Define the DAG
with DAG(
    'etl_dim_customer',
    default_args=default_args,
    description='ETL process for customer dimension',
    schedule_interval='0 1 * * *',  # Run daily at 1:00 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dimension', 'customer'],
) as dag:
    
    # Task to run ETL process
    run_etl = PythonOperator(
        task_id='run_etl_dim_customer',
        python_callable=etl_dim_customer
    )