import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
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

def extract_policy_data():
    """
    Extract policy data from source database for registration facts
    """
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    
    # Extract policy data
    sql = """
    SELECT 
        p.policy_id,
        p.customer_id,
        p.product_id,
        p.tanggal_mulai,
        p.tanggal_berakhir,
        p.premium,
        p.sum_assured,
        EXTRACT(YEAR FROM p.tanggal_berakhir) - EXTRACT(YEAR FROM p.tanggal_mulai) AS policy_term,
        c.referral_code IS NOT NULL AS referral_flag
    FROM policy p
    JOIN customer c ON p.customer_id = c.customer_id
    """
    
    df = source_hook.get_pandas_df(sql)
    
    return df

def transform_registration_facts(df):
    """
    Transform policy data into registration facts with dimension keys
    """
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get customer dimension keys
    customer_keys = dw_hook.get_pandas_df(
        "SELECT customer_id, customer_key FROM dim_customer WHERE is_current = TRUE"
    )
    
    # Get product dimension keys
    product_keys = dw_hook.get_pandas_df(
        "SELECT product_id, product_key FROM dim_product WHERE is_current = TRUE"
    )
    
    # Get policy dimension keys
    policy_keys = dw_hook.get_pandas_df(
        "SELECT policy_id, policy_key FROM dim_policy WHERE is_current = TRUE"
    )
    
    # Get time dimension keys
    time_keys = dw_hook.get_pandas_df(
        "SELECT date_actual, time_key FROM dim_time"
    )
    
    # Convert time_keys date_actual to datetime for merging
    time_keys['date_actual'] = pd.to_datetime(time_keys['date_actual']).dt.date
    
    # Convert tanggal_mulai to datetime.date for merging
    df['tanggal_mulai'] = pd.to_datetime(df['tanggal_mulai']).dt.date
    
    df['customer_id'] = df['customer_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)
    df['policy_id'] = df['policy_id'].astype(str)
    customer_keys['customer_id'] = customer_keys['customer_id'].astype(str)
    product_keys['product_id'] = product_keys['product_id'].astype(str)
    policy_keys['policy_id'] = policy_keys['policy_id'].astype(str)
    # Merge with dimension keys
    df = pd.merge(df, customer_keys, on='customer_id', how='left')
    df = pd.merge(df, product_keys, on='product_id', how='left')
    df = pd.merge(df, policy_keys, on='policy_id', how='left')
    df = pd.merge(df, time_keys, left_on='tanggal_mulai', right_on='date_actual', how='left')
    
    # Select and rename columns
    result_df = df[[
        'customer_key', 'product_key', 'time_key', 'policy_key',
        'premium', 'sum_assured', 'policy_term', 'referral_flag'
    ]].rename(columns={'premium': 'premium_amount'})
    
    # Generate batch ID
    batch_id = f"REG_FACT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    result_df['batch_id'] = batch_id
    
    return result_df

def load_fact_registration(df):
    """
    Load registration fact table
    """
    if df.empty:
        return "No data to load"
    
    # Remove rows with NULL dimension keys (could not be matched)
    df = df.dropna(subset=['customer_key', 'product_key', 'time_key', 'policy_key'])
    
    if df.empty:
        return "No valid data to load after removing NULLs"
    
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Clear existing data (for simplicity, in production you might use incremental load)
    target_hook.run("TRUNCATE TABLE fact_registration")
    
    # Load data
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO fact_registration (
                customer_key, product_key, time_key, policy_key,
                premium_amount, sum_assured, policy_term, referral_flag, batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(row['customer_key']), int(row['product_key']), 
                int(row['time_key']), int(row['policy_key']),
                float(row['premium_amount']), float(row['sum_assured']), 
                int(row['policy_term']), bool(row['referral_flag']), row['batch_id']
            )
        )
    
    # Commit the transaction
    conn.commit()
    conn.close()
    
    return f"Loaded {len(df)} registration fact records with batch ID: {df['batch_id'].iloc[0]}"

def etl_fact_registration():
    """
    ETL process for registration fact table
    """
    # Extract
    df = extract_policy_data()
    
    # Transform
    transformed_df = transform_registration_facts(df)
    
    # Load
    result = load_fact_registration(transformed_df)
    
    return result

# Define the DAG
with DAG(
    'etl_fact_registration',
    default_args=default_args,
    description='ETL process for registration fact table',
    schedule_interval='0 2 * * *',  # Run daily at 2:00 AM (after dimensions are loaded)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'fact', 'registration'],
) as dag:
    
    # Task to run ETL process
    run_etl = PythonOperator(
        task_id='run_etl_fact_registration',
        python_callable=etl_fact_registration
    )