import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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

def extract_claim_data():
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    sql = """
    SELECT 
        cl.claim_id,
        cl.policy_id,
        cl.claim_date,
        cl.claim_type,
        cl.claim_description,
        cl.claim_amount,
        cl.status,
        cl.decision_date,
        cl.approved_amount,
        cl.rejection_reason,
        (cl.decision_date - cl.claim_date)::int AS processing_days
    FROM claim cl
    """
    df = source_hook.get_pandas_df(sql)
    return df

def transform_claim_facts(df):
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Join to dim_policy
    policy_keys = dw_hook.get_pandas_df(
        "SELECT policy_id, policy_key, customer_key FROM dim_policy WHERE is_current = TRUE"
    )
    
    # Join to dim_time (claim_date)
    time_keys = dw_hook.get_pandas_df(
        "SELECT date_actual, time_key FROM dim_time"
    )
    
    time_keys['date_actual'] = pd.to_datetime(time_keys['date_actual']).dt.date
    df['claim_date'] = pd.to_datetime(df['claim_date']).dt.date
    
    # Convert decision_date to datetime.date for merging if it exists
    if 'decision_date' in df.columns:
        df['decision_date'] = pd.to_datetime(df['decision_date']).dt.date
    
    df['policy_id'] = df['policy_id'].astype(str)
    print('df ---',df.head(2))
    policy_keys['policy_id'] = policy_keys['policy_id'].astype(str)
    print('policy_keys ---',policy_keys.head(2))
    # Join with dimension tables
    df = pd.merge(df, policy_keys, on='policy_id', how='left')
    df = pd.merge(df, time_keys, left_on='claim_date', right_on='date_actual', how='left')
    df = df.rename(columns={'time_key': 'time_key_submitted'})
    
    # Handle decision date if it exists
    if 'decision_date' in df.columns and not df['decision_date'].isna().all():
        df = pd.merge(
            df, 
            time_keys,
            left_on='decision_date',
            right_on='date_actual',
            how='left',
            suffixes=('', '_decision')
        )
        df = df.rename(columns={'time_key': 'time_key_decision'})
    else:
        df['time_key_decision'] = None
    
    # Calculate derived fields
    df['rejection_flag'] = df['status'] == 'Rejected'
    df['paid_flag'] = df['status'] == 'Paid'
    
    # Generate batch ID
    batch_id = f"CLAIM_FACT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df['batch_id'] = batch_id
    
    # Select final columns
    result_df = df[[
        'claim_id', 'policy_key', 'customer_key', 'time_key_submitted', 
        'time_key_decision', 'claim_type', 'claim_amount', 
        'approved_amount', 'status', 'processing_days', 
        'rejection_flag', 'paid_flag', 'batch_id'
    ]]
    print('result_df ---', result_df.head(2))
    return result_df

def load_fact_claim(df):
    if df.empty:
        return "No data to load"
    
    # Remove rows with NULL dimension keys
    df = df.dropna(subset=['policy_key', 'customer_key', 'time_key_submitted'])
    
    if df.empty:
        return "No valid data to load after removing NULLs"
    
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Clear existing data
    target_hook.run("TRUNCATE TABLE fact_claim")
    
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO fact_claim (
                claim_id, policy_key, customer_key, time_key_submitted, 
                time_key_decision, claim_type, claim_amount, 
                approved_amount, status, processing_days,
                rejection_flag, paid_flag, batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['claim_id'], 
                int(row['policy_key']), 
                int(row['customer_key']),
                int(row['time_key_submitted']),
                int(row['time_key_decision']) if not pd.isna(row['time_key_decision']) else None,
                row['claim_type'],
                float(row['claim_amount']),
                float(row['approved_amount']) if not pd.isna(row['approved_amount']) else None,
                row['status'],
                int(row['processing_days']) if not pd.isna(row['processing_days']) else None,
                bool(row['rejection_flag']),
                bool(row['paid_flag']),
                row['batch_id']
            )
        )
    
    conn.commit()
    conn.close()
    
    return f"Loaded {len(df)} claim fact records with batch ID: {df['batch_id'].iloc[0]}"

def etl_fact_claim():
    df = extract_claim_data()
    transformed_df = transform_claim_facts(df)
    result = load_fact_claim(transformed_df)
    return result

with DAG(
    'etl_fact_claim',
    default_args=default_args,
    description='ETL process for claim fact table',
    schedule_interval='0 2 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'fact', 'claim'],
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl_fact_claim',
        python_callable=etl_fact_claim
    )
