import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from config.db_config import SOURCE_DB_CONN_ID, DW_DB_CONN_ID

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_sales_activity():
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    sql = """
    SELECT 
        activity_id, employee_id, customer_id, product_id, program_id, 
        activity_date, activity_type, activity_channel,
        duration_minutes, status, resulted_in_sale, follow_up_date
    FROM sales_activity
    """
    df = source_hook.get_pandas_df(sql)
    return df

def transform_sales_activity(df):
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get dimension keys
    emp_keys = dw_hook.get_pandas_df("SELECT employee_id, employee_key FROM dim_employee WHERE is_current = TRUE")
    cust_keys = dw_hook.get_pandas_df("SELECT customer_id, customer_key FROM dim_customer WHERE is_current = TRUE")
    prod_keys = dw_hook.get_pandas_df("SELECT product_id, product_key FROM dim_product WHERE is_current = TRUE")
    prog_keys = dw_hook.get_pandas_df("SELECT program_id, program_key FROM dim_marketing_program WHERE is_current = TRUE")
    time_keys = dw_hook.get_pandas_df("SELECT date_actual, time_key FROM dim_time")
    
    # Prepare date for merging
    time_keys['date_actual'] = pd.to_datetime(time_keys['date_actual']).dt.date
    df['activity_date'] = pd.to_datetime(df['activity_date']).dt.date
    
    df['customer_id'] = df['customer_id'].astype(str)
    df['employee_id'] = df['employee_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)
    df['program_id'] = df['program_id'].astype(str)
    emp_keys['employee_id'] = emp_keys['employee_id'].astype(str)
    cust_keys['customer_id'] = cust_keys['customer_id'].astype(str)
    prod_keys['product_id'] = prod_keys['product_id'].astype(str)
    prog_keys['program_id'] = prog_keys['program_id'].astype(str)
    # Merge with dimension tables
    df = pd.merge(df, emp_keys, on='employee_id', how='left')
    df = pd.merge(df, cust_keys, on='customer_id', how='left')
    df = pd.merge(df, prod_keys, on='product_id', how='left')
    df = pd.merge(df, prog_keys, on='program_id', how='left')
    df = pd.merge(df, time_keys, left_on='activity_date', right_on='date_actual', how='left')
    
    # Calculate derived fields
    df['follow_up_scheduled'] = df['follow_up_date'].notna()
    df['snapshot_date'] = datetime.now().date()
    
    # Generate batch ID
    batch_id = f"SALES_FACT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df['batch_id'] = batch_id
    
    # Select final columns
    result_df = df[[
        'activity_id', 'employee_key', 'customer_key', 'product_key', 'program_key', 
        'time_key', 'activity_type', 'activity_channel', 'duration_minutes', 
        'status', 'resulted_in_sale', 'follow_up_scheduled', 'batch_id', 'snapshot_date'
    ]]
    
    return result_df

def load_fact_sales_activity(df):
    if df.empty:
        return "No data to load"
    
    # Remove rows with NULL dimension keys
    df = df.dropna(subset=['employee_key', 'customer_key', 'product_key', 'time_key'])
    
    if df.empty:
        return "No valid data to load after removing NULLs"
    
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    target_hook.run("TRUNCATE TABLE fact_sales_activity")
    
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO fact_sales_activity (
                activity_id, employee_key, customer_key, product_key, program_key,
                time_key, activity_type, activity_channel, duration_minutes,
                status, resulted_in_sale, follow_up_scheduled, batch_id, snapshot_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['activity_id'], int(row['employee_key']), int(row['customer_key']), int(row['product_key']),
                int(row['program_key']) if not pd.isna(row['program_key']) else None,
                int(row['time_key']), row['activity_type'], row['activity_channel'], 
                int(row['duration_minutes']),
                row['status'], bool(row['resulted_in_sale']), bool(row['follow_up_scheduled']),
                row['batch_id'], row['snapshot_date']
            )
        )
    
    conn.commit()
    conn.close()
    
    return f"Loaded {len(df)} sales activity fact records with batch ID: {df['batch_id'].iloc[0]}"

def etl_fact_sales_activity():
    df = extract_sales_activity()
    transformed_df = transform_sales_activity(df)
    result = load_fact_sales_activity(transformed_df)
    return result

with DAG(
    'etl_fact_sales_activity',
    default_args=default_args,
    description='ETL process for sales activity fact table',
    schedule_interval='0 2 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'fact', 'sales_activity'],
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl_fact_sales_activity',
        python_callable=etl_fact_sales_activity
    )
