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

def extract_customer_service():
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    sql = """
    SELECT 
        service_id,
        customer_id,
        employee_id,
        policy_id,
        interaction_date,
        interaction_type,
        channel,
        complaint_category,
        duration_minutes,
        status,
        resolution_time,     
        satisfaction_rating,
        follow_up_required
    FROM customer_service
    """
    df = source_hook.get_pandas_df(sql)

    # Rename to match downstream expectations
    if 'resolution_time' in df.columns:
        df = df.rename(columns={'resolution_time': 'resolution_date'})

    return df

def transform_customer_service(df):
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Load dimension keys
    cust_keys = dw_hook.get_pandas_df(
        "SELECT customer_id, customer_key FROM dim_customer WHERE is_current = TRUE"
    )
    emp_keys = dw_hook.get_pandas_df(
        "SELECT employee_id, employee_key FROM dim_employee WHERE is_current = TRUE"
    )
    policy_keys = dw_hook.get_pandas_df(
        "SELECT policy_id, policy_key FROM dim_policy WHERE is_current = TRUE"
    )
    time_keys = dw_hook.get_pandas_df("SELECT date_actual, time_key FROM dim_time")
    
    # Prepare dates
    time_keys['date_actual'] = pd.to_datetime(time_keys['date_actual']).dt.date
    df['interaction_date'] = pd.to_datetime(df['interaction_date']).dt.date
    if 'resolution_date' in df.columns:
        df['resolution_date'] = pd.to_datetime(df['resolution_date']).dt.date
    
    # Merge with customer, employee, policy
    df = pd.merge(df, cust_keys, on='customer_id', how='left')
    df = pd.merge(df, emp_keys, on='employee_id', how='left')
    df = pd.merge(df, policy_keys, on='policy_id', how='left')
    
    # Merge interaction date → time_key_interaction
    df = pd.merge(
        df,
        time_keys,
        left_on='interaction_date',
        right_on='date_actual',
        how='left'
    ).rename(columns={'time_key': 'time_key_interaction'})
    
    # Merge resolution date → time_key_resolution
    if 'resolution_date' in df.columns and not df['resolution_date'].isna().all():
        df = pd.merge(
            df,
            time_keys,
            left_on='resolution_date',
            right_on='date_actual',
            how='left',
            suffixes=('', '_res')
        ).rename(columns={'time_key': 'time_key_resolution'})
    else:
        df['time_key_resolution'] = None
    
    # Calculate resolution_time_hours
    df['resolution_time_hours'] = None
    mask = (~pd.isna(df['interaction_date'])) & (~pd.isna(df['resolution_date']))
    if mask.any():
        df.loc[mask, 'resolution_time_hours'] = (
            (pd.to_datetime(df.loc[mask, 'resolution_date']) -
             pd.to_datetime(df.loc[mask, 'interaction_date']))
            .dt.total_seconds() / 3600
        )
    
    # Batch ID
    batch_id = f"CS_FACT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df['batch_id'] = batch_id
    
    # Final selection
    result_df = df[[
        'service_id',
        'customer_key',
        'employee_key',
        'policy_key',
        'time_key_interaction',
        'time_key_resolution',
        'interaction_type',
        'channel',
        'complaint_category',
        'duration_minutes',
        'status',
        'satisfaction_rating',
        'follow_up_required',
        'resolution_time_hours',
        'batch_id'
    ]]
    
    return result_df

def load_fact_customer_service(df):
    if df.empty:
        return "No data to load"
    
    df = df.dropna(subset=['customer_key', 'employee_key', 'time_key_interaction'])
    if df.empty:
        return "No valid data to load after removing NULLs"
    
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    target_hook.run("TRUNCATE TABLE fact_customer_service")
    
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO fact_customer_service (
            service_id,
            customer_key,
            employee_key,
            policy_key,
            time_key_interaction,
            time_key_resolution,
            interaction_type,
            channel,
            complaint_category,
            duration_minutes,
            status,
            satisfaction_rating,
            follow_up_required,
            resolution_time_hours,
            batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row['service_id'],
            int(row['customer_key']),
            int(row['employee_key']),
            int(row['policy_key']) if not pd.isna(row['policy_key']) else None,
            int(row['time_key_interaction']),
            int(row['time_key_resolution']) if not pd.isna(row['time_key_resolution']) else None,
            row['interaction_type'],
            row['channel'],
            row['complaint_category'] if row.get('complaint_category') else None,
            int(row['duration_minutes']) if not pd.isna(row['duration_minutes']) else None,
            row['status'],
            int(row['satisfaction_rating']) if not pd.isna(row['satisfaction_rating']) else None,
            bool(row['follow_up_required']) if not pd.isna(row['follow_up_required']) else False,
            float(row['resolution_time_hours']) if not pd.isna(row['resolution_time_hours']) else None,
            row['batch_id']
        ))
    conn.commit()
    conn.close()
    
    return f"Loaded {len(df)} customer service fact records with batch ID: {df['batch_id'].iloc[0]}"

def etl_fact_customer_service():
    df = extract_customer_service()
    transformed_df = transform_customer_service(df)
    return load_fact_customer_service(transformed_df)

with DAG(
    'etl_fact_customer_service',
    default_args=default_args,
    description='ETL process for customer service fact table',
    schedule_interval='0 2 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'fact', 'customer_service'],
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl_fact_customer_service',
        python_callable=etl_fact_customer_service
    )
