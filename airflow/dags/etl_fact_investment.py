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

def extract_investment():
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    sql = """
    SELECT 
        investment_id, 
        policy_id, 
        investment_type, 
        risk_level, 
        investment_date, 
        amount, 
        fund_manager, 
        initial_nav, 
        units_purchased
    FROM investment
    """
    df = source_hook.get_pandas_df(sql)
    print(f"[extract] Retrieved {len(df)} rows from source `investment`")
    if not df.empty:
        print(df.head(5).to_dict(orient='records'))
    else:
        print("[extract] WARNING: source `investment` is empty!")
    return df

def transform_investment(df):
    print(f"[transform] Starting transform on {len(df)} rows")
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)

    # 1) Load dim_policy keys
    policy_keys = dw_hook.get_pandas_df("""
        SELECT policy_id, policy_key, customer_key 
        FROM dim_policy 
        WHERE is_current = TRUE
    """)
    print('df police ----',policy_keys.head(2))
    print('df  ----',df.head(2))
    print(f"[transform] Loaded {len(policy_keys)} rows from dim_policy")
    
    # 2) Merge policy
    print('df----',df.dtypes)
    print('policy----',policy_keys.dtypes)
    df['policy_id'] = df['policy_id'].astype(str)
    policy_keys['policy_id'] = policy_keys['policy_id'].astype(str)
    
    print('df----',df.dtypes)
    print('policy----',policy_keys.dtypes)

    merged1 = pd.merge(df, policy_keys, on='policy_id', how='left')
    missing_policy = merged1['policy_key'].isna().sum()
    print('join table--- ',merged1.head(2))
    print(f"[transform] After merge policy: {len(merged1)} rows, {missing_policy} missing policy_key")
    
    # 3) Load dim_time keys
    time_keys = dw_hook.get_pandas_df("SELECT date_actual, time_key FROM dim_time")
    time_keys['date_actual'] = pd.to_datetime(time_keys['date_actual']).dt.date
    print(f"[transform] Loaded {len(time_keys)} rows from dim_time")
    
    # 4) Prepare and merge time dimension
    merged1['investment_date'] = pd.to_datetime(merged1['investment_date']).dt.date
    merged2 = pd.merge(
        merged1, 
        time_keys, 
        left_on='investment_date', 
        right_on='date_actual', 
        how='left'
    )
    missing_time = merged2['time_key'].isna().sum()
    print(f"[transform] After merge time: {len(merged2)} rows, {missing_time} missing time_key")
    
    # 5) Rename and calculate derived fields
    merged2 = merged2.rename(columns={'time_key': 'time_key_investment'})
    today = datetime.now().date()
    merged2['days_since_investment'] = merged2['investment_date'].apply(lambda d: (today - d).days)
    merged2['investment_status'] = merged2['days_since_investment'] \
        .apply(lambda x: 'Jatuh Tempo' if x >= 365 else 'Aktif')
    merged2['batch_id'] = f"INVEST_FACT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # 6) Drop rows with NULL dimension keys
    before_drop = len(merged2)
    final_df = merged2.dropna(subset=['policy_key', 'customer_key', 'time_key_investment'])
    after_drop = len(final_df)
    print(f"[transform] After dropna: kept {after_drop} of {before_drop} rows")
    
    # 7) Select final columns
    result_df = final_df[[
        'investment_id',
        'policy_key',
        'customer_key',
        'time_key_investment',
        'investment_type',
        'risk_level',
        'amount',
        'fund_manager',
        'initial_nav',
        'units_purchased',
        'days_since_investment',
        'investment_status',
        'batch_id'
    ]]
    print(f"[transform] Final DataFrame ready with {len(result_df)} rows")
    return result_df

def load_fact_investment(df):
    print(f"[load] Loading {len(df)} rows into fact_investment")
    if df.empty:
        print("[load] No data to load. Exiting.")
        return "No data to load"
    
    # Truncate target table
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    print("[load] Truncating fact_investment")
    target_hook.run("TRUNCATE TABLE fact_investment")
    
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    for idx, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO fact_investment (
                investment_id,
                policy_key,
                customer_key,
                time_key_investment,
                investment_type,
                risk_level,
                amount,
                fund_manager,
                initial_nav,
                units_purchased,
                days_since_investment,
                investment_status,
                batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['investment_id'],
                int(row['policy_key']),
                int(row['customer_key']),
                int(row['time_key_investment']),
                row['investment_type'],
                row['risk_level'],
                float(row['amount']),
                row['fund_manager'],
                float(row['initial_nav']),
                float(row['units_purchased']),
                int(row['days_since_investment']),
                row['investment_status'],
                row['batch_id']
            )
        )
    conn.commit()
    conn.close()
    print(f"[load] Successfully loaded {len(df)} rows with batch ID {df['batch_id'].iloc[0]}")
    return f"Loaded {len(df)} investment fact records with batch ID: {df['batch_id'].iloc[0]}"

def etl_fact_investment():
    df = extract_investment()
    transformed_df = transform_investment(df)
    result = load_fact_investment(transformed_df)
    return result

with DAG(
    'etl_fact_investment',
    default_args=default_args,
    description='ETL process for investment fact table',
    schedule_interval='0 2 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'fact', 'investment'],
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl_fact_investment',
        python_callable=etl_fact_investment
    )
