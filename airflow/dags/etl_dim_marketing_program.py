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

def extract_marketing_program_data():
    """
    Extract marketing program data from source database
    """
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    
    # Extract marketing program data
    sql = """
    SELECT 
        program_id, 
        nama_program, 
        start_date, 
        end_date, 
        target, 
        description,
        jenis_program,
        budget
    FROM marketing_program
    """
    
    df = source_hook.get_pandas_df(sql)
    
    return df

def transform_marketing_program_data(df):
    """
    Transform marketing program data by adding calculated fields
    """
    # Add program_status based on dates
    current_date = datetime.now().date()
    
    # Convert dates to proper format if they're not already
    df['start_date'] = pd.to_datetime(df['start_date']).dt.date
    df['end_date'] = pd.to_datetime(df['end_date']).dt.date
    
    # Calculate program status
    conditions = [
        (df['start_date'] > current_date),
        (df['start_date'] <= current_date) & (df['end_date'] >= current_date),
        (df['end_date'] < current_date)
    ]
    choices = ['Planned', 'Active', 'Completed']
    df['program_status'] = pd.np.select(conditions, choices, default='Unknown')
    
    # Calculate duration in days
    df['durasi_program'] = (pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])).dt.days
    
    # Generate batch ID
    batch_id = f"MKTG_LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Add SCD Type 2 metadata
    df['valid_from'] = datetime.now().date()
    df['valid_to'] = None
    df['is_current'] = True
    df['batch_id'] = batch_id
    
    return df

def load_dim_marketing_program(df):
    """
    Load marketing program dimension using SCD Type 2 approach
    """
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    # Generate batch ID
    batch_id = df['batch_id'].iloc[0]
    
    # Iterate through each row and apply SCD Type 2 logic
    for _, row in df.iterrows():
        # Check if marketing program already exists
        cursor.execute(
            "SELECT program_key FROM dim_marketing_program WHERE program_id = %s AND is_current = TRUE",
            (row['program_id'],)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Compare to see if anything changed
            cursor.execute(
                """
                SELECT COUNT(*) FROM dim_marketing_program 
                WHERE program_id = %s AND is_current = TRUE AND
                (
                    nama_program != %s OR
                    start_date != %s OR
                    end_date != %s OR
                    target != %s OR
                    description != %s OR
                    jenis_program != %s OR
                    budget != %s
                )
                """,
                (row['program_id'], row['nama_program'], row['start_date'], 
                 row['end_date'], row['target'], row['description'], 
                 row['jenis_program'], row['budget'])
            )
            has_changes = cursor.fetchone()[0] > 0
            
            if has_changes:
                # Update existing record (expire it)
                cursor.execute(
                    """
                    UPDATE dim_marketing_program 
                    SET is_current = FALSE, valid_to = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE program_id = %s AND is_current = TRUE
                    """,
                    (row['valid_from'], row['program_id'])
                )
                
                # Insert new record
                cursor.execute(
                    """
                    INSERT INTO dim_marketing_program (
                        program_id, nama_program, start_date, end_date, target, description,
                        jenis_program, budget, program_status, durasi_program, 
                        valid_from, valid_to, is_current, batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['program_id'], row['nama_program'], row['start_date'], 
                        row['end_date'], row['target'], row['description'],
                        row['jenis_program'], row['budget'],
                        row['program_status'], row['durasi_program'],
                        row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                    )
                )
        else:
            # Insert new marketing program
            cursor.execute(
                """
                INSERT INTO dim_marketing_program (
                    program_id, nama_program, start_date, end_date, target, description,
                    jenis_program, budget, program_status, durasi_program, 
                    valid_from, valid_to, is_current, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['program_id'], row['nama_program'], row['start_date'], 
                    row['end_date'], row['target'], row['description'],
                    row['jenis_program'], row['budget'],
                    row['program_status'], row['durasi_program'],
                    row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                )
            )
    
    # Commit the transaction
    conn.commit()
    conn.close()
    
    return f"Loaded {len(df)} marketing program records with batch ID: {batch_id}"

def etl_dim_marketing_program():
    """
    ETL process for marketing program dimension
    """
    # Extract
    df = extract_marketing_program_data()
    
    # Transform
    transformed_df = transform_marketing_program_data(df)
    
    # Load
    result = load_dim_marketing_program(transformed_df)
    
    return result

# Define the DAG
with DAG(
    'etl_dim_marketing_program',
    default_args=default_args,
    description='ETL process for marketing program dimension',
    schedule_interval='0 1 * * *',  # Run daily at 1:00 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dimension', 'marketing_program'],
) as dag:
    
    # Task to run ETL process
    run_etl = PythonOperator(
        task_id='run_etl_dim_marketing_program',
        python_callable=etl_dim_marketing_program
    )