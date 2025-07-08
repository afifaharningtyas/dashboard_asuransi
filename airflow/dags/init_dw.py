import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
import pandas as pd
import numpy as np

# Import konfigurasi database
from config.db_config import DW_DB_CONN_ID, DW_DB_PARAMS, SQL_QUERIES_DIR

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def generate_time_dimension(start_date, end_date):
    """
    Generate time dimension records for the specified date range
    """
    # Create a date range
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # Initialize list to store time dimension data
    time_data = []
    
    # For each date, generate a record
    for date_obj in date_range:
        time_record = {
            'date_actual': date_obj.strftime('%Y-%m-%d'),
            'day_of_week': date_obj.dayofweek,
            'day_name': date_obj.day_name(),
            'day_of_month': date_obj.day,
            'day_of_year': date_obj.dayofyear,
            'week_of_year': date_obj.isocalendar()[1],
            'month_actual': date_obj.month,
            'month_name': date_obj.month_name(),
            'quarter_actual': date_obj.quarter,
            'year_actual': date_obj.year,
            'is_weekend': date_obj.dayofweek >= 5,  # 5 = Saturday, 6 = Sunday
            'is_holiday': False,  # Default to False, can be updated later
            'holiday_name': None,
            'fiscal_year': date_obj.year  # Assuming fiscal year = calendar year
        }
        time_data.append(time_record)
    
    return time_data

def load_time_dimension():
    """
    Create and load time dimension into the data warehouse
    """
    # Date range for time dimension (past 5 years to future 5 years)
    start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-01-01')
    end_date = (datetime.now() + timedelta(days=365*5)).strftime('%Y-12-31')
    
    # Generate time dimension data
    time_data = generate_time_dimension(start_date, end_date)
    
    # Create a Pandas DataFrame
    time_df = pd.DataFrame(time_data)
    
    # Connect to the DW database and load data
    postgres_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    conn = postgres_hook.get_conn()
    
    # Create a cursor
    cursor = conn.cursor()
    
    # Insert records into dim_time
    for _, row in time_df.iterrows():
        cursor.execute(
            """
            INSERT INTO dim_time (
                date_actual, day_of_week, day_name, day_of_month, day_of_year, 
                week_of_year, month_actual, month_name, quarter_actual, year_actual,
                is_weekend, is_holiday, holiday_name, fiscal_year
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_actual) DO NOTHING
            """,
            (
                row['date_actual'], row['day_of_week'], row['day_name'], 
                row['day_of_month'], row['day_of_year'], row['week_of_year'], 
                row['month_actual'], row['month_name'], row['quarter_actual'], 
                row['year_actual'], row['is_weekend'], row['is_holiday'],
                row['holiday_name'], row['fiscal_year']
            )
        )
    
    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

def check_tables_exist():
    """
    Memeriksa apakah tabel-tabel dimensi dan fakta sudah ada.
    Return 'create_dim_tables' jika ada tabel yang belum ada, atau 'end' jika semua tabel sudah ada.
    """
    required_tables = [
        'dim_time', 'dim_customer', 'dim_product', 'dim_employee', 'dim_marketing_program', 'dim_policy',
        'fact_registration', 'fact_claim', 'fact_sales_activity', 'fact_investment', 'fact_customer_service'
    ]
    
    # Connect to the DW database
    try:
        postgres_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Check each table
        missing_tables = []
        for table in required_tables:
            cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}' AND table_schema = 'public')")
            exists = cursor.fetchone()[0]
            if not exists:
                missing_tables.append(table)
        
        cursor.close()
        conn.close()
        
        print(f"Missing tables: {missing_tables}")
        
        # Return task ID berdasarkan apakah ada tabel yang belum ada
        if missing_tables:
            return 'create_dim_tables'
        else:
            return 'end'
    except Exception as e:
        print(f"Error checking tables: {e}")
        # If there's an error connecting, likely we need to create tables
        return 'create_dim_tables'

# Define the DAG
with DAG(
    'init_data_warehouse',
    default_args=default_args,
    description='Initialize Data Warehouse tables and load time dimension',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dw', 'init'],
) as dag:
    
    # Task awal
    start = DummyOperator(task_id='start')
    
    # Task 1: Cek apakah tabel-tabel sudah ada - menentukan branch
    check_tables = BranchPythonOperator(
        task_id='check_tables_exist',
        python_callable=check_tables_exist
    )
    
    # Task 2: Buat tabel dimensi
    create_dim_tables = PostgresOperator(
        task_id='create_dim_tables',
        postgres_conn_id=DW_DB_CONN_ID,
        sql=[
            open(os.path.join(SQL_QUERIES_DIR, 'dim_time.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'dim_customer.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'dim_product.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'dim_employee.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'dim_marketing_program.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'dim_policy.sql')).read()
        ],
        autocommit=True
    )
    
    # Task 3: Buat tabel fakta
    create_fact_tables = PostgresOperator(
        task_id='create_fact_tables',
        postgres_conn_id=DW_DB_CONN_ID,
        sql=[
            open(os.path.join(SQL_QUERIES_DIR, 'fact_registration.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'fact_claim.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'fact_sales_activity.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'fact_investment.sql')).read(),
            open(os.path.join(SQL_QUERIES_DIR, 'fact_customer_service.sql')).read()
        ],
        autocommit=True
    )
    
    # Task 4: Load dimension waktu
    load_time_dim = PythonOperator(
        task_id='load_time_dimension',
        python_callable=load_time_dimension
    )
    
    # Task akhir
    end = DummyOperator(task_id='end', trigger_rule='none_failed')
    
    # Define task dependencies dengan alur yang disederhanakan
    
    # Alur utama: Start -> Check Tables -> Create Tables (jika diperlukan) -> Load Time Dimension -> End
    start >> check_tables
    
    # Jika tabel belum ada -> buat tabel dimensi & fakta -> load time dimension
    check_tables >> create_dim_tables >> create_fact_tables >> load_time_dim >> end
    
    # Jika semua tabel sudah ada -> langsung selesai
    check_tables >> end