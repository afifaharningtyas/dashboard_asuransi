import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# Import database configuration
from config.db_config import DW_DB_CONN_ID

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create directory for dashboard exports if it doesn't exist
DASHBOARD_EXPORT_DIR = '/opt/airflow/data/dashboard'
os.makedirs(DASHBOARD_EXPORT_DIR, exist_ok=True)

# Define functions to export data for different dashboards
def export_registration_dashboard_data(**kwargs):
    """
    Export data needed for the Registration Dashboard
    """
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get parameters from context
    params = kwargs.get('params', {})
    months_lookback = params.get('months_lookback', 12)
    
    # Query to get registration metrics by product and time
    sql = f"""
    SELECT 
        dt.year_actual, 
        dt.month_name,
        dp.nama_product,
        dp.jenis_product,
        dp.kategori_premi,
        COUNT(fr.registration_key) as total_registrations,
        SUM(fr.premium_amount) as total_premium,
        SUM(fr.sum_assured) as total_sum_assured,
        AVG(fr.premium_amount) as avg_premium,
        SUM(CASE WHEN fr.referral_flag THEN 1 ELSE 0 END) as referral_count
    FROM fact_registration fr
    JOIN dim_product dp ON fr.product_key = dp.product_key
    JOIN dim_time dt ON fr.time_key = dt.time_key
    WHERE dt.date_actual >= CURRENT_DATE - INTERVAL '{months_lookback} months'
    GROUP BY dt.year_actual, dt.month_name, dp.nama_product, dp.jenis_product, dp.kategori_premi
    ORDER BY dt.year_actual, dt.month_name
    """
    
    df = dw_hook.get_pandas_df(sql)
    
    # Save to CSV
    output_path = os.path.join(DASHBOARD_EXPORT_DIR, f'registration_metrics_{datetime.now().strftime("%Y%m%d")}.csv')
    df.to_csv(output_path, index=False)
    
    return f"Registration dashboard data exported to {output_path} with {len(df)} records"

def export_customer_dashboard_data(**kwargs):
    """
    Export data needed for the Customer Dashboard
    """
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get parameters from context
    params = kwargs.get('params', {})
    months_lookback = params.get('months_lookback', 12)
    
    # Query to get customer demographics and metrics
    sql = """
    SELECT 
        FLOOR(dc.umur/10)*10 as age_group,
        COUNT(DISTINCT dc.customer_key) as customer_count,
        COUNT(fr.registration_key) as policy_count,
        SUM(fr.premium_amount) as total_premium,
        AVG(fr.premium_amount) as avg_premium_per_customer,
        SUM(fr.sum_assured) as total_sum_assured,
        AVG(dc.lama_menjadi_pelanggan) as avg_customer_tenure_days
    FROM dim_customer dc
    LEFT JOIN fact_registration fr ON dc.customer_key = fr.customer_key
    WHERE dc.is_current = TRUE
    GROUP BY FLOOR(dc.umur/10)*10
    ORDER BY age_group
    """
    
    df = dw_hook.get_pandas_df(sql)
    
    # Save to CSV
    output_path = os.path.join(DASHBOARD_EXPORT_DIR, f'customer_metrics_{datetime.now().strftime("%Y%m%d")}.csv')
    df.to_csv(output_path, index=False)
    
    return f"Customer dashboard data exported to {output_path} with {len(df)} records"

def export_sales_dashboard_data(**kwargs):
    """
    Export data needed for the Sales Dashboard
    """
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get parameters from context
    params = kwargs.get('params', {})
    months_lookback = params.get('months_lookback', 12)
    
    # Query to get sales activities and conversion metrics
    sql = f"""
    SELECT 
        dt.year_actual,
        dt.month_name,
        de.department,
        de.jabatan,
        dp.nama_product,
        dp.jenis_product,
        COUNT(fs.activity_id) as total_activities,
        SUM(CASE WHEN fs.resulted_in_sale THEN 1 ELSE 0 END) as total_sales,
        SUM(CASE WHEN fs.resulted_in_sale THEN 1 ELSE 0 END)::float / 
            NULLIF(COUNT(fs.activity_id), 0) * 100 as conversion_rate,
        AVG(fs.duration_minutes) as avg_activity_duration
    FROM fact_sales_activity fs
    JOIN dim_employee de ON fs.employee_key = de.employee_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_time dt ON fs.time_key = dt.time_key
    WHERE dt.date_actual >= CURRENT_DATE - INTERVAL '{months_lookback} months'
      AND de.is_current = TRUE
    GROUP BY dt.year_actual, dt.month_name, de.department, de.jabatan, dp.nama_product, dp.jenis_product
    ORDER BY dt.year_actual, dt.month_name
    """
    
    df = dw_hook.get_pandas_df(sql)
    
    # Save to CSV
    output_path = os.path.join(DASHBOARD_EXPORT_DIR, f'sales_metrics_{datetime.now().strftime("%Y%m%d")}.csv')
    df.to_csv(output_path, index=False)
    
    return f"Sales dashboard data exported to {output_path} with {len(df)} records"

def export_claims_dashboard_data(**kwargs):
    """
    Export data needed for the Claims Dashboard
    """
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get parameters from context
    params = kwargs.get('params', {})
    months_lookback = params.get('months_lookback', 12)
    
    # Query to get claim metrics
    sql = f"""
    SELECT 
        dt.year_actual,
        dt.month_name,
        fc.claim_type,
        dpr.nama_product,
        dpr.jenis_product,
        COUNT(fc.claim_id) as total_claims,
        SUM(fc.claim_amount) as total_claim_amount,
        SUM(fc.approved_amount) as total_approved_amount,
        AVG(fc.processing_days) as avg_processing_days,
        SUM(CASE WHEN fc.rejection_flag THEN 1 ELSE 0 END) as rejected_claims,
        SUM(CASE WHEN fc.rejection_flag THEN 1 ELSE 0 END)::float / 
            NULLIF(COUNT(fc.claim_id), 0) * 100 as rejection_rate
    FROM fact_claim fc
    JOIN dim_policy dp ON fc.policy_key = dp.policy_key
    JOIN dim_product dpr ON dp.product_key = dpr.product_key
    JOIN dim_time dt ON fc.time_key_submitted = dt.time_key
    WHERE dt.date_actual >= CURRENT_DATE - INTERVAL '{months_lookback} months'
      AND dp.is_current = TRUE
      AND dpr.is_current = TRUE
    GROUP BY dt.year_actual, dt.month_name, fc.claim_type, dpr.nama_product, dpr.jenis_product
    ORDER BY dt.year_actual, dt.month_name
    """
    
    df = dw_hook.get_pandas_df(sql)
    
    # Save to CSV
    output_path = os.path.join(DASHBOARD_EXPORT_DIR, f'claims_metrics_{datetime.now().strftime("%Y%m%d")}.csv')
    df.to_csv(output_path, index=False)
    
    return f"Claims dashboard data exported to {output_path} with {len(df)} records"

def export_customer_service_dashboard_data(**kwargs):
    """
    Export data needed for the Customer Service Dashboard
    """
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get parameters from context
    params = kwargs.get('params', {})
    months_lookback = params.get('months_lookback', 12)
    
    # Query to get customer service metrics
    sql = f"""
    SELECT 
        dt.year_actual,
        dt.month_name,
        fcs.interaction_type,
        fcs.channel,
        fcs.complaint_category,
        COUNT(fcs.service_id) as total_interactions,
        AVG(fcs.duration_minutes) as avg_duration,
        AVG(fcs.satisfaction_rating) as avg_satisfaction,
        SUM(CASE WHEN fcs.follow_up_required THEN 1 ELSE 0 END) as follow_ups_required,
        AVG(fcs.resolution_time_hours) as avg_resolution_time
    FROM fact_customer_service fcs
    JOIN dim_time dt ON fcs.time_key_interaction = dt.time_key
    WHERE dt.date_actual >= CURRENT_DATE - INTERVAL '{months_lookback} months'
    GROUP BY dt.year_actual, dt.month_name, fcs.interaction_type, fcs.channel, fcs.complaint_category
    ORDER BY dt.year_actual, dt.month_name
    """
    
    df = dw_hook.get_pandas_df(sql)
    
    # Save to CSV
    output_path = os.path.join(DASHBOARD_EXPORT_DIR, f'customer_service_metrics_{datetime.now().strftime("%Y%m%d")}.csv')
    df.to_csv(output_path, index=False)
    
    return f"Customer service dashboard data exported to {output_path} with {len(df)} records"

def export_investment_dashboard_data(**kwargs):
    """
    Export data needed for the Investment Dashboard
    """
    dw_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    
    # Get parameters from context
    params = kwargs.get('params', {})
    months_lookback = params.get('months_lookback', 12)
    
    # Query to get investment metrics
    sql = f"""
    SELECT 
        dt.year_actual,
        dt.month_name,
        fi.investment_type,
        fi.investment_status,
        COUNT(fi.investment_id) as total_investments,
        SUM(fi.amount) as total_investment_amount,
        AVG(fi.return_percentage) as avg_return,
        AVG(fi.days_since_investment) as avg_investment_age
    FROM fact_investment fi
    JOIN dim_time dt ON fi.time_key_investment = dt.time_key
    WHERE dt.date_actual >= CURRENT_DATE - INTERVAL '{months_lookback} months'
    GROUP BY dt.year_actual, dt.month_name, fi.investment_type, fi.investment_status
    ORDER BY dt.year_actual, dt.month_name
    """
    
    df = dw_hook.get_pandas_df(sql)
    
    # Save to CSV
    output_path = os.path.join(DASHBOARD_EXPORT_DIR, f'investment_metrics_{datetime.now().strftime("%Y%m%d")}.csv')
    df.to_csv(output_path, index=False)
    
    return f"Investment dashboard data exported to {output_path} with {len(df)} records"

# Define the DAG
with DAG(
    'export_dashboard_data',
    default_args=default_args,
    description='Export data from data warehouse for dashboard visualization',
    schedule_interval='0 2 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dashboard', 'export', 'manual'],
    params={
        "months_lookback": 12,
        "export_type": "all",
        "description": "Manual or scheduled dashboard data export"
    },
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    # Create export tasks
    export_registration = PythonOperator(
        task_id='export_registration_data',
        python_callable=export_registration_dashboard_data,
        provide_context=True
    )
    
    export_customer = PythonOperator(
        task_id='export_customer_data',
        python_callable=export_customer_dashboard_data,
        provide_context=True
    )
    
    export_sales = PythonOperator(
        task_id='export_sales_data',
        python_callable=export_sales_dashboard_data,
        provide_context=True
    )
    
    export_claims = PythonOperator(
        task_id='export_claims_data',
        python_callable=export_claims_dashboard_data,
        provide_context=True
    )
    
    export_customer_service = PythonOperator(
        task_id='export_customer_service_data',
        python_callable=export_customer_service_dashboard_data,
        provide_context=True
    )
    
    export_investment = PythonOperator(
        task_id='export_investment_data',
        python_callable=export_investment_dashboard_data,
        provide_context=True
    )
    
    # Set task dependencies
    start >> [export_registration, export_customer, export_sales, 
              export_claims, export_customer_service, export_investment] >> end