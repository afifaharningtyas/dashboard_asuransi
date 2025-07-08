from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    'manual_etl_pipeline',
    default_args=default_args,
    description='Manual ETL pipeline designed to be triggered from UI',
    schedule_interval=None,  # No automatic scheduling - manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'manual'],
) as dag:
    
    # Start task
    start = DummyOperator(
        task_id='start',
    )
    
    # Trigger dimension DAGs
    trigger_customer_dim = TriggerDagRunOperator(
        task_id='trigger_customer_dimension',
        trigger_dag_id='etl_dim_customer',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )
    
    trigger_product_dim = TriggerDagRunOperator(
        task_id='trigger_product_dimension',
        trigger_dag_id='etl_dim_product',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )
    
    trigger_marketing_dim = TriggerDagRunOperator(
        task_id='trigger_marketing_dimension',
        trigger_dag_id='etl_dim_marketing_program',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )
    
    # Checkpoint after dimensions
    dimensions_loaded = DummyOperator(
        task_id='dimensions_loaded',
    )
    
    # Trigger fact DAGs
    trigger_registration_fact = TriggerDagRunOperator(
        task_id='trigger_registration_fact',
        trigger_dag_id='etl_fact_registration',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )
    
    # Add other fact DAGs here as needed
    
    # Checkpoint after facts
    facts_loaded = DummyOperator(
        task_id='facts_loaded',
    )
    
    # Export dashboard data
    trigger_dashboard_export = TriggerDagRunOperator(
        task_id='trigger_dashboard_export',
        trigger_dag_id='export_dashboard_data',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )
    
    # End task
    end = DummyOperator(
        task_id='end',
    )
    
    # Define dependencies
    start >> [trigger_customer_dim, trigger_product_dim, trigger_marketing_dim] >> dimensions_loaded
    dimensions_loaded >> trigger_registration_fact >> facts_loaded
    facts_loaded >> trigger_dashboard_export >> end