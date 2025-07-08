from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# This function allows conditional triggering of DAGs based on run mode
def should_run_dimension(run_mode, **kwargs):
    """Check if dimension ETL should run based on run_mode parameter"""
    return run_mode in ['full', 'dimension_only']

def should_run_fact(run_mode, **kwargs):
    """Check if fact ETL should run based on run_mode parameter"""
    return run_mode in ['full', 'fact_only']

def should_run_init(skip_init, **kwargs):
    """Check if init_dw should run based on skip_init parameter"""
    # Convert string parameter to boolean properly
    if isinstance(skip_init, str):
        skip_init = skip_init.lower() == 'true'
    return not skip_init

# Define the DAG
with DAG(
    'master_etl',
    default_args=default_args,
    description='Master ETL process to orchestrate all ETL DAGs (can be triggered manually from GUI)',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'master', 'manual'],
    # Add parameters to make manual triggering more configurable
    params={
        "run_mode": "full",  # Options: full, dimension_only, fact_only
        "description": "Manual or scheduled ETL process",
        "skip_init": False,  # Skip initialization step if data warehouse already exists
    },
) as dag:
    
    # Start task
    start = DummyOperator(
        task_id='start',
    )
    
    # Check if init should be run
    check_init = ShortCircuitOperator(
        task_id='check_init',
        python_callable=should_run_init,
        op_kwargs={'skip_init': "{{ params.skip_init }}"},
        do_xcom_push=False
    )
    
    # Initialize DW schema (only triggers if it doesn't exist yet)
    init_dw = TriggerDagRunOperator(
        task_id='trigger_init_dw',
        trigger_dag_id='init_data_warehouse',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )
    
    # Check if dimension ETL should run
    check_dim = ShortCircuitOperator(
        task_id='check_dimension_etl',
        python_callable=should_run_dimension,
        op_kwargs={'run_mode': "{{ params.run_mode }}"},
        do_xcom_push=False
    )
    
    # Load Dimensions (run in parallel)
    load_dim_customer = TriggerDagRunOperator(
        task_id='trigger_dim_customer',
        trigger_dag_id='etl_dim_customer',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_dim_product = TriggerDagRunOperator(
        task_id='trigger_dim_product',
        trigger_dag_id='etl_dim_product',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_dim_marketing_program = TriggerDagRunOperator(
        task_id='trigger_dim_marketing_program',
        trigger_dag_id='etl_dim_marketing_program',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_dim_employee = TriggerDagRunOperator(
        task_id='trigger_dim_employee',
        trigger_dag_id='etl_dim_employee',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_dim_policy = TriggerDagRunOperator(
        task_id='trigger_dim_policy',
        trigger_dag_id='etl_dim_policy',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_dim_time = TriggerDagRunOperator(
        task_id='trigger_dim_time',
        trigger_dag_id='etl_dim_time',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    dim_complete = DummyOperator(
        task_id='dim_loading_complete',
    )
    
    # Check if fact ETL should run
    check_fact = ShortCircuitOperator(
        task_id='check_fact_etl',
        python_callable=should_run_fact,
        op_kwargs={'run_mode': "{{ params.run_mode }}"},
        do_xcom_push=False
    )
    
    # Load Facts (after dimensions are loaded)
    load_fact_registration = TriggerDagRunOperator(
        task_id='trigger_fact_registration',
        trigger_dag_id='etl_fact_registration',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_fact_claim = TriggerDagRunOperator(
        task_id='trigger_fact_claim',
        trigger_dag_id='etl_fact_claim',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_fact_sales_activity = TriggerDagRunOperator(
        task_id='trigger_fact_sales_activity',
        trigger_dag_id='etl_fact_sales_activity',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_fact_investment = TriggerDagRunOperator(
        task_id='trigger_fact_investment',
        trigger_dag_id='etl_fact_investment',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    load_fact_customer_service = TriggerDagRunOperator(
        task_id='trigger_fact_customer_service',
        trigger_dag_id='etl_fact_customer_service',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    fact_complete = DummyOperator(
        task_id='fact_loading_complete',
    )
    
    # Export dashboard
    export_dashboard = TriggerDagRunOperator(
        task_id='trigger_export_dashboard',
        trigger_dag_id='export_dashboard_data',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        conf={"parent_run_id": "{{ run_id }}"}
    )
    
    # End task
    end = DummyOperator(
        task_id='end',
    )
    
    # Define dependencies
    start >> check_init
    check_init >> init_dw
    
    # Connect init to dimension check or directly to fact check depending on whether dimensions should be run
    init_dw >> check_dim
    
    # Dimension ETL path
    check_dim >> [load_dim_customer, load_dim_product, load_dim_marketing_program, 
                 load_dim_employee, load_dim_policy, load_dim_time]
    [load_dim_customer, load_dim_product, load_dim_marketing_program, 
     load_dim_employee, load_dim_policy, load_dim_time] >> dim_complete
    
    # Connect dimension completion to fact check
    dim_complete >> check_fact
    
    # Fact ETL path
    check_fact >> [load_fact_registration, load_fact_claim, load_fact_sales_activity, 
                  load_fact_investment, load_fact_customer_service]
    [load_fact_registration, load_fact_claim, load_fact_sales_activity, 
     load_fact_investment, load_fact_customer_service] >> fact_complete
    
    # Dashboard export and end
    fact_complete >> export_dashboard >> end
    
    # Alternative paths for skipping steps
    # If init is skipped, go directly to dimension check
    check_init >> check_dim
    
    # If dimensions are skipped, go directly to fact check
    check_dim >> check_fact
    
    # If facts are skipped, go directly to dashboard export
    check_fact >> export_dashboard