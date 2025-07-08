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

def extract_product_data():
    """
    Extract product data from source database
    """
    source_hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    
    # Extract product data
    sql = """
    SELECT 
        product_id, 
        nama_product, 
        jenis_product, 
        manfaat, 
        premi_dasar
    FROM product
    """
    
    df = source_hook.get_pandas_df(sql)
    
    return df

def transform_product_data(df):
    """
    Transform product data by adding calculated fields
    """
    # Add kategori_premi based on premi_dasar
    conditions = [
        (df['premi_dasar'] < 500000),
        (df['premi_dasar'] >= 500000) & (df['premi_dasar'] < 2000000),
        (df['premi_dasar'] >= 2000000)
    ]
    choices = ['Low', 'Medium', 'High']
    df['kategori_premi'] = pd.np.select(conditions, choices, default='Unknown')
    
    # Generate batch ID
    batch_id = f"PROD_LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Add SCD Type 2 metadata
    df['valid_from'] = datetime.now().date()
    df['valid_to'] = None
    df['is_current'] = True
    df['batch_id'] = batch_id
    
    return df

def load_dim_product(df):
    """
    Load product dimension using SCD Type 2 approach
    """
    target_hook = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    
    # Generate batch ID
    batch_id = df['batch_id'].iloc[0]
    
    # Iterate through each row and apply SCD Type 2 logic
    for _, row in df.iterrows():
        # Check if product already exists
        cursor.execute(
            "SELECT product_key FROM dim_product WHERE product_id = %s AND is_current = TRUE",
            (row['product_id'],)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Compare to see if anything changed
            cursor.execute(
                """
                SELECT COUNT(*) FROM dim_product 
                WHERE product_id = %s AND is_current = TRUE AND
                (
                    nama_product != %s OR
                    jenis_product != %s OR
                    manfaat != %s OR
                    premi_dasar != %s
                )
                """,
                (row['product_id'], row['nama_product'], row['jenis_product'], 
                 row['manfaat'], row['premi_dasar'])
            )
            has_changes = cursor.fetchone()[0] > 0
            
            if has_changes:
                # Update existing record (expire it)
                cursor.execute(
                    """
                    UPDATE dim_product 
                    SET is_current = FALSE, valid_to = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE product_id = %s AND is_current = TRUE
                    """,
                    (row['valid_from'], row['product_id'])
                )
                
                # Insert new record
                cursor.execute(
                    """
                    INSERT INTO dim_product (
                        product_id, nama_product, jenis_product, manfaat, premi_dasar,
                        kategori_premi, valid_from, valid_to, is_current, batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['product_id'], row['nama_product'], row['jenis_product'], 
                        row['manfaat'], row['premi_dasar'], row['kategori_premi'],
                        row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                    )
                )
        else:
            # Insert new product
            cursor.execute(
                """
                INSERT INTO dim_product (
                    product_id, nama_product, jenis_product, manfaat, premi_dasar,
                    kategori_premi, valid_from, valid_to, is_current, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['product_id'], row['nama_product'], row['jenis_product'], 
                    row['manfaat'], row['premi_dasar'], row['kategori_premi'],
                    row['valid_from'], row['valid_to'], row['is_current'], row['batch_id']
                )
            )
    
    # Commit the transaction
    conn.commit()
    conn.close()
    
    return f"Loaded {len(df)} product records with batch ID: {batch_id}"

def etl_dim_product():
    """
    ETL process for product dimension
    """
    # Extract
    df = extract_product_data()
    
    # Transform
    transformed_df = transform_product_data(df)
    
    # Load
    result = load_dim_product(transformed_df)
    
    return result

# Define the DAG
with DAG(
    'etl_dim_product',
    default_args=default_args,
    description='ETL process for product dimension',
    schedule_interval='0 1 * * *',  # Run daily at 1:00 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dimension', 'product'],
) as dag:
    
    # Task to run ETL process
    run_etl = PythonOperator(
        task_id='run_etl_dim_product',
        python_callable=etl_dim_product
    )