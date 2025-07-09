import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

# Import konfigurasi database
from config.db_config import SOURCE_DB_CONN_ID, DW_DB_CONN_ID

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_policy_data():
    src = PostgresHook(postgres_conn_id=SOURCE_DB_CONN_ID)
    sql = """
      SELECT 
          policy_id,
          customer_id,
          product_id,
          tanggal_mulai,
          tanggal_berakhir,
          (tanggal_berakhir - tanggal_mulai)::int AS durasi_polis
      FROM policy
    """
    return src.get_pandas_df(sql)

def extract_dim_keys():
    dw = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    # mapping customer
    df_cust = dw.get_pandas_df("""
      SELECT customer_id, customer_key
      FROM dim_customer
      WHERE is_current = TRUE
    """)
    # mapping product
    df_prod = dw.get_pandas_df("""
      SELECT product_id, product_key
      FROM dim_product
      WHERE is_current = TRUE
    """)
    return df_cust, df_prod

def transform_and_enrich(df, df_cust, df_prod):
    # transform tanggal dan status
    now = datetime.now().date()
    df['tanggal_mulai']    = pd.to_datetime(df['tanggal_mulai']).dt.date
    df['tanggal_berakhir'] = pd.to_datetime(df['tanggal_berakhir']).dt.date

    conditions = [
        (df['tanggal_mulai'] > now),
        (df['tanggal_mulai'] <= now) & (df['tanggal_berakhir'] >= now),
        (df['tanggal_berakhir'] < now)
    ]
    choices = ['Future', 'Active', 'Expired']
    df['status_polis'] = np.select(conditions, choices, default='Unknown')

    batch_id = f"POLICY_LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df['valid_from'] = now
    df['valid_to']   = None
    df['is_current'] = True
    df['batch_id']   = batch_id

    df['customer_id'] = df['customer_id'].astype(str)
    df['product_id']  = df['product_id'].astype(str)
    df_cust['customer_id'] = df_cust['customer_id'].astype(str)
    df_prod['product_id']  = df_prod['product_id'].astype(str)
    # merge mapping keys
    df = df.merge(df_cust, on='customer_id', how='left') \
           .merge(df_prod, on='product_id', how='left')

    return df

def load_dim_policy(df):
    dw = PostgresHook(postgres_conn_id=DW_DB_CONN_ID)
    conn = dw.get_conn()
    cur  = conn.cursor()
    batch_id = df['batch_id'].iat[0]
    print('dffinal -----',df.head(2))
    for _, row in df.iterrows():
        # cek existing current
        cur.execute(
            "SELECT policy_key FROM dim_policy WHERE policy_id = %s AND is_current = TRUE",
            (row['policy_id'],)
        )
        existing = cur.fetchone()

        if existing:
            # cek perubahan
            cur.execute(
                """
                SELECT COUNT(*) 
                FROM dim_policy 
                WHERE policy_id = %s AND is_current = TRUE
                  AND (
                    customer_id   != %s OR
                    product_id    != %s OR
                    tanggal_mulai != %s OR
                    tanggal_berakhir != %s
                  )
                """,
                (
                  row['policy_id'], row['customer_id'], row['product_id'],
                  row['tanggal_mulai'], row['tanggal_berakhir']
                )
            )
            if cur.fetchone()[0] > 0:
                # deactivate old
                cur.execute(
                    """
                    UPDATE dim_policy
                    SET is_current = FALSE,
                        valid_to    = %s,
                        updated_at  = CURRENT_TIMESTAMP
                    WHERE policy_id = %s
                      AND is_current = TRUE
                    """,
                    (row['valid_from'], row['policy_id'])
                )
                # insert new version
                cur.execute(
                    """
                    INSERT INTO dim_policy (
                      policy_id, customer_id, product_id,
                      customer_key, product_key,
                      tanggal_mulai, tanggal_berakhir,
                      durasi_polis, status_polis,
                      valid_from, valid_to,
                      is_current, batch_id
                    ) VALUES (
                      %s, %s, %s,
                      %s, %s,
                      %s, %s,
                      %s, %s,
                      %s, %s,
                      %s, %s
                    )
                    """,
                    (
                      row['policy_id'], row['customer_id'], row['product_id'],
                      row['customer_key'], row['product_key'],
                      row['tanggal_mulai'], row['tanggal_berakhir'],
                      row['durasi_polis'], row['status_polis'],
                      row['valid_from'], row['valid_to'],
                      row['is_current'], row['batch_id']
                    )
                )
        else:
            # langsung insert jika belum pernah ada
            cur.execute(
                """
                INSERT INTO dim_policy (
                  policy_id, customer_id, product_id,
                  customer_key, product_key,
                  tanggal_mulai, tanggal_berakhir,
                  durasi_polis, status_polis,
                  valid_from, valid_to,
                  is_current, batch_id
                ) VALUES (
                  %s, %s, %s,
                  %s, %s,
                  %s, %s,
                  %s, %s,
                  %s, %s,
                  %s, %s
                )
                """,
                (
                  row['policy_id'], row['customer_id'], row['product_id'],
                  row['customer_key'], row['product_key'],
                  row['tanggal_mulai'], row['tanggal_berakhir'],
                  row['durasi_polis'], row['status_polis'],
                  row['valid_from'], row['valid_to'],
                  row['is_current'], row['batch_id']
                )
            )

    conn.commit()
    conn.close()
    return f"Loaded {len(df)} records (batch {batch_id})"

def etl_dim_policy():
    print("Starting ETL dim_policy…")
    df_raw      = extract_policy_data()
    df_cust, df_prod = extract_dim_keys()
    print('df cust-----',df_cust.head(2))
    df_enriched = transform_and_enrich(df_raw, df_cust, df_prod)
    return load_dim_policy(df_enriched)

with DAG(
    'etl_dim_policy',
    default_args=default_args,
    description='ETL process for policy dimension',
    schedule_interval='0 1 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dimension', 'policy']
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_dim_policy',
        python_callable=etl_dim_policy
    )
