#!/usr/bin/env python3
import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime, timedelta
import pandas as pd

# Import konfigurasi database (pastikan config/db_config.py tersedia)
from config.db_config import DW_DB_PARAMS, SQL_QUERIES_DIR

# Daftar tabel yang dibutuhkan
REQUIRED_TABLES = [
    'dim_time', 'dim_customer', 'dim_product', 'dim_employee',
    'dim_marketing_program', 'dim_policy',
    'fact_registration', 'fact_claim', 'fact_sales_activity',
    'fact_investment', 'fact_customer_service'
]


def connect_postgres(dbname):
    return psycopg2.connect(
        dbname=dbname,
        user=DW_DB_PARAMS['user'],
        password=DW_DB_PARAMS['password'],
        host=DW_DB_PARAMS['host'],
        port=DW_DB_PARAMS['port']
    )


def db_exists():
    """
    Cek apakah database DW_DB_PARAMS['database'] sudah ada.
    """
    conn = connect_postgres('postgres')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    dbname = DW_DB_PARAMS['database']
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    print(f"Database '{dbname}' exists: {exists}")
    return exists


def create_database():
    """
    Buat database baru.
    """
    conn = connect_postgres('postgres')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    dbname = DW_DB_PARAMS['database']
    print(f"Creating database '{dbname}'...")
    cur.execute(f"CREATE DATABASE {dbname}")
    cur.close()
    conn.close()
    print("Database created.")


def get_missing_tables():
    """
    Cek tabel yang belum ada di schema public.
    """
    conn = connect_postgres(DW_DB_PARAMS['database'])
    cur = conn.cursor()
    missing = []
    for tbl in REQUIRED_TABLES:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)",
            (tbl,)
        )
        if not cur.fetchone()[0]:
            missing.append(tbl)
    cur.close()
    conn.close()
    if missing:
        print(f"Missing tables: {missing}")
    else:
        print("All required tables exist.")
    return missing


def create_tables(tables):
    """
    Buat tabel menggunakan file SQL di SQL_QUERIES_DIR.
    """
    conn = connect_postgres(DW_DB_PARAMS['database'])
    cur = conn.cursor()
    for tbl in tables:
        sql_file = os.path.join(SQL_QUERIES_DIR, f"{tbl}.sql")
        if os.path.exists(sql_file):
            with open(sql_file, 'r') as f:
                ddl = f.read()
            print(f"Executing DDL for {tbl}...")
            cur.execute(ddl)
        else:
            print(f"SQL file not found for table '{tbl}': {sql_file}")
    conn.commit()
    cur.close()
    conn.close()
    print("Table creation completed.")


def load_time_dimension():
    """
    Generate dan load dim_time.
    """
    start = datetime.now() - timedelta(days=365*5)
    end = datetime.now() + timedelta(days=365*5)
    dates = pd.date_range(start=start, end=end)
    records = []
    for d in dates:
        records.append(
            (
                d.date(), d.dayofweek, d.day_name(), d.day, d.dayofyear,
                d.isocalendar()[1], d.month, d.month_name(), d.quarter,
                d.year, d.dayofweek >= 5, False, None, d.year
            )
        )
    conn = connect_postgres(DW_DB_PARAMS['database'])
    cur = conn.cursor()
    print(f"Loading {len(records)} records into dim_time...")
    insert_sql = """
        INSERT INTO dim_time (
            date_actual, day_of_week, day_name, day_of_month, day_of_year,
            week_of_year, month_actual, month_name, quarter_actual, year_actual,
            is_weekend, is_holiday, holiday_name, fiscal_year
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_actual) DO NOTHING
    """
    for rec in records:
        cur.execute(insert_sql, rec)
    conn.commit()
    cur.close()
    conn.close()
    print("Time dimension loading completed.")


def main():
    print("DB Params: ", DW_DB_PARAMS)
    # Step 1: DB
    if not db_exists():
        create_database()
    else:
        print("Skipping database creation.")
    # Step 2: Tables
    missing = get_missing_tables()
    if missing:
        create_tables(missing)
    else:
        print("Skipping table creation.")
    # Step 3: Load time dim
    load_time_dimension()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
