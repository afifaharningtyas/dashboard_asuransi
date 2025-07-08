"""
Database connection configuration for Airflow DAGs
"""

# Source Database (PostgreSQL)
SOURCE_DB_CONN_ID = "postgres_insurance_bi"
SOURCE_DB_PARAMS = {
    'host': 'host.docker.internal',
    'port': 5432,
    'database': 'insurance_bi',
    'user': 'postgres',
    'password': 'postgres'
}

# Target Data Warehouse (same PostgreSQL instance, different database)
DW_DB_CONN_ID = "postgres_insurance_dw"
DW_DB_PARAMS = {
    'host': 'host.docker.internal',
    'port': 5432,
    'database': 'insurance_dw',
    'user': 'postgres',
    'password': 'postgres'
}

# SQL query templates directory
SQL_QUERIES_DIR = '/opt/airflow/dags/sql'