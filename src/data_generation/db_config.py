import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Database connection configuration
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'  # Ganti dengan password yang sesuai
DB_HOST = 'localhost'
DB_PORT = '5432'  # Port 5432 sesuai dengan PostgreSQL lokal
DB_NAME = 'insurance_bi'

def get_db_engine():
    """Create and return a SQLAlchemy database engine"""
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def create_database_if_not_exists():
    """Create the database if it doesn't exist"""
    # Connect to the default 'postgres' database to create our new database
    postgres_conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
    postgres_engine = create_engine(postgres_conn_string)
    
    # Check if our database exists
    with postgres_engine.connect() as conn:
        # Menggunakan text() untuk membuat SQL yang eksekusi
        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'"))
        exists = result.fetchone() is not None
        
        if not exists:
            # Harus commit transaction dulu sebelum CREATE DATABASE
            conn.execute(text("COMMIT"))
            conn.execute(text(f'CREATE DATABASE {DB_NAME}'))
            print(f"Database '{DB_NAME}' created.")
        else:
            print(f"Database '{DB_NAME}' already exists.")
    
    postgres_engine.dispose()

def save_to_db(df, table_name, if_exists='replace'):
    """Save a dataframe to PostgreSQL database"""
    engine = get_db_engine()
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"Saved {len(df)} records to table '{table_name}'")
    engine.dispose()

def check_table_exists(table_name):
    """Check if a table exists in the database"""
    engine = get_db_engine()
    inspector = inspect(engine)
    exists = table_name in inspector.get_table_names()
    engine.dispose()
    return exists