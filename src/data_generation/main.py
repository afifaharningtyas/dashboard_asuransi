import os
import pandas as pd
from datetime import datetime
import sys

# Import semua generator
from customer_generator import generate_customers
from product_generator import generate_products
from employee_generator import generate_employees
from marketing_program_generator import generate_marketing_programs
from policy_generator import generate_policies, get_existing_data
from claim_generator import generate_claims
from sales_activity_generator import generate_sales_activities
from investment_generator import generate_investments, generate_investment_returns
from customer_service_generator import generate_customer_service
from db_config import create_database_if_not_exists, save_to_db

def print_dataframe_sample(df, title, rows=2):
    """Menampilkan sampel dataframe dengan judul"""
    print(f"\n{'='*50}")
    print(f"{title} - {len(df)} records generated")
    print(f"{'='*50}")
    print(df.head(rows))
    print(f"{'='*50}\n")

def generate_and_save_data(generator_func, table_name, dependency_tables=None, **kwargs):
    """Membuat data menggunakan generator, menyimpan ke file CSV dan PostgreSQL"""
    
    # Jika ada dependensi, pastikan data dependensi sudah ada
    if dependency_tables:
        for dep_table in dependency_tables:
            dep_data = get_existing_data(dep_table)
            if dep_data is None or len(dep_data) == 0:
                print(f"Error: {table_name} generation requires {dep_table} data which doesn't exist")
                return None
    
    # Generate data
    print(f"\nGenerating {table_name} data...")
    df = generator_func(**kwargs)
    
    if df is None:
        print(f"Failed to generate {table_name} data")
        return None
    
    # Menampilkan sampel data
    print_dataframe_sample(df, f"{table_name.upper()} DATA", rows=2)
    
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Save to CSV as backup
    output_path = os.path.join(data_dir, f'{table_name}.csv')
    df.to_csv(output_path, index=False)
    print(f"✓ {len(df)} {table_name} records saved to {output_path}")
    
    # Save to PostgreSQL
    try:
        create_database_if_not_exists()
        save_to_db(df, table_name)
        print(f"✓ {table_name} data successfully saved to PostgreSQL database")
    except Exception as e:
        print(f"✗ Error saving {table_name} to PostgreSQL: {str(e)}")
        print(f"  {table_name} data was saved to CSV only")
    
    return df

def main():
    """
    Membuat semua data dan menyimpannya ke CSV dan PostgreSQL
    """
    print(f"DATA GENERATION STARTED AT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Generate master data (tidak bergantung pada data lain)
    customer_df = generate_and_save_data(generate_customers, 'customer', num_records=100000)
    product_df = generate_and_save_data(generate_products, 'product', num_records=100)
    employee_df = generate_and_save_data(generate_employees, 'employee', num_records=5000)
    marketing_program_df = generate_and_save_data(
        generate_marketing_programs, 
        'marketing_program', 
        num_records=200
    )
    
    # 2. Generate transactional data (bergantung pada master data)
    policy_df = generate_and_save_data(
        generate_policies, 
        'policy',
        dependency_tables=['customer', 'product'], 
        num_records=200000
    )
    
    claim_df = generate_and_save_data(
        generate_claims, 
        'claim',
        dependency_tables=['policy', 'product'], 
        num_records=120000
    )
    
    sales_activity_df = generate_and_save_data(
        generate_sales_activities, 
        'sales_activity',
        dependency_tables=['customer', 'employee', 'product'], 
        num_records=300000
    )
    
    investment_df = generate_and_save_data(
        generate_investments, 
        'investment',
        dependency_tables=['policy'], 
        num_records=50000
    )
    
    if investment_df is not None:
        investment_return_df = generate_and_save_data(
            generate_investment_returns, 
            'investment_return',
            investment_df=investment_df, 
            periods=5
        )
    
    customer_service_df = generate_and_save_data(
        generate_customer_service, 
        'customer_service',
        dependency_tables=['customer', 'employee', 'policy'], 
        num_records=100000
    )
    
    print(f"\nDATA GENERATION COMPLETED AT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
if __name__ == "__main__":
    main()