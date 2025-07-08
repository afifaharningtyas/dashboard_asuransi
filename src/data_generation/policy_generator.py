from faker import Faker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys
from db_config import create_database_if_not_exists, save_to_db, get_db_engine

fake = Faker('id_ID')

# Status polis
POLICY_STATUS = ['Active', 'Lapsed', 'Terminated', 'Matured']

# Metode pembayaran
PAYMENT_METHODS = ['Credit Card', 'Bank Transfer', 'Direct Debit', 'Cash', 'E-wallet']

def get_existing_data(table_name):
    """Mendapatkan data dari tabel yang sudah ada di database"""
    try:
        engine = get_db_engine()
        df = pd.read_sql(f'SELECT * FROM {table_name}', engine)
        engine.dispose()
        return df
    except Exception as e:
        # Jika gagal membaca dari database, coba baca dari file CSV
        try:
            script_path = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
            csv_path = os.path.join(project_root, 'data', 'raw', f'{table_name}.csv')
            df = pd.read_csv(csv_path)
            return df
        except Exception as csv_error:
            print(f"Error reading data: {str(csv_error)}")
            return None

def generate_policies(num_records=1500):
    """Generate policy data with references to customers and products"""
    # Get existing customers and products
    customers = get_existing_data('customer')
    products = get_existing_data('product')
    
    if customers is None or products is None:
        print("Error: Cannot generate policies without customer and product data")
        return None
    
    customer_ids = customers['customer_id'].tolist()
    product_ids = products['product_id'].tolist()
    
    policies = []
    
    for _ in range(num_records):
        # Select a random customer and product
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        
        # Generate policy start date (between 3 years ago and today)
        policy_date = fake.date_between(start_date='-3y', end_date='today')
        
        # Generate term and calculate end date
        policy_term_years = random.choice([1, 3, 5, 10, 15, 20])
        end_date = datetime.combine(policy_date, datetime.min.time()) + timedelta(days=365*policy_term_years)
        
        # Determine status based on dates
        today = datetime.now()
        
        if end_date.date() < today.date():
            status = 'Matured'
        elif random.random() < 0.1:  # 10% chance policy is not active
            status = random.choice(['Lapsed', 'Terminated'])
        else:
            status = 'Active'
        
        # Get product premium from products dataframe
        product_row = products[products['product_id'] == product_id]
        base_premium = product_row['premi_dasar'].values[0] if not product_row.empty else 1000000
        
        # Adjust premium based on term
        premium = base_premium * (1 + (policy_term_years / 10))
        
        # Generate unique policy number
        policy_id = fake.unique.bothify(text='POL-######')
        
        policy = {
            'policy_id': policy_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'tanggal_mulai': policy_date,
            'tanggal_berakhir': end_date.date(),
            'status': status,
            'premium': int(premium),
            'payment_frequency': random.choice(['Monthly', 'Quarterly', 'Semi-Annual', 'Annual']),
            'payment_method': random.choice(PAYMENT_METHODS),
            'sum_assured': int(premium * random.randint(100, 500))
        }
        policies.append(policy)
    
    df = pd.DataFrame(policies)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate policy data
    policy_df = generate_policies()
    
    if policy_df is not None:
        # Save to CSV as backup
        output_path = os.path.join(data_dir, 'policy.csv')
        policy_df.to_csv(output_path, index=False)
        print(f"Generated {len(policy_df)} policy records saved to {output_path}")
        
        # Create database if not exists and save to PostgreSQL
        try:
            create_database_if_not_exists()
            save_to_db(policy_df, 'policy')
            print("Data successfully saved to PostgreSQL database")
        except Exception as e:
            print(f"Error saving to PostgreSQL: {str(e)}")
            print("Data was saved to CSV only")
    else:
        print("Failed to generate policy data")