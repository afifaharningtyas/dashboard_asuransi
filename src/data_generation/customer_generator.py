from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
import random
import os
import sys
from db_config import create_database_if_not_exists, save_to_db

fake = Faker('id_ID')  # Using Indonesian locale

def generate_customers(num_records=100000):
    customers = []
    
    for _ in range(num_records):
        customer = {
            'customer_id': fake.unique.random_number(digits=8),
            'nama': fake.name(),
            'tanggal_lahir': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'alamat': fake.address(),
            'referral_code': fake.random_element(elements=[None] + [fake.bothify(text='REF-####') for _ in range(5)]),
            'tanggal_daftar': fake.date_between(start_date='-3y', end_date='today')
        }
        customers.append(customer)
    
    df = pd.DataFrame(customers)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate customer data
    customer_df = generate_customers()
    
    # Save to CSV as backup
    output_path = os.path.join(data_dir, 'customer.csv')
    customer_df.to_csv(output_path, index=False)
    print(f"Generated {len(customer_df)} customer records saved to {output_path}")
    
    # Create database if not exists and save to PostgreSQL
    try:
        create_database_if_not_exists()
        save_to_db(customer_df, 'customer')
        print("Data successfully saved to PostgreSQL database")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {str(e)}")
        print("Data was saved to CSV only")