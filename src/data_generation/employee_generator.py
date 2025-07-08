from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
import random
import os
from db_config import create_database_if_not_exists, save_to_db

fake = Faker('id_ID')

DEPARTMENTS = ['Sales', 'Customer Service', 'Claims', 'Underwriting', 'Finance']
POSITIONS = {
    'Sales': ['Sales Executive', 'Senior Sales Executive', 'Sales Manager', 'Regional Sales Manager'],
    'Customer Service': ['CS Officer', 'Senior CS Officer', 'CS Supervisor', 'CS Manager'],
    'Claims': ['Claims Officer', 'Claims Analyst', 'Claims Supervisor', 'Claims Manager'],
    'Underwriting': ['Underwriter', 'Senior Underwriter', 'Underwriting Supervisor', 'Underwriting Manager'],
    'Finance': ['Finance Staff', 'Finance Analyst', 'Finance Supervisor', 'Finance Manager']
}

# def generate_employees(num_records=200):
#     employees = []
    
#     for _ in range(num_records):
#         department = random.choice(DEPARTMENTS)
#         position = random.choice(POSITIONS[department])
#         join_date = fake.date_between(start_date='-5y', end_date='today')
        
#         employee = {
#             'employee_id': fake.unique.random_number(digits=6),
#             'nama': fake.name(),
#             'position': position,
#             'department': department,
#             'join_date': join_date
#         }
#         employees.append(employee)
    
#     df = pd.DataFrame(employees)
#     return df
def generate_employees(num_records=200):
    """
    Generate DataFrame karyawan dengan kolom:
    employee_id, nama, tanggal_lahir, alamat, jabatan, department, tanggal_masuk
    """
    records = []
    for _ in range(num_records):
        dept = random.choice(DEPARTMENTS)
        jabatan = random.choice(POSITIONS[dept])
        # Tanggal lahir antara usia 18-60
        dob = fake.date_of_birth(minimum_age=18, maximum_age=60)
        alamat = fake.address().replace("\n", ", ")
        # Tanggal masuk antara 0 hingga 5 tahun yang lalu
        join_date = fake.date_between(start_date='-5y', end_date='today')

        record = {
            'employee_id': fake.unique.random_number(digits=6),
            'nama': fake.name(),
            'tanggal_lahir': dob,
            'alamat': alamat,
            'jabatan': jabatan,
            'department': dept,
            'tanggal_masuk': join_date
        }
        records.append(record)
    df = pd.DataFrame(records)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate employee data
    employee_df = generate_employees()
    
    # Save to CSV as backup
    output_path = os.path.join(data_dir, 'employee.csv')
    employee_df.to_csv(output_path, index=False)
    print(f"Generated {len(employee_df)} employee records saved to {output_path}")
    
    # Create database if not exists and save to PostgreSQL
    try:
        create_database_if_not_exists()
        save_to_db(employee_df, 'employee')
        print("Data successfully saved to PostgreSQL database")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {str(e)}")
        print("Data was saved to CSV only")