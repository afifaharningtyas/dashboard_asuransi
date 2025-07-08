from faker import Faker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys
from db_config import create_database_if_not_exists, save_to_db, get_db_engine

fake = Faker('id_ID')

# Investment types
INVESTMENT_TYPES = [
    'Money Market Fund',
    'Fixed Income Fund',
    'Balanced Fund',
    'Equity Fund',
    'Sharia Fund'
]

# Risk levels
RISK_LEVELS = {
    'Money Market Fund': 'Low',
    'Fixed Income Fund': 'Low-Medium',
    'Balanced Fund': 'Medium',
    'Equity Fund': 'High',
    'Sharia Fund': 'Medium'
}

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

def generate_investments(num_records=500):
    """Generate investment data with references to policies"""
    # Get existing policies
    policies = get_existing_data('policy')
    
    if policies is None:
        print("Error: Cannot generate investments without policy data")
        return None
    
    # Filter unit-linked policies (Asuransi Jiwa type products usually have investment component)
    policy_ids = policies['policy_id'].tolist()
    
    investments = []
    
    for _ in range(num_records):
        # Select a random policy
        # policy_id = random.choice(policy_ids)
        policy_id = fake.unique.bothify(text='POL-######')

        
        # Get policy data
        policy_row = policies[policies['policy_id'] == policy_id]
        
        if policy_row.empty:
            continue
            
        policy_premium = policy_row['premium'].values[0]
        
        # Generate investment type and associated risk level
        investment_type = random.choice(INVESTMENT_TYPES)
        risk_level = RISK_LEVELS[investment_type]
        
        # Generate investment date (must be after policy start date)
        policy_start = pd.to_datetime(policy_row['tanggal_mulai'].values[0])
        
        # Ensure policy start is not in the future
        if policy_start.date() > datetime.now().date():
            continue
            
        # Investment date should be between policy start and today
        investment_date = fake.date_between(start_date=policy_start, end_date='today')
        
        # Generate investment amount (based on policy premium)
        # Usually, a percentage of premium goes to investment
        investment_percentage = random.uniform(0.5, 0.9)  # 50% to 90% of premium
        investment_amount = int(policy_premium * investment_percentage)
        
        investment = {
            'investment_id': fake.unique.bothify(text='INV-######'),
            'policy_id': policy_id,
            'investment_type': investment_type,
            'risk_level': risk_level,
            'investment_date': investment_date,
            'amount': investment_amount,
            'fund_manager': fake.company(),
            'initial_nav': round(random.uniform(1000, 1500), 2),  # Initial Net Asset Value
            'units_purchased': round(investment_amount / random.uniform(1000, 1500), 4)
        }
        
        investments.append(investment)
    
    df = pd.DataFrame(investments)
    return df

def generate_investment_returns(investment_df=None, periods=5):
    """Generate investment return data for investments"""
    if investment_df is None:
        investment_df = get_existing_data('investment')
    
    if investment_df is None:
        print("Error: Cannot generate investment returns without investment data")
        return None
    
    returns = []
    
    # For each investment, generate multiple return records
    for _, investment in investment_df.iterrows():
        investment_id = investment['investment_id']
        investment_date = pd.to_datetime(investment['investment_date'])
        investment_type = investment['investment_type']
        initial_nav = investment['initial_nav']
        units = investment['units_purchased']
        
        # Performance depends on investment type
        if investment_type == 'Money Market Fund':
            mean_return = 0.005  # 0.5% monthly return on average
            std_dev = 0.002  # Low volatility
        elif investment_type == 'Fixed Income Fund':
            mean_return = 0.008  # 0.8% monthly return
            std_dev = 0.004
        elif investment_type == 'Balanced Fund':
            mean_return = 0.010  # 1% monthly return
            std_dev = 0.008
        elif investment_type == 'Equity Fund':
            mean_return = 0.015  # 1.5% monthly return
            std_dev = 0.015  # High volatility
        else:  # Sharia Fund
            mean_return = 0.009  # 0.9% monthly return
            std_dev = 0.006
        
        # Start with initial NAV
        current_nav = initial_nav
        
        # Generate return data for each period
        for i in range(1, periods + 1):
            # Calculate return date (monthly returns)
            return_date = investment_date + timedelta(days=30*i)
            
            # Skip if return date is in the future
            if return_date.date() > datetime.now().date():
                continue
                
            # Calculate monthly return percentage (can be negative)
            monthly_return_pct = np.random.normal(mean_return, std_dev)
            
            # Update NAV
            new_nav = current_nav * (1 + monthly_return_pct)
            
            # Calculate absolute return amount
            return_amount = (new_nav - current_nav) * units
            
            return_record = {
                'return_id': fake.unique.bothify(text='RET-######'),
                'investment_id': investment_id,
                'return_date': return_date.date(),
                'previous_nav': current_nav,
                'current_nav': round(new_nav, 2),
                'return_percentage': round(monthly_return_pct * 100, 2),  # Convert to percentage
                'return_amount': round(return_amount, 2),
                'ytd_return': round((new_nav / initial_nav - 1) * 100, 2)  # Year-to-date return
            }
            
            returns.append(return_record)
            
            # Update current NAV for next period
            current_nav = new_nav
    
    df = pd.DataFrame(returns)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate investment data
    investment_df = generate_investments()
    
    if investment_df is not None:
        # Save to CSV as backup
        output_path = os.path.join(data_dir, 'investment.csv')
        investment_df.to_csv(output_path, index=False)
        print(f"Generated {len(investment_df)} investment records saved to {output_path}")
        
        # Create database if not exists and save to PostgreSQL
        try:
            create_database_if_not_exists()
            save_to_db(investment_df, 'investment')
            print("Investment data successfully saved to PostgreSQL database")
        except Exception as e:
            print(f"Error saving investments to PostgreSQL: {str(e)}")
            print("Investment data was saved to CSV only")
        
        # Generate investment returns
        returns_df = generate_investment_returns(investment_df)
        
        if returns_df is not None:
            # Save to CSV as backup
            returns_output_path = os.path.join(data_dir, 'investment_return.csv')
            returns_df.to_csv(returns_output_path, index=False)
            print(f"Generated {len(returns_df)} investment return records saved to {returns_output_path}")
            
            # Save to PostgreSQL
            try:
                save_to_db(returns_df, 'investment_return')
                print("Investment return data successfully saved to PostgreSQL database")
            except Exception as e:
                print(f"Error saving investment returns to PostgreSQL: {str(e)}")
                print("Investment return data was saved to CSV only")
    else:
        print("Failed to generate investment data")