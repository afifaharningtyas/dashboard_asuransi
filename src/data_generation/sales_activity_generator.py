from faker import Faker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys
from db_config import create_database_if_not_exists, save_to_db, get_db_engine

fake = Faker('id_ID')

# Activity types
ACTIVITY_TYPES = [
    'Initial Contact',
    'Product Presentation',
    'Follow-up Call',
    'Client Meeting',
    'Contract Signing',
    'Post-sale Service'
]

# Activity status
ACTIVITY_STATUS = [
    'Scheduled',
    'Completed',
    'Cancelled',
    'Postponed'
]

# Activity channels
ACTIVITY_CHANNELS = [
    'Phone Call',
    'Email',
    'In-person Meeting',
    'Video Conference',
    'Social Media',
    'WhatsApp'
]

NOTES_TEMPLATES = [
    # Positive interactions
    "Klien sangat antusias dengan produk yang ditawarkan. Diskusi berjalan lancar dan produktif.",
    "Presentasi produk mendapat respon positif dari klien. Ada ketertarikan untuk melanjutkan diskusi.",
    "Pertemuan berjalan baik, klien memberikan feedback yang konstruktif tentang proposal.",
    "Klien menunjukkan minat yang tinggi terhadap fitur-fitur produk yang dijelaskan.",
    "Diskusi tentang kebutuhan klien berlangsung mendalam dan informatif.",
    "Klien merasa puas dengan layanan yang diberikan selama ini.",
    "Terjadi kesepahaman yang baik mengenai spesifikasi produk yang dibutuhkan.",
    "Klien mengapresiasi penjelasan detail yang diberikan tentang solusi yang ditawarkan.",
    
    # Neutral interactions
    "Melakukan follow-up rutin untuk memastikan kepuasan klien terhadap produk.",
    "Diskusi mengenai timeline implementasi dan tahapan-tahapan yang akan dilakukan.",
    "Klien meminta waktu untuk mempertimbangkan proposal yang telah diberikan.",
    "Presentasi produk telah dilakukan, menunggu feedback dari klien.",
    "Melakukan pengecekan kebutuhan tambahan yang mungkin diperlukan klien.",
    "Dokumentasi kebutuhan klien telah dicatat dengan lengkap.",
    "Klien meminta penjelasan lebih detail mengenai beberapa aspek teknis.",
    "Proses negosiasi masih berlangsung, beberapa poin masih dalam pembahasan.",
    
    # Challenging interactions
    "Klien masih mempertimbangkan beberapa opsi lain yang tersedia di pasar.",
    "Terdapat beberapa concern dari klien yang perlu diaddress lebih lanjut.",
    "Klien meminta penyesuaian harga yang lebih kompetitif.",
    "Diskusi mengenai terms and conditions masih memerlukan negosiasi lebih lanjut.",
    "Klien butuh approval internal sebelum dapat melanjutkan ke tahap berikutnya.",
    "Terjadi sedikit miskomunikasi yang sudah berhasil diklarifikasi.",
    "Klien meminta revisi pada beberapa bagian dari proposal yang diajukan.",
    "Perlu koordinasi lebih lanjut dengan tim teknis untuk menjawab pertanyaan klien.",
    
    # Post-sale service
    "Layanan purna jual berjalan dengan baik, klien puas dengan support yang diberikan.",
    "Melakukan maintenance rutin dan pengecekan sistem yang sudah diimplementasi.",
    "Klien melaporkan beberapa issue minor yang sudah berhasil diselesaikan.",
    "Training pengguna telah dilakukan dengan hasil yang memuaskan.",
    "Klien meminta penambahan fitur atau upgrade untuk sistem yang sudah ada.",
    "Evaluasi berkala menunjukkan sistem berjalan sesuai dengan ekspektasi.",
    "Klien memberikan testimonial positif mengenai produk dan layanan yang diberikan."
]

def generate_indonesian_notes():
    """Generate contextual notes in Indonesian"""
    return random.choice(NOTES_TEMPLATES)

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

def generate_sales_activities(num_records=2000):
    """Generate sales activity data with references to employees, customers and products"""
    # Get existing employees, customers, products, and marketing programs
    employees = get_existing_data('employee')
    customers = get_existing_data('customer')
    products = get_existing_data('product')
    marketing_programs = get_existing_data('marketing_program')
    
    if employees is None or customers is None or products is None:
        print("Error: Cannot generate sales activities without employee, customer, and product data")
        return None
    
    # Filter only sales employees
    sales_employees = employees[employees['department'] == 'Sales']
    
    if len(sales_employees) == 0:
        print("Error: No sales employees found for generating activities")
        print("Using all employees instead")
        sales_employees = employees
    
    # Get IDs
    employee_ids = sales_employees['employee_id'].tolist()
    customer_ids = customers['customer_id'].tolist()
    product_ids = products['product_id'].tolist()
    
    # Marketing program IDs (if available)
    program_ids = marketing_programs['program_id'].tolist() if marketing_programs is not None else [None]
    
    activities = []
    
    for _ in range(num_records):
        # Select random IDs
        employee_id = random.choice(employee_ids)
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        
        # 40% chance of being associated with a marketing program
        program_id = random.choice(program_ids) if random.random() < 0.4 and program_ids != [None] else None
        
        # Generate activity date (between 1 year ago and 1 month in future)
        activity_date = fake.date_between(start_date='-1y', end_date='+1m')
        
        # Determine status based on date
        today = datetime.now().date()
        if activity_date > today:
            status = random.choice(['Scheduled', 'Postponed'])
        else:
            # Past activities are mostly completed, with some cancelled
            status = random.choices(['Completed', 'Cancelled'], weights=[0.85, 0.15], k=1)[0]
        
        # For completed activities, determine if it led to a sale (30% chance)
        resulted_in_sale = True if status == 'Completed' and random.random() < 0.3 else False
        
        # Generate activity details
        activity_type = random.choice(ACTIVITY_TYPES)
        
        # Duration depends on activity type
        if activity_type in ['Client Meeting', 'Product Presentation']:
            duration_minutes = random.randint(30, 120)  # 30 minutes to 2 hours
        elif activity_type == 'Initial Contact':
            duration_minutes = random.randint(5, 30)    # 5 to 30 minutes
        else:
            duration_minutes = random.randint(10, 60)   # 10 minutes to 1 hour
        
        activity = {
            'activity_id': fake.unique.bothify(text='ACT-######'),
            'employee_id': employee_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'program_id': program_id,
            'activity_date': activity_date,
            'activity_type': activity_type,
            'activity_channel': random.choice(ACTIVITY_CHANNELS),
            'duration_minutes': duration_minutes,
            'status': status,
            'notes': generate_indonesian_notes(), #fake.text(max_nb_chars=200),
            'resulted_in_sale': resulted_in_sale,
            'follow_up_date': fake.date_between(start_date=activity_date, end_date=activity_date + timedelta(days=30)) if random.random() < 0.5 else None
        }
        
        activities.append(activity)
    
    df = pd.DataFrame(activities)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate sales activity data
    activity_df = generate_sales_activities()
    
    if activity_df is not None:
        # Save to CSV as backup
        output_path = os.path.join(data_dir, 'sales_activity.csv')
        activity_df.to_csv(output_path, index=False)
        print(f"Generated {len(activity_df)} sales activity records saved to {output_path}")
        
        # Create database if not exists and save to PostgreSQL
        try:
            create_database_if_not_exists()
            save_to_db(activity_df, 'sales_activity')
            print("Data successfully saved to PostgreSQL database")
        except Exception as e:
            print(f"Error saving to PostgreSQL: {str(e)}")
            print("Data was saved to CSV only")
    else:
        print("Failed to generate sales activity data")