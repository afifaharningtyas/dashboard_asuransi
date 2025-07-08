from faker import Faker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys
from db_config import create_database_if_not_exists, save_to_db, get_db_engine

fake = Faker('id_ID')

# Tipe interaksi customer service
INTERACTION_TYPES = [
    'Pertanyaan',
    'Keluhan',
    'Perubahan Data',
    'Permintaan Informasi',
    'Pembatalan Polis',
    'Klaim',
    'Pembayaran'
]

# Channels
SERVICE_CHANNELS = [
    'Call Center',
    'Email',
    'Live Chat',
    'Mobile App',
    'Website',
    'Walk-in',
    'Social Media'
]

# Kategori keluhan
COMPLAINT_CATEGORIES = [
    'Layanan',
    'Produk',
    'Harga',
    'Proses Klaim',
    'Proses Pembayaran',
    'Informasi',
    'Agent'
]

# Status
SERVICE_STATUS = [
    'Open',
    'In Progress',
    'Closed',
    'Escalated',
    'Pending Customer'
]

# Description templates dalam bahasa Indonesia berdasarkan jenis interaksi
DESCRIPTION_TEMPLATES = {
    'Pertanyaan': [
        "Nasabah menanyakan tentang cara melakukan klaim polis asuransi yang dimilikinya.",
        "Nasabah bertanya mengenai prosedur pembayaran premi bulanan dan metode pembayaran yang tersedia.",
        "Nasabah ingin mengetahui manfaat dan coverage dari polis asuransi yang sedang berjalan.",
        "Nasabah menanyakan tentang cara mengubah data beneficiary pada polis asuransi.",
        "Nasabah bertanya mengenai proses renewal polis yang akan berakhir dalam waktu dekat.",
        "Nasabah ingin mengetahui status pembayaran premi dan saldo outstanding yang ada.",
        "Nasabah menanyakan tentang cara mengajukan rider tambahan pada polis existing.",
        "Nasabah bertanya mengenai prosedur dan persyaratan untuk mengajukan pinjaman polis.",
        "Nasabah ingin mengetahui nilai tunai dan surrender value dari polis unit link.",
        "Nasabah menanyakan tentang cara mengakses dan menggunakan aplikasi mobile insurance."
    ],
    'Keluhan': [
        "Nasabah mengeluhkan lambatnya proses pencairan klaim yang sudah diajukan 2 minggu lalu.",
        "Nasabah komplain mengenai pelayanan agent yang kurang responsif dan tidak memberikan informasi yang jelas.",
        "Nasabah mengeluhkan adanya potongan biaya yang tidak dijelaskan pada saat pembayaran premi.",
        "Nasabah komplain tentang kesulitan mengakses layanan online dan aplikasi mobile yang sering error.",
        "Nasabah mengeluhkan perbedaan informasi yang diberikan oleh agent dengan yang tertera di polis.",
        "Nasabah komplain mengenai proses verifikasi dokumen klaim yang terlalu rumit dan memakan waktu lama.",
        "Nasabah mengeluhkan tidak adanya notifikasi reminder untuk pembayaran premi yang jatuh tempo.",
        "Nasabah komplain tentang call center yang sulit dihubungi dan sering busy.",
        "Nasabah mengeluhkan adanya mis-selling dari agent yang tidak menjelaskan risiko produk dengan baik.",
        "Nasabah komplain mengenai pelayanan di kantor cabang yang kurang memuaskan dan antrian yang panjang."
    ],
    'Perubahan Data': [
        "Nasabah meminta perubahan alamat domisili karena pindah ke kota lain.",
        "Nasabah ingin mengubah nomor telepon yang terdaftar di sistem karena ganti nomor.",
        "Nasabah meminta perubahan data beneficiary dari orang tua ke pasangan.",
        "Nasabah ingin mengubah metode pembayaran dari transfer manual ke auto debit.",
        "Nasabah meminta perubahan alamat email untuk keperluan komunikasi dan notifikasi.",
        "Nasabah ingin mengubah frekuensi pembayaran dari bulanan menjadi tahunan.",
        "Nasabah meminta perubahan data pekerjaan karena ada perubahan karir.",
        "Nasabah ingin mengubah nama sesuai dengan dokumen identitas yang baru.",
        "Nasabah meminta perubahan data bank untuk keperluan pencairan klaim atau surrender.",
        "Nasabah ingin mengubah persentase alokasi dana pada produk unit link."
    ],
    'Permintaan Informasi': [
        "Nasabah meminta informasi detail mengenai produk asuransi baru yang diluncurkan perusahaan.",
        "Nasabah ingin mendapatkan informasi tentang cara mengoptimalkan manfaat dari polis yang dimiliki.",
        "Nasabah meminta penjelasan tentang perhitungan premi dan faktor-faktor yang mempengaruhinya.",
        "Nasabah ingin mendapatkan informasi tentang program loyalitas dan reward yang tersedia.",
        "Nasabah meminta informasi mengenai prosedur dan persyaratan untuk menambah sum insured.",
        "Nasabah ingin mendapatkan laporan investasi dan performance unit link secara berkala.",
        "Nasabah meminta informasi tentang coverage dan exclusion dari polis asuransi kesehatan.",
        "Nasabah ingin mendapatkan informasi tentang tax benefit dan insentif pajak dari asuransi.",
        "Nasabah meminta informasi mengenai network rumah sakit yang bekerja sama dengan perusahaan.",
        "Nasabah ingin mendapatkan informasi tentang cara melakukan top up pada polis unit link."
    ],
    'Pembatalan Polis': [
        "Nasabah mengajukan pembatalan polis karena kesulitan finansial dan tidak mampu membayar premi.",
        "Nasabah ingin membatalkan polis karena sudah mendapatkan coverage serupa dari perusahaan lain.",
        "Nasabah mengajukan pembatalan polis karena tidak puas dengan pelayanan yang diberikan.",
        "Nasabah ingin membatalkan polis karena ada perubahan kebutuhan asuransi yang signifikan.",
        "Nasabah mengajukan pembatalan polis karena produk tidak sesuai dengan ekspektasi awal.",
        "Nasabah ingin membatalkan polis dalam masa cooling off period setelah mempertimbangkan ulang.",
        "Nasabah mengajukan pembatalan polis karena agent tidak memberikan informasi yang akurat.",
        "Nasabah ingin membatalkan polis karena ada perubahan regulasi yang mempengaruhi manfaat.",
        "Nasabah mengajukan pembatalan polis karena ingin beralih ke produk yang lebih sesuai.",
        "Nasabah ingin membatalkan polis karena ada kendala dalam proses pembayaran premi."
    ],
    'Klaim': [
        "Nasabah mengajukan klaim asuransi kesehatan untuk biaya rawat inap di rumah sakit.",
        "Nasabah mengajukan klaim asuransi jiwa karena tertanggung mengalami kecelakaan fatal.",
        "Nasabah mengajukan klaim asuransi kendaraan karena mobil mengalami kerusakan akibat kecelakaan.",
        "Nasabah mengajukan klaim asuransi kesehatan untuk biaya operasi dan perawatan medis.",
        "Nasabah mengajukan klaim asuransi travel karena pembatalan perjalanan mendadak.",
        "Nasabah mengajukan klaim asuransi rumah karena kerusakan akibat bencana alam.",
        "Nasabah mengajukan klaim asuransi critical illness karena didiagnosis penyakit kritis.",
        "Nasabah mengajukan klaim asuransi kecelakaan diri karena mengalami cacat tetap.",
        "Nasabah mengajukan klaim asuransi pendidikan untuk biaya sekolah anak.",
        "Nasabah mengajukan klaim asuransi mikro karena kehilangan penghasilan akibat sakit."
    ],
    'Pembayaran': [
        "Nasabah menanyakan cara pembayaran premi yang tertunggak dan denda yang dikenakan.",
        "Nasabah melaporkan bahwa pembayaran premi via transfer sudah dilakukan namun belum ter-update di sistem.",
        "Nasabah ingin mengaktifkan auto debit untuk pembayaran premi bulanan agar lebih praktis.",
        "Nasabah menanyakan tentang grace period dan konsekuensi jika terlambat bayar premi.",
        "Nasabah melaporkan adanya double charge pada rekening bank untuk pembayaran premi.",
        "Nasabah ingin mengubah tanggal jatuh tempo pembayaran premi sesuai dengan jadwal gaji.",
        "Nasabah menanyakan tentang cara pembayaran premi dalam mata uang asing.",
        "Nasabah melaporkan bahwa auto debit gagal dan ingin mengetahui penyebabnya.",
        "Nasabah ingin mendapatkan konfirmasi pembayaran dan receipt untuk keperluan klaim pajak.",
        "Nasabah menanyakan tentang opsi pembayaran premi secara online melalui berbagai platform digital."
    ]
}

def generate_indonesian_description(interaction_type):
    """Generate contextual description in Indonesian based on interaction type"""
    if interaction_type in DESCRIPTION_TEMPLATES:
        return random.choice(DESCRIPTION_TEMPLATES[interaction_type])
    else:
        # Fallback untuk tipe interaksi yang tidak terdefined
        return "Nasabah menghubungi customer service untuk mendapatkan bantuan dan informasi mengenai produk asuransi."

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

def generate_customer_service(num_records=100000):
    """Generate customer service interaction data"""
    # Get existing customers, employees, and policies
    customers = get_existing_data('customer')
    employees = get_existing_data('employee')
    policies = get_existing_data('policy')
    
    if customers is None or employees is None:
        print("Error: Cannot generate customer service data without customer and employee data")
        return None
    
    # Filter only customer service employees
    cs_employees = employees[employees['department'] == 'Customer Service']
    
    if len(cs_employees) == 0:
        print("Error: No customer service employees found. Using all employees instead")
        cs_employees = employees
    
    customer_ids = customers['customer_id'].tolist()
    employee_ids = cs_employees['employee_id'].tolist()
    
    # Policies (if available)
    policy_ids = policies['policy_id'].tolist() if policies is not None else []
    
    services = []
    
    for _ in range(num_records):
        # Select random customer and employee
        customer_id = random.choice(customer_ids)
        employee_id = random.choice(employee_ids)
        
        # Interaction date (between 1 year ago and today)
        interaction_date = fake.date_time_between(start_date='-1y', end_date='now')
        
        # Randomly decide if interaction is related to a policy (60% chance)
        policy_related = random.random() < 0.6 and len(policy_ids) > 0
        policy_id = random.choice(policy_ids) if policy_related else None
        
        # Generate interaction type
        interaction_type = random.choice(INTERACTION_TYPES)
        
        # If it's a complaint, add complaint category
        is_complaint = interaction_type == 'Keluhan'
        complaint_category = random.choice(COMPLAINT_CATEGORIES) if is_complaint else None
        
        # Determine duration based on interaction type and complexity
        if interaction_type in ['Keluhan', 'Klaim']:
            duration_minutes = random.randint(10, 60)  # More complex issues
        else:
            duration_minutes = random.randint(3, 30)  # Simpler issues
        
        # Determine status based on date
        today = datetime.now()
        
        # Recent interactions (within last 3 days) might still be open
        if (today - interaction_date).days < 3:
            status_weights = [0.3, 0.4, 0.1, 0.1, 0.1]  # More likely to be open or in progress
        else:
            status_weights = [0.05, 0.1, 0.7, 0.1, 0.05]  # More likely to be closed
            
        status = random.choices(SERVICE_STATUS, weights=status_weights, k=1)[0]
        
        # Resolution time (None if still open)
        if status == 'Closed':
            # Resolution time between 1 hour and 5 days after interaction
            min_hours = 1
            max_hours = 24 * 5  # 5 days in hours
            resolution_hours = random.randint(min_hours, max_hours)
            resolution_time = interaction_date + timedelta(hours=resolution_hours)
        else:
            resolution_time = None
            
        # Customer satisfaction (only if closed)
        satisfaction = random.randint(1, 5) if status == 'Closed' else None
        
        service = {
            'service_id': fake.unique.bothify(text='SVC-######'),
            'customer_id': customer_id,
            'employee_id': employee_id,
            'policy_id': policy_id,
            'interaction_date': interaction_date,
            'interaction_type': interaction_type,
            'channel': random.choice(SERVICE_CHANNELS),
            'complaint_category': complaint_category,
            'description': generate_indonesian_description(interaction_type),
            'duration_minutes': duration_minutes,
            'status': status,
            'resolution_time': resolution_time,
            'satisfaction_rating': satisfaction,
            'follow_up_required': random.random() < 0.3  # 30% chance of requiring follow-up
        }
        
        services.append(service)
    
    df = pd.DataFrame(services)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate customer service data
    service_df = generate_customer_service()
    
    if service_df is not None:
        # Save to CSV as backup
        output_path = os.path.join(data_dir, 'customer_service.csv')
        service_df.to_csv(output_path, index=False)
        print(f"Generated {len(service_df)} customer service records saved to {output_path}")
        
        # Create database if not exists and save to PostgreSQL
        try:
            create_database_if_not_exists()
            save_to_db(service_df, 'customer_service')
            print("Data successfully saved to PostgreSQL database")
        except Exception as e:
            print(f"Error saving to PostgreSQL: {str(e)}")
            print("Data was saved to CSV only")
    else:
        print("Failed to generate customer service data")