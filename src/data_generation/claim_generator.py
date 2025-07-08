from faker import Faker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys
from db_config import create_database_if_not_exists, save_to_db, get_db_engine

fake = Faker('id_ID')

# Status klaim
CLAIM_STATUS = ['Submitted', 'Under Review', 'Approved', 'Rejected', 'Paid']

# Jenis klaim berdasarkan jenis produk
CLAIM_TYPES = {
    'Asuransi Jiwa': ['Death Benefit', 'Terminal Illness', 'Critical Illness', 'Disability'],
    'Asuransi Kesehatan': ['Medical Reimbursement', 'Surgery', 'Hospital Cash', 'Outpatient Treatment'],
    'Asuransi Pendidikan': ['Education Fund', 'Scholarship', 'Education Protection'],
    'Asuransi Properti': ['Fire Damage', 'Natural Disaster', 'Theft', 'Property Damage'],
    'Asuransi Kendaraan': ['Accident', 'Theft', 'Third Party Liability', 'Total Loss']
}

# Detailed claim descriptions for each claim type
CLAIM_DESCRIPTIONS = {
    'Death Benefit': [
        'Klaim manfaat meninggal dunia karena kecelakaan lalu lintas',
        'Klaim santunan kematian akibat penyakit jantung',
        'Manfaat meninggal dunia karena stroke mendadak',
        'Klaim death benefit akibat kanker stadium lanjut',
        'Santunan kematian karena kecelakaan kerja'
    ],
    'Terminal Illness': [
        'Klaim penyakit terminal kanker paru-paru stadium 4',
        'Diagnosis penyakit terminal gagal ginjal kronis',
        'Klaim terminal illness akibat kanker hati',
        'Penyakit terminal stroke berulang dengan komplikasi',
        'Diagnosis terminal heart failure dengan prognosis buruk'
    ],
    'Critical Illness': [
        'Klaim penyakit kritis serangan jantung pertama',
        'Diagnosis kanker payudara stadium 2 dengan kemoterapi',
        'Klaim critical illness stroke dengan kelumpuhan parsial',
        'Penyakit kritis gagal ginjal memerlukan cuci darah',
        'Diagnosis kanker usus besar dengan operasi mayor'
    ],
    'Disability': [
        'Klaim cacat tetap total akibat kecelakaan motor',
        'Disability claim kehilangan penglihatan permanen',
        'Cacat tetap sebagian kehilangan fungsi tangan kanan',
        'Klaim disability akibat amputasi kaki karena diabetes',
        'Cacat tetap total akibat kecelakaan kerja konstruksi'
    ],
    'Medical Reimbursement': [
        'Klaim reimbursement rawat inap RS selama 5 hari',
        'Penggantian biaya medical check-up tahunan',
        'Klaim reimbursement biaya pengobatan diabetes',
        'Penggantian biaya rawat jalan spesialis jantung',
        'Klaim medical reimbursement fisioterapi pasca operasi'
    ],
    'Surgery': [
        'Klaim operasi caesar dengan komplikasi',
        'Biaya operasi pengangkatan batu ginjal',
        'Klaim operasi jantung bypass',
        'Operasi pengangkatan tumor jinak',
        'Klaim biaya operasi hernia inguinalis'
    ],
    'Hospital Cash': [
        'Klaim santunan harian rawat inap 7 hari',
        'Hospital cash benefit rawat inap ICU 3 hari',
        'Santunan harian rawat inap karena pneumonia',
        'Klaim hospital cash rawat inap bedah',
        'Santunan harian rawat inap karena demam berdarah'
    ],
    'Outpatient Treatment': [
        'Klaim rawat jalan konsultasi spesialis mata',
        'Biaya pengobatan rawat jalan hipertensi',
        'Klaim outpatient treatment cek lab rutin',
        'Rawat jalan konsultasi spesialis kulit',
        'Klaim biaya pengobatan rawat jalan asma'
    ],
    'Education Fund': [
        'Klaim dana pendidikan untuk biaya kuliah S1',
        'Pencairan dana pendidikan tingkat SMA',
        'Klaim education fund untuk biaya sekolah dasar',
        'Dana pendidikan untuk biaya kuliah kedokteran',
        'Klaim dana pendidikan untuk biaya kursus bahasa'
    ],
    'Scholarship': [
        'Klaim beasiswa prestasi akademik semester 1',
        'Beasiswa untuk siswa berprestasi tingkat SMA',
        'Klaim scholarship untuk kuliah di luar negeri',
        'Beasiswa prestasi olahraga tingkat nasional',
        'Klaim beasiswa untuk program magister'
    ],
    'Education Protection': [
        'Klaim proteksi pendidikan karena orang tua meninggal',
        'Education protection akibat PHK orang tua',
        'Klaim proteksi pendidikan karena cacat tetap ayah',
        'Education protection akibat sakit kritis orang tua',
        'Klaim proteksi pendidikan karena kebangkrutan usaha'
    ],
    'Fire Damage': [
        'Klaim kerusakan rumah akibat kebakaran kompor gas',
        'Kerugian kebakaran rumah karena korsleting listrik',
        'Klaim fire damage akibat kebakaran dari tetangga',
        'Kerusakan properti akibat kebakaran hutan',
        'Klaim kebakaran rumah karena petir'
    ],
    'Natural Disaster': [
        'Klaim kerusakan rumah akibat banjir bandang',
        'Kerugian properti akibat gempa bumi 6.2 SR',
        'Klaim natural disaster akibat angin puting beliung',
        'Kerusakan rumah akibat tanah longsor',
        'Klaim bencana alam akibat tsunami'
    ],
    'Theft': [
        'Klaim pencurian barang elektronik dari rumah',
        'Kerugian akibat pencurian motor dari garasi',
        'Klaim theft pencurian perhiasan dari brankas',
        'Pencurian laptop dan gadget dari kantor',
        'Klaim pencurian dengan pemberatan (curas)'
    ],
    'Property Damage': [
        'Klaim kerusakan properti akibat bocor atap',
        'Kerugian kerusakan dinding karena retak struktur',
        'Klaim property damage akibat banjir rob',
        'Kerusakan properti karena pohon tumbang',
        'Klaim kerusakan akibat ledakan tabung gas tetangga'
    ],
    'Accident': [
        'Klaim kecelakaan mobil tabrak lari di tol',
        'Kecelakaan motor single accident di tikungan',
        'Klaim accident tabrakan beruntun 3 kendaraan',
        'Kecelakaan mobil vs truk di jalan raya',
        'Klaim kecelakaan parkir menabrak pagar'
    ],
    'Third Party Liability': [
        'Klaim tanggung jawab pihak ketiga cedera pejalan kaki',
        'TPL claim kerusakan properti akibat menabrak rumah',
        'Klaim liability menabrak motor hingga patah kaki',
        'Third party liability merusak fasilitas umum',
        'Klaim TPL menabrak mobil mewah di mall'
    ],
    'Total Loss': [
        'Klaim total loss mobil terbakar habis',
        'Total loss kendaraan akibat banjir besar',
        'Klaim TLO mobil masuk jurang di pegunungan',
        'Total loss akibat kecelakaan parah tidak bisa diperbaiki',
        'Klaim TLO kendaraan dicuri dan tidak ditemukan'
    ]
}

def generate_claim_description(claim_type):
    """Generate realistic claim description based on claim type"""
    if claim_type in CLAIM_DESCRIPTIONS:
        base_description = random.choice(CLAIM_DESCRIPTIONS[claim_type])
        
        # Add some additional details randomly
        additional_details = [
            f"Nomor laporan polisi: {fake.bothify(text='LP/###/??/####/????')}",
            f"Lokasi kejadian: {fake.city()}",
            f"Tanggal kejadian: {fake.date_this_year()}",
            f"Dokumen pendukung: {random.choice(['Surat keterangan dokter', 'Surat keterangan kepolisian', 'Kwitansi rumah sakit', 'Laporan survei', 'Surat keterangan ahli waris'])}",
            f"Saksi: {fake.name()}",
            f"Estimasi kerugian: Rp {random.randint(1000000, 50000000):,}",
            f"No. rekam medis: {fake.bothify(text='RM-######')}",
            f"Rumah sakit: {fake.company()} Hospital"
        ]
        
        # Randomly add 1-3 additional details
        num_details = random.randint(1, 3)
        selected_details = random.sample(additional_details, num_details)
        
        full_description = base_description + ". " + ". ".join(selected_details) + "."
        
        return full_description
    else:
        # Fallback for unknown claim types
        return f"Klaim {claim_type.lower()} dengan detail yang sedang dalam proses verifikasi oleh tim underwriting."

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

def generate_claims(num_records=100000):
    """Generate claim data with references to policies"""
    # Get existing policies and products
    policies = get_existing_data('policy')
    products = get_existing_data('product')
    
    if policies is None or products is None:
        print("Error: Cannot generate claims without policy and product data")
        return None
    
    # Filter only active and matured policies for claims
    valid_policies = policies[policies['status'].isin(['Active', 'Matured'])]
    
    if len(valid_policies) == 0:
        print("Error: No valid policies found for generating claims")
        return None
    
    claims = []
    
    # Create a map of product_id to jenis_product for easier lookup
    product_types = {row['product_id']: row['jenis_product'] for _, row in products.iterrows()}
    
    for _ in range(num_records):
        # Select a random policy
        policy_row = valid_policies.sample(1).iloc[0]
        policy_id = policy_row['policy_id']
        product_id = policy_row['product_id']
        
        # Get product type
        product_type = product_types.get(product_id, 'Asuransi Jiwa')  # Default to Asuransi Jiwa if not found
        
        # Get claim type based on product type
        claim_type = random.choice(CLAIM_TYPES.get(product_type, CLAIM_TYPES['Asuransi Jiwa']))
        
        # Generate claim submission date (must be after policy start date)
        policy_start = pd.to_datetime(policy_row['tanggal_mulai'])
        policy_end = pd.to_datetime(policy_row['tanggal_berakhir'])
        
        # Claim date should be between policy start and today or policy end (whichever is earlier)
        max_claim_date = min(datetime.now().date(), policy_end.date())
        
        # Ensure the start date is not in the future
        if policy_start.date() > datetime.now().date():
            continue  # Skip this iteration if policy starts in the future
            
        claim_date = fake.date_between(start_date=policy_start, end_date=max_claim_date)
        
        # Generate processing time (1-30 days)
        processing_days = random.randint(1, 30)
        decision_date = datetime.combine(claim_date, datetime.min.time()) + timedelta(days=processing_days)
        
        # Determine status based on decision date
        if decision_date.date() > datetime.now().date():
            # If decision date is in the future, claim is still being processed
            status = random.choice(['Submitted', 'Under Review'])
            decision_date = None
        else:
            # Decision has been made
            # Weights for all statuses in CLAIM_STATUS: Submitted, Under Review, Approved, Rejected, Paid
            status_weights = [0.1, 0.2, 0.4, 0.1, 0.2]  # Adjusted weights to match all 5 statuses
            status = random.choices(CLAIM_STATUS, weights=status_weights, k=1)[0]
            
            # If approved, add payment date
            if status == 'Approved':
                payment_delay = random.randint(1, 7)  # 1-7 days to pay after approval
                payment_date = decision_date + timedelta(days=payment_delay)
                if payment_date.date() <= datetime.now().date():
                    status = 'Paid'
        
        # Generate claim amount (random percentage of sum_assured)
        sum_assured = policy_row['sum_assured']
        claim_percentage = random.uniform(0.05, 1.0)  # 5% to 100% of sum assured
        claim_amount = int(sum_assured * claim_percentage)
        
        # For rejected claims, approved amount is 0
        approved_amount = 0 if status == 'Rejected' else claim_amount
        
        # Generate realistic claim description
        claim_description = generate_claim_description(claim_type)
        
        claim = {
            'claim_id': fake.unique.bothify(text='CLM-######'),
            'policy_id': policy_id,
            'claim_date': claim_date,
            'claim_type': claim_type,
            'claim_description': claim_description,
            'claim_amount': claim_amount,
            'status': status,
            'decision_date': decision_date.date() if decision_date else None,
            'approved_amount': approved_amount if status in ['Approved', 'Paid'] else None,
            'rejection_reason': fake.sentence() if status == 'Rejected' else None
        }
        
        claims.append(claim)
    
    df = pd.DataFrame(claims)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate claim data
    claim_df = generate_claims()
    
    if claim_df is not None:
        # Save to CSV as backup
        output_path = os.path.join(data_dir, 'claim.csv')
        claim_df.to_csv(output_path, index=False)
        print(f"Generated {len(claim_df)} claim records saved to {output_path}")
        
        # Create database if not exists and save to PostgreSQL
        try:
            create_database_if_not_exists()
            save_to_db(claim_df, 'claim')
            print("Data successfully saved to PostgreSQL database")
        except Exception as e:
            print(f"Error saving to PostgreSQL: {str(e)}")
            print("Data was saved to CSV only")
    else:
        print("Failed to generate claim data")