from faker import Faker
import pandas as pd
import random
import os
from db_config import create_database_if_not_exists, save_to_db

fake = Faker('id_ID')

# Define product types and their characteristics
PRODUCT_TYPES = [
    'Asuransi Jiwa',
    'Asuransi Kesehatan',
    'Asuransi Pendidikan',
    'Asuransi Properti',
    'Asuransi Kendaraan'
]

PRODUCT_BENEFITS = {
    'Asuransi Jiwa': ['Santunan Meninggal', 'Nilai Tunai', 'Bonus Akhir Kontrak'],
    'Asuransi Kesehatan': ['Rawat Inap', 'Operasi', 'Perawatan Gigi', 'Melahirkan'],
    'Asuransi Pendidikan': ['Dana Pendidikan', 'Beasiswa', 'Proteksi Pendidikan'],
    'Asuransi Properti': ['Kebakaran', 'Bencana Alam', 'Pencurian'],
    'Asuransi Kendaraan': ['All Risk', 'Total Loss Only', 'Third Party']
}

def generate_products(num_records=50):
    products = []
    
    for _ in range(num_records):
        jenis = random.choice(PRODUCT_TYPES)
        benefits = random.sample(PRODUCT_BENEFITS[jenis], k=random.randint(1, len(PRODUCT_BENEFITS[jenis])))
        
        product = {
            'product_id': fake.unique.random_number(digits=6),
            'nama_product': f"{jenis} {fake.word().title()}",
            'jenis_product': jenis,
            'manfaat': ', '.join(benefits),
            'premi_dasar': random.randint(100000, 5000000)
        }
        products.append(product)
    
    df = pd.DataFrame(products)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate product data
    product_df = generate_products()
    
    # Save to CSV as backup
    output_path = os.path.join(data_dir, 'product.csv')
    product_df.to_csv(output_path, index=False)
    print(f"Generated {len(product_df)} product records saved to {output_path}")
    
    # Create database if not exists and save to PostgreSQL
    try:
        create_database_if_not_exists()
        save_to_db(product_df, 'product')
        print("Data successfully saved to PostgreSQL database")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {str(e)}")
        print("Data was saved to CSV only")