from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
import random
import os
from db_config import create_database_if_not_exists, save_to_db

fake = Faker('id_ID')

PROGRAM_TYPES = [
    'Digital Campaign',
    'Direct Selling',
    'Event Marketing',
    'Referral Program',
    'Corporate Partnership'
]

JENIS_PROGRAM = [
    'Online',
    'Offline',
    'Hybrid',
    'Digital',
    'Traditional'
]

DESCRIPTION_TEMPLATES = [
    # Digital Campaign descriptions
    "Program pemasaran digital yang memanfaatkan platform media sosial untuk meningkatkan brand awareness dan engagement dengan target audience. Strategi konten yang digunakan meliputi video, infografis, dan interactive content.",
    "Kampanye digital terintegrasi yang menggunakan multi-channel approach termasuk Google Ads, Facebook Ads, dan email marketing untuk mencapai target penjualan yang telah ditetapkan.",
    "Program digital marketing yang berfokus pada content marketing dan SEO optimization untuk meningkatkan organic traffic dan conversion rate website perusahaan.",
    "Strategi pemasaran digital yang mengintegrasikan social media marketing, influencer collaboration, dan paid advertising untuk mencapai target market yang lebih luas.",
    
    # Direct Selling descriptions
    "Program penjualan langsung yang melibatkan tim sales untuk melakukan pendekatan personal kepada potential customers melalui door-to-door sales dan direct presentation.",
    "Strategi direct selling yang menggunakan pendekatan konsultatif untuk membangun relationship yang kuat dengan klien dan meningkatkan customer loyalty.",
    "Program penjualan langsung yang berfokus pada product demonstration dan personal consultation untuk memberikan pengalaman yang lebih personal kepada customer.",
    "Inisiatif direct selling yang memanfaatkan networking dan referral system untuk memperluas jangkauan pasar dan meningkatkan sales volume.",
    
    # Event Marketing descriptions
    "Program event marketing yang mengorganisir berbagai acara promosi seperti product launching, exhibition, dan customer gathering untuk meningkatkan brand visibility.",
    "Strategi event marketing yang meliputi penyelenggaraan workshop, seminar, dan training session untuk mengedukasi customer tentang produk dan layanan perusahaan.",
    "Program event marketing yang mengintegrasikan offline dan online events untuk menciptakan experience yang memorable bagi customer dan meningkatkan brand engagement.",
    "Inisiatif event marketing yang berfokus pada community building melalui penyelenggaraan regular meetup, networking event, dan customer appreciation program.",
    
    # Referral Program descriptions
    "Program referral yang memberikan insentif kepada existing customer untuk merekomendasikan produk atau layanan kepada teman dan keluarga mereka.",
    "Strategi referral marketing yang menggunakan reward system untuk memotivasi customer menjadi brand ambassador dan memperluas customer base melalui word-of-mouth marketing.",
    "Program referral yang memanfaatkan digital platform untuk memudahkan customer dalam sharing referral code dan tracking reward yang mereka dapatkan.",
    "Inisiatif referral program yang mengintegrasikan gamification elements untuk membuat proses referral menjadi lebih engaging dan menyenangkan bagi customer.",
    
    # Corporate Partnership descriptions
    "Program kemitraan strategis dengan perusahaan lain untuk menciptakan mutual benefit dan memperluas market reach melalui cross-promotion dan joint marketing activities.",
    "Strategi corporate partnership yang melibatkan kolaborasi dengan vendor, supplier, dan business partner untuk menciptakan integrated marketing solution.",
    "Program partnership yang berfokus pada strategic alliance dengan perusahaan yang memiliki target market yang komplementer untuk mencapai win-win solution.",
    "Inisiatif corporate partnership yang mengintegrasikan co-branding dan joint venture untuk menciptakan value proposition yang lebih kuat di pasar.",
    
    # General marketing descriptions
    "Program marketing terintegrasi yang menggabungkan various marketing channels untuk mencapai maximum impact dan ROI yang optimal.",
    "Strategi marketing yang berfokus pada customer journey optimization untuk meningkatkan conversion rate di setiap touchpoint customer.",
    "Program marketing yang menggunakan data-driven approach untuk mengoptimalkan marketing spend dan meningkatkan effectiveness kampanye.",
    "Inisiatif marketing yang memanfaatkan marketing automation tools untuk meningkatkan efficiency dan personalization dalam customer communication.",
    "Program marketing yang berfokus pada customer retention dan loyalty building melalui personalized marketing approach dan excellent customer service.",
    "Strategi marketing yang mengintegrasikan traditional dan digital marketing channels untuk mencapai broader audience dan maximize brand exposure."
]

def generate_indonesian_description():
    """Generate contextual description in Indonesian"""
    return random.choice(DESCRIPTION_TEMPLATES)

def generate_marketing_programs(num_records=30):
    programs = []
    
    for _ in range(num_records):
        start_date = fake.date_between(start_date='-1y', end_date='+6m')
        duration = random.randint(30, 180)  # Program duration between 1-6 months
        end_date = start_date + timedelta(days=duration)
        
        # Generate budget based on program type and duration
        base_budget = random.randint(10, 100) * 1000000  # Base budget in millions
        duration_factor = duration / 30  # Monthly multiplier
        budget = int(base_budget * duration_factor)
        
        program = {
            'program_id': fake.unique.random_number(digits=6),
            'nama_program': f"{random.choice(PROGRAM_TYPES)} - {fake.word().title()}",
            'start_date': start_date,
            'end_date': end_date,
            'target': random.randint(50, 500) * 1000000,  # Target in millions
            'description': generate_indonesian_description(),#fake.text(max_nb_chars=200),
            'jenis_program': random.choice(JENIS_PROGRAM),
            'budget': budget
        }
        programs.append(program)
    
    df = pd.DataFrame(programs)
    return df

if __name__ == "__main__":
    # Get the absolute path to the project root directory
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
    
    # Create data/raw directory for CSV backup
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate marketing program data
    program_df = generate_marketing_programs()
    
    # Save to CSV as backup
    output_path = os.path.join(data_dir, 'marketing_program.csv')
    program_df.to_csv(output_path, index=False)
    print(f"Generated {len(program_df)} marketing program records saved to {output_path}")
    
    # Create database if not exists and save to PostgreSQL
    try:
        create_database_if_not_exists()
        save_to_db(program_df, 'marketing_program')
        print("Data successfully saved to PostgreSQL database")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {str(e)}")
        print("Data was saved to CSV only")