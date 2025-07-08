# Star Schema dan Enterprise Bus Matrix

## Enterprise Bus Matrix (Update)

| Dimension Tables        | FACT_REGISTRATION | FACT_CLAIM | FACT_SALES_ACTIVITY | FACT_INVESTMENT | FACT_CUSTOMER_SERVICE |
|------------------------|-------------------|------------|---------------------|-----------------|----------------------|
| DIM_CUSTOMER           | ✓                 | ✓          | ✓                  | ✓               | ✓                    |
| DIM_PRODUCT            | ✓                 |            | ✓                  |                 |                      |
| DIM_EMPLOYEE           |                   |            | ✓                  |                 | ✓                    |
| DIM_TIME               | ✓                 | ✓          | ✓                  | ✓               | ✓                    |
| DIM_POLICY             | ✓                 | ✓          |                    | ✓               | ✓                    |
| DIM_MARKETING_PROGRAM  |                   |            | ✓                  |                 |                      |

## Star Schema Detail (Kolom sesuai SQL)

### 1. FACT_REGISTRATION
- registration_key (PK)
- customer_key (FK)
- product_key (FK)
- time_key (FK)
- policy_key (FK)
- premium_amount
- sum_assured
- policy_term
- referral_flag
- batch_id
- created_at

### 2. FACT_CLAIM
- claim_key (PK)
- claim_id
- policy_key (FK)
- customer_key (FK)
- time_key_submitted (FK)
- time_key_decision (FK)
- claim_type
- claim_amount
- approved_amount
- status
- processing_days
- rejection_flag
- paid_flag
- batch_id
- created_at

### 3. FACT_SALES_ACTIVITY
- activity_key (PK)
- activity_id
- employee_key (FK)
- customer_key (FK)
- product_key (FK)
- program_key (FK, nullable)
- time_key (FK)
- activity_type
- activity_channel
- duration_minutes
- status
- resulted_in_sale
- follow_up_scheduled
- batch_id
- snapshot_date
- created_at

### 4. FACT_INVESTMENT
- investment_key (PK)
- investment_id
- policy_key (FK)
- customer_key (FK)
- time_key_investment (FK)
- investment_type
- amount
- return_percentage
- days_since_investment
- investment_status
- batch_id
- created_at
- updated_at

### 5. FACT_CUSTOMER_SERVICE
- service_key (PK)
- service_id
- customer_key (FK)
- employee_key (FK)
- policy_key (FK, nullable)
- time_key_interaction (FK)
- time_key_resolution (FK, nullable)
- interaction_type
- channel
- complaint_category
- duration_minutes
- status
- satisfaction_rating
- follow_up_required
- resolution_time_hours
- batch_id
- created_at

## Measures (Metrics) per Fact Table

### FACT_REGISTRATION
- Jumlah pendaftaran baru
- Total premium
- Total nilai polis
- Rata-rata premium per produk
- Konversi dari referral

### FACT_CLAIM
- Jumlah klaim
- Total nilai klaim
- Rata-rata waktu pemrosesan
- Rasio klaim per polis
- Distribusi status klaim

### FACT_SALES_ACTIVITY
- Total aktivitas sales
- Tingkat konversi
- Revenue per sales
- Efektivitas program marketing
- Produktivitas sales

### FACT_INVESTMENT
- Total investasi
- Total return
- ROI
- Performa per jenis investasi
- Distribusi sumber dana

### FACT_CUSTOMER_SERVICE
- Jumlah keluhan
- Rata-rata waktu resolusi
- Tingkat kepuasan
- Distribusi kategori keluhan
- Performa layanan per channel