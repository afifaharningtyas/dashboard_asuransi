# Star Schema dan Enterprise Bus Matrix

## Enterprise Bus Matrix

| Dimension Tables    | FACT_REGISTRATION | FACT_CLAIM | FACT_SALES_ACTIVITY | FACT_INVESTMENT | FACT_CUSTOMER_SERVICE |
|--------------------|-------------------|------------|-------------------|----------------|---------------------|
| DIM_CUSTOMER       | ✓                | ✓          | ✓                 |                | ✓                   |
| DIM_PRODUCT        | ✓                |            | ✓                 |                |                     |
| DIM_EMPLOYEE       |                  |            | ✓                 |                | ✓                   |
| DIM_TIME          | ✓                | ✓          | ✓                 | ✓              | ✓                   |
| DIM_LOCATION      | ✓                | ✓          |                   |                |                     |
| DIM_POLICY        |                  | ✓          |                   |                |                     |
| DIM_CLAIM_STATUS  |                  | ✓          |                   |                |                     |
| DIM_MARKETING_PROGRAM |               |            | ✓                 |                |                     |
| DIM_INVESTMENT_TYPE |                 |            |                   | ✓              |                     |
| DIM_FUND_SOURCE   |                  |            |                   | ✓              |                     |
| DIM_CHANNEL       |                  |            |                   |                | ✓                   |
| DIM_COMPLAINT_CATEGORY |              |            |                   |                | ✓                   |

## Star Schema Detail

### 1. FACT_REGISTRATION
```mermaid
graph TD
    FR[FACT_REGISTRATION] --> DC[DIM_CUSTOMER]
    FR --> DP[DIM_PRODUCT]
    FR --> DT[DIM_TIME]
    FR --> DL[DIM_LOCATION]

    FR[FACT_REGISTRATION<br/>- registration_key PK<br/>- customer_key FK<br/>- product_key FK<br/>- time_key FK<br/>- location_key FK<br/>- premium_amount<br/>- policy_value<br/>- referral_source]
```

### 2. FACT_CLAIM
```mermaid
graph TD
    FC[FACT_CLAIM] --> DP[DIM_POLICY]
    FC --> DC[DIM_CUSTOMER]
    FC --> DT[DIM_TIME]
    FC --> DL[DIM_LOCATION]
    FC --> DS[DIM_CLAIM_STATUS]

    FC[FACT_CLAIM<br/>- claim_key PK<br/>- policy_key FK<br/>- customer_key FK<br/>- time_key FK<br/>- location_key FK<br/>- status_key FK<br/>- claim_amount<br/>- processing_time]
```

### 3. FACT_SALES_ACTIVITY
```mermaid
graph TD
    FS[FACT_SALES_ACTIVITY] --> DE[DIM_EMPLOYEE]
    FS --> DC[DIM_CUSTOMER]
    FS --> DP[DIM_PRODUCT]
    FS --> DM[DIM_MARKETING_PROGRAM]
    FS --> DT[DIM_TIME]

    FS[FACT_SALES_ACTIVITY<br/>- activity_key PK<br/>- employee_key FK<br/>- customer_key FK<br/>- product_key FK<br/>- program_key FK<br/>- time_key FK<br/>- conversion_status<br/>- sales_amount]
```

### 4. FACT_INVESTMENT
```mermaid
graph TD
    FI[FACT_INVESTMENT] --> DIT[DIM_INVESTMENT_TYPE]
    FI --> DT[DIM_TIME]
    FI --> DFS[DIM_FUND_SOURCE]

    FI[FACT_INVESTMENT<br/>- investment_key PK<br/>- type_key FK<br/>- time_key FK<br/>- source_key FK<br/>- investment_amount<br/>- return_amount<br/>- roi_percentage]
```

### 5. FACT_CUSTOMER_SERVICE
```mermaid
graph TD
    FCS[FACT_CUSTOMER_SERVICE] --> DC[DIM_CUSTOMER]
    FCS --> DE[DIM_EMPLOYEE]
    FCS --> DT[DIM_TIME]
    FCS --> DCH[DIM_CHANNEL]
    FCS --> DCC[DIM_COMPLAINT_CATEGORY]

    FCS[FACT_CUSTOMER_SERVICE<br/>- service_key PK<br/>- customer_key FK<br/>- employee_key FK<br/>- time_key FK<br/>- channel_key FK<br/>- category_key FK<br/>- resolution_time<br/>- satisfaction_score]
```

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