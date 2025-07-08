# Project Data Warehouse Asuransi - Checklist

## 1. Dokumentasi Data Warehouse
- [x] ERD (Entity Relationship Diagram)
- [x] Identifikasi Proses Bisnis dan Jenisnya
- [x] Fact Tables dan Dimension Tables
- [ ] Enterprise Bus Matrix
- [x] Star Schema untuk setiap proses bisnis
- [x] Dokumentasi proses ETL

## 2. Setup Infrastructure
- [x] Install PostgreSQL
- [x] Setup database lokal
- [x] Install Apache Airflow
- [x] Konfigurasi Airflow dengan PostgreSQL

## 3. Data Dummy Generation
### 3.1 Master Data
- [x] Generate data Customer
- [x] Generate data Product
- [x] Generate data Employee
- [x] Generate data Marketing_Program

### 3.2 Transactional Data
- [x] Generate data Policy
- [x] Generate data Claim
- [x] Generate data Sales_Activity
- [x] Generate data Investment
- [x] Generate data Investment_Return
- [x] Generate data Customer_Service

## 4. ETL Pipeline Development
### 4.1 Extract
- [x] Create SQL queries for source tables
- [x] Setup connection to source database
- [x] Create extraction DAGs in Airflow

### 4.2 Transform
- [x] Develop transformation logic for Facts
- [x] Develop transformation logic for Dimensions
- [x] Create data quality checks
- [x] Setup error handling

### 4.3 Load
- [x] Create target tables in Data Warehouse
- [x] Develop loading procedures
- [x] Setup incremental loading
- [ ] Implement loading validation

## 5. Testing
- [ ] Unit testing untuk transformasi
- [ ] Integration testing
- [ ] End-to-end pipeline testing
- [ ] Data quality validation

## Detail Proses Bisnis yang Sudah Diimplementasikan

### 1. Proses Pendaftaran Asuransi
**Jenis**: Transaction Processing
- [x] Fact Table: FACT_REGISTRATION
- Dimensions:
  - [x] DIM_CUSTOMER (SCD Type 2)
  - [x] DIM_PRODUCT
  - [x] DIM_TIME
  - [x] DIM_POLICY

### 2. Proses Klaim
**Jenis**: Transaction Processing
- [x] Fact Table: FACT_CLAIM
- Dimensions:
  - [x] DIM_POLICY
  - [x] DIM_CUSTOMER
  - [x] DIM_TIME

### 3. Proses Sales dan Marketing
**Jenis**: Periodic Snapshot
- [x] Fact Table: FACT_SALES_ACTIVITY
- Dimensions:
  - [x] DIM_EMPLOYEE
  - [x] DIM_CUSTOMER
  - [x] DIM_PRODUCT
  - [x] DIM_MARKETING_PROGRAM
  - [x] DIM_TIME

### 4. Proses Investasi
**Jenis**: Accumulating Snapshot
- [x] Fact Table: FACT_INVESTMENT
- Dimensions:
  - [x] DIM_POLICY
  - [x] DIM_CUSTOMER
  - [x] DIM_TIME

### 5. Proses Customer Service
**Jenis**: Transaction Processing
- [x] Fact Table: FACT_CUSTOMER_SERVICE
- Dimensions:
  - [x] DIM_CUSTOMER
  - [x] DIM_EMPLOYEE
  - [x] DIM_POLICY
  - [x] DIM_TIME

## DAGs yang Sudah Dibuat
1. **init_data_warehouse**: Inisialisasi data warehouse dan pembuatan tabel dimensi dan fakta
2. **etl_dim_customer**: ETL untuk dimensi customer (SCD Type 2)
3. **etl_fact_registration**: ETL untuk fakta registrasi polis asuransi

## DAGs yang Perlu Dibuat Selanjutnya
1. ETL untuk dimensi product
2. ETL untuk dimensi employee
3. ETL untuk dimensi policy
4. ETL untuk dimensi marketing_program
5. ETL untuk fakta claim
6. ETL untuk fakta sales_activity
7. ETL untuk fakta investment
8. ETL untuk fakta customer_service
9. Master DAG untuk mengorkestrasi seluruh ETL pipeline

## Tool yang Digunakan
1. **Database**: PostgreSQL
2. **ETL Orchestration**: Apache Airflow
3. **Data Generation**: Python (Faker library)
4. **Version Control**: Git
5. **Development Environment**: VSCode

## Langkah Berikutnya
1. Implementasi DAG untuk dimensi dan fakta yang tersisa
2. Membuat master DAG untuk mengorkestrasi seluruh ETL pipeline
3. Melakukan testing end-to-end
4. Validasi data quality