-- Dimension table for Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    nama VARCHAR(255),
    tanggal_lahir DATE,
    alamat TEXT,
    referral_code VARCHAR(50),
    tanggal_daftar DATE,
    umur INT,  -- Calculated field
    lama_menjadi_pelanggan INT, -- Calculated field in days
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX IF NOT EXISTS idx_customer_id ON dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_current ON dim_customer(is_current);