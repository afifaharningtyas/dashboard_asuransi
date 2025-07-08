-- Dimension table for Policy (Updated with missing columns)
CREATE TABLE IF NOT EXISTS dim_policy (
    policy_key SERIAL PRIMARY KEY,
    policy_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20),  -- Added: Original customer ID from source
    product_id VARCHAR(20),   -- Added: Original product ID from source
    customer_key INT,
    product_key INT,
    tanggal_mulai DATE,
    tanggal_berakhir DATE,
    status VARCHAR(20),
    status_polis VARCHAR(20), -- Added: Policy status (Active, Expired, Future)
    premium NUMERIC(12, 2),
    payment_frequency VARCHAR(20),
    payment_method VARCHAR(30),
    sum_assured NUMERIC(15, 2),
    durasi_polis INT, -- Calculated field in days (not years based on your ETL)
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
);

-- Index for lookups
CREATE INDEX IF NOT EXISTS idx_policy_id ON dim_policy(policy_id);
CREATE INDEX IF NOT EXISTS idx_policy_current ON dim_policy(is_current);
CREATE INDEX IF NOT EXISTS idx_policy_customer ON dim_policy(customer_key);
CREATE INDEX IF NOT EXISTS idx_policy_product ON dim_policy(product_key);
CREATE INDEX IF NOT EXISTS idx_policy_customer_id ON dim_policy(customer_id);
CREATE INDEX IF NOT EXISTS idx_policy_product_id ON dim_policy(product_id);
CREATE INDEX IF NOT EXISTS idx_policy_status ON dim_policy(status_polis);