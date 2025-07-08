-- Dimension table for Product
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    nama_product VARCHAR(255),
    jenis_product VARCHAR(100),
    manfaat TEXT,
    premi_dasar NUMERIC(12, 2),
    kategori_premi VARCHAR(20), -- Calculated field (Low, Medium, High)
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX IF NOT EXISTS idx_product_id ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_product_current ON dim_product(is_current);