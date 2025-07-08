-- Dimension table for Employee
CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    nama VARCHAR(255),
    tanggal_lahir DATE,
    alamat TEXT,
    jabatan VARCHAR(100),
    department VARCHAR(50),
    tanggal_masuk DATE,
    umur INT, -- Calculated field based on birth date
    masa_kerja INT, -- Calculated field in days
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX IF NOT EXISTS idx_employee_id ON dim_employee(employee_id);
CREATE INDEX IF NOT EXISTS idx_employee_current ON dim_employee(is_current);