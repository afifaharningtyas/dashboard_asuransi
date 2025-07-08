-- Dimension table for Marketing Program
CREATE TABLE IF NOT EXISTS dim_marketing_program (
    program_key SERIAL PRIMARY KEY,
    program_id INTEGER NOT NULL,
    nama_program VARCHAR(255),
    start_date DATE,
    end_date DATE,
    target NUMERIC(15, 2),
    description TEXT,
    jenis_program VARCHAR(50),
    budget NUMERIC(15, 2),
    program_status VARCHAR(20), -- Calculated field (Active, Completed, Planned)
    durasi_program INT, -- Calculated field in days
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX IF NOT EXISTS idx_program_id ON dim_marketing_program(program_id);
CREATE INDEX IF NOT EXISTS idx_program_current ON dim_marketing_program(is_current);