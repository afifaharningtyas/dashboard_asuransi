-- Dimension table for Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_key SERIAL PRIMARY KEY,
    date_actual DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_actual INT,
    month_name VARCHAR(10),
    quarter_actual INT,
    year_actual INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100),
    fiscal_year INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Unique index on actual date
CREATE UNIQUE INDEX IF NOT EXISTS idx_date_actual ON dim_time(date_actual);