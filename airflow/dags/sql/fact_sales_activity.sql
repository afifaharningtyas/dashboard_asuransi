-- Fact table for Sales and Marketing Activities (Periodic Snapshot)
CREATE TABLE IF NOT EXISTS fact_sales_activity (
    activity_key SERIAL PRIMARY KEY,
    activity_id VARCHAR(20) NOT NULL,
    employee_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    program_key INT, -- Can be NULL if not related to a marketing program
    time_key INT NOT NULL, -- Activity date
    activity_type VARCHAR(50),
    activity_channel VARCHAR(50),
    duration_minutes INT,
    status VARCHAR(20),
    resulted_in_sale BOOLEAN,
    follow_up_scheduled BOOLEAN,
    batch_id VARCHAR(50),
    snapshot_date DATE NOT NULL, -- Date when this snapshot was taken
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (program_key) REFERENCES dim_marketing_program(program_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_fact_sales_emp ON fact_sales_activity(employee_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_cust ON fact_sales_activity(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_prod ON fact_sales_activity(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_prog ON fact_sales_activity(program_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_time ON fact_sales_activity(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_snapshot ON fact_sales_activity(snapshot_date);