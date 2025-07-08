-- Fact table for Insurance Registration Process
CREATE TABLE IF NOT EXISTS fact_registration (
    registration_key SERIAL PRIMARY KEY,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    time_key INT NOT NULL, -- Registration date
    policy_key INT NOT NULL,
    premium_amount NUMERIC(12, 2),
    sum_assured NUMERIC(15, 2),
    policy_term INT, -- in years
    referral_flag BOOLEAN,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (policy_key) REFERENCES dim_policy(policy_key)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_fact_reg_customer ON fact_registration(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_reg_product ON fact_registration(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_reg_time ON fact_registration(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_reg_policy ON fact_registration(policy_key);