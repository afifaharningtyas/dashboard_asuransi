-- Fact table for Investment (Accumulating Snapshot)
CREATE TABLE IF NOT EXISTS fact_investment (
    investment_key SERIAL PRIMARY KEY,
    investment_id VARCHAR(20) NOT NULL,
    policy_key INT NOT NULL,
    customer_key INT NOT NULL,
    time_key_investment INT NOT NULL, -- Investment date
    investment_type VARCHAR(50),
    risk_level VARCHAR(30),
    fund_manager VARCHAR(100),
    initial_nav NUMERIC(12, 2),
    units_purchased NUMERIC(15, 4),
    amount NUMERIC(15, 2),
    return_percentage NUMERIC(5, 2),
    days_since_investment INT,
    investment_status VARCHAR(20),
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (policy_key) REFERENCES dim_policy(policy_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (time_key_investment) REFERENCES dim_time(time_key)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_fact_invest_policy ON fact_investment(policy_key);
CREATE INDEX IF NOT EXISTS idx_fact_invest_customer ON fact_investment(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_invest_time ON fact_investment(time_key_investment);
CREATE INDEX IF NOT EXISTS idx_fact_invest_type ON fact_investment(investment_type);

