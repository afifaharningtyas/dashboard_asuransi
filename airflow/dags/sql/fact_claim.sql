-- Fact table for Insurance Claims
CREATE TABLE IF NOT EXISTS fact_claim (
    claim_key SERIAL PRIMARY KEY,
    claim_id VARCHAR(20) NOT NULL,
    policy_key INT NOT NULL,
    customer_key INT NOT NULL,
    time_key_submitted INT NOT NULL, -- Claim submission date
    time_key_decision INT, -- Claim decision date
    claim_type VARCHAR(100),
    claim_amount NUMERIC(15, 2),
    approved_amount NUMERIC(15, 2),
    status VARCHAR(20),
    processing_days INT, -- Days between submission and decision
    rejection_flag BOOLEAN,
    paid_flag BOOLEAN,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (policy_key) REFERENCES dim_policy(policy_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (time_key_submitted) REFERENCES dim_time(time_key),
    FOREIGN KEY (time_key_decision) REFERENCES dim_time(time_key)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_fact_claim_policy ON fact_claim(policy_key);
CREATE INDEX IF NOT EXISTS idx_fact_claim_customer ON fact_claim(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_claim_submit ON fact_claim(time_key_submitted);
CREATE INDEX IF NOT EXISTS idx_fact_claim_decision ON fact_claim(time_key_decision);
CREATE INDEX IF NOT EXISTS idx_fact_claim_status ON fact_claim(status);