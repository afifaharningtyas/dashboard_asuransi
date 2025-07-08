-- Fact table for Customer Service
CREATE TABLE IF NOT EXISTS fact_customer_service (
    service_key SERIAL PRIMARY KEY,
    service_id VARCHAR(20) NOT NULL,
    customer_key INT NOT NULL,
    employee_key INT NOT NULL,
    policy_key INT, -- Can be NULL if not related to a specific policy
    time_key_interaction INT NOT NULL, -- Interaction date
    time_key_resolution INT, -- Resolution date (can be NULL if not resolved)
    interaction_type VARCHAR(50),
    channel VARCHAR(50),
    complaint_category VARCHAR(50),
    duration_minutes INT,
    status VARCHAR(20),
    satisfaction_rating INT,
    follow_up_required BOOLEAN,
    resolution_time_hours INT, -- Time to resolution in hours
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key),
    FOREIGN KEY (policy_key) REFERENCES dim_policy(policy_key),
    FOREIGN KEY (time_key_interaction) REFERENCES dim_time(time_key),
    FOREIGN KEY (time_key_resolution) REFERENCES dim_time(time_key)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_fact_cs_customer ON fact_customer_service(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_cs_employee ON fact_customer_service(employee_key);
CREATE INDEX IF NOT EXISTS idx_fact_cs_policy ON fact_customer_service(policy_key);
CREATE INDEX IF NOT EXISTS idx_fact_cs_inter ON fact_customer_service(time_key_interaction);
CREATE INDEX IF NOT EXISTS idx_fact_cs_resol ON fact_customer_service(time_key_resolution);
CREATE INDEX IF NOT EXISTS idx_fact_cs_status ON fact_customer_service(status);
CREATE INDEX IF NOT EXISTS idx_fact_cs_type ON fact_customer_service(interaction_type);