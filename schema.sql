-- SQL schema for customer_heartbeats time-series table
CREATE TABLE IF NOT EXISTS customer_heartbeats (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    heart_rate INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_anomaly BOOLEAN NOT NULL DEFAULT false,
    -- ensure idempotent behaviour for identical sensor events
    UNIQUE (customer_id, timestamp)
);

-- Indexes for efficient lookup by time and by customer
CREATE INDEX IF NOT EXISTS idx_customer_heartbeats_ts ON customer_heartbeats (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_customer_heartbeats_customer ON customer_heartbeats (customer_id);

COMMENT ON TABLE customer_heartbeats IS 'Time-series store for synthetic customer heart rate readings';

-- Optional: partitioning strategy (uncomment and adapt for large datasets)
-- Example: partition the table by RANGE on timestamp (monthly partitions)
-- Note: to enable partitioning, create a partitioned parent table and migrate data or recreate table.
--
-- CREATE TABLE customer_heartbeats_part (
--   id BIGSERIAL PRIMARY KEY,
--   customer_id TEXT NOT NULL,
--   timestamp TIMESTAMPTZ NOT NULL,
--   heart_rate INTEGER NOT NULL,
--   ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
--   is_anomaly BOOLEAN NOT NULL DEFAULT false
-- ) PARTITION BY RANGE (timestamp);
--
-- CREATE TABLE customer_heartbeats_2026_05 PARTITION OF customer_heartbeats_part
--    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');


