-- Sentinet TimescaleDB Initialization Script
-- Creates hypertables and indexes for high-frequency market data

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Market Ticks Table (Raw Level 1 Data)
CREATE TABLE market_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    bid_price DOUBLE PRECISION,
    ask_price DOUBLE PRECISION,
    bid_size DOUBLE PRECISION,
    ask_size DOUBLE PRECISION,
    volume BIGINT,
    exchange VARCHAR(10)
);

-- Convert to hypertable (partitioned by time)
SELECT create_hypertable('market_ticks', 'time');

-- Create indexes for efficient queries
CREATE INDEX idx_market_ticks_symbol_time ON market_ticks (symbol, time DESC);
CREATE INDEX idx_market_ticks_time ON market_ticks (time DESC);

-- Asset Metrics Table (Aggregated Analytics)
CREATE TABLE asset_metrics (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    z_score DOUBLE PRECISION,
    rolling_mean DOUBLE PRECISION,
    rolling_std DOUBLE PRECISION,
    imbalance_ratio DOUBLE PRECISION,
    volume_sum BIGINT
);

-- Convert to hypertable
SELECT create_hypertable('asset_metrics', 'time');

-- Create indexes
CREATE INDEX idx_asset_metrics_symbol_time ON asset_metrics (symbol, time DESC);

-- Correlation Matrix Table
CREATE TABLE correlation_matrix (
    time TIMESTAMPTZ NOT NULL,
    symbol_a VARCHAR(20) NOT NULL,
    symbol_b VARCHAR(20) NOT NULL,
    correlation DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (time, symbol_a, symbol_b)
);

-- Convert to hypertable
SELECT create_hypertable('correlation_matrix', 'time');

-- Alerts Table (Anomaly Events)
CREATE TABLE alerts (
    id SERIAL,
    time TIMESTAMPTZ NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(20),
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB
);

-- Convert to hypertable
SELECT create_hypertable('alerts', 'time');

-- Create indexes
CREATE INDEX idx_alerts_time ON alerts (time DESC);
CREATE INDEX idx_alerts_severity ON alerts (severity, time DESC);
CREATE INDEX idx_alerts_type ON alerts (alert_type, time DESC);

-- Retention policy: Drop data older than 30 days
SELECT add_retention_policy('market_ticks', INTERVAL '30 days');
SELECT add_retention_policy('asset_metrics', INTERVAL '30 days');
SELECT add_retention_policy('correlation_matrix', INTERVAL '30 days');
SELECT add_retention_policy('alerts', INTERVAL '90 days');

-- Compression policy: Compress data older than 7 days
ALTER TABLE market_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol'
);

ALTER TABLE asset_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol'
);

SELECT add_compression_policy('market_ticks', INTERVAL '7 days');
SELECT add_compression_policy('asset_metrics', INTERVAL '7 days');

-- Create a continuous aggregate for 1-minute OHLCV bars
CREATE MATERIALIZED VIEW market_ticks_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    symbol,
    FIRST(price, time) AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price, time) AS close,
    SUM(volume) AS volume
FROM market_ticks
GROUP BY bucket, symbol;

-- Refresh policy for the continuous aggregate
SELECT add_continuous_aggregate_policy('market_ticks_1min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

-- Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sentinet_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sentinet_user;
