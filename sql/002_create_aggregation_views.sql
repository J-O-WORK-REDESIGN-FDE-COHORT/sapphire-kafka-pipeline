-- Migration: 002_create_aggregation_views.sql
-- Purpose:   Create TimescaleDB continuous aggregate views for temperature_metrics.
--            Three granularities are provided: daily, weekly, and monthly.
--            Each view computes min, max, and avg temperature per user_id and
--            device_source bucket, which is the primary shape consumed by
--            sapphire-charting-api trend queries.
--
-- Naming convention:
--   temperature_daily   — one row per user_id + device_source + calendar day
--   temperature_weekly  — one row per user_id + device_source + ISO week
--   temperature_monthly — one row per user_id + device_source + calendar month
--
-- Dependencies:
--   - 001_create_temperature_hypertable.sql must be applied first.
--   - Run BEFORE 003_aggregation_refresh_policy.sql.

-- ---------------------------------------------------------------------------
-- Daily continuous aggregate
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS temperature_daily
WITH (timescaledb.continuous) AS
SELECT
    user_id,
    device_source,
    time_bucket('1 day', recorded_at)   AS bucket,
    MIN(value)                           AS min_value,
    MAX(value)                           AS max_value,
    AVG(value)                           AS avg_value,
    -- Preserve the unit of the first record in the bucket.
    -- All records in a bucket for a given user + device will share a unit
    -- because the ingestion service stores values as submitted; the display
    -- layer normalises units at query time.
    MAX(unit)                            AS unit,
    COUNT(*)                             AS sample_count
FROM temperature_metrics
GROUP BY user_id, device_source, time_bucket('1 day', recorded_at)
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW temperature_daily IS
    'Daily aggregation of temperature_metrics: min/max/avg value per user and device source. '
    'Refreshed automatically via the policy in 003_aggregation_refresh_policy.sql.';

-- ---------------------------------------------------------------------------
-- Weekly continuous aggregate
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS temperature_weekly
WITH (timescaledb.continuous) AS
SELECT
    user_id,
    device_source,
    time_bucket('7 days', recorded_at)  AS bucket,
    MIN(value)                           AS min_value,
    MAX(value)                           AS max_value,
    AVG(value)                           AS avg_value,
    MAX(unit)                            AS unit,
    COUNT(*)                             AS sample_count
FROM temperature_metrics
GROUP BY user_id, device_source, time_bucket('7 days', recorded_at)
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW temperature_weekly IS
    'Weekly (7-day bucket) aggregation of temperature_metrics: min/max/avg value per user and device source. '
    'Refreshed automatically via the policy in 003_aggregation_refresh_policy.sql.';

-- ---------------------------------------------------------------------------
-- Monthly continuous aggregate
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS temperature_monthly
WITH (timescaledb.continuous) AS
SELECT
    user_id,
    device_source,
    time_bucket('1 month', recorded_at) AS bucket,
    MIN(value)                           AS min_value,
    MAX(value)                           AS max_value,
    AVG(value)                           AS avg_value,
    MAX(unit)                            AS unit,
    COUNT(*)                             AS sample_count
FROM temperature_metrics
GROUP BY user_id, device_source, time_bucket('1 month', recorded_at)
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW temperature_monthly IS
    'Monthly (calendar-month bucket) aggregation of temperature_metrics: min/max/avg value per user and device source. '
    'Refreshed automatically via the policy in 003_aggregation_refresh_policy.sql.';
