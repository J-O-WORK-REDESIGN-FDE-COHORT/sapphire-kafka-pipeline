-- Migration: 001_create_temperature_hypertable.sql
-- Purpose:   Create the temperature_metrics table and convert it to a TimescaleDB
--            hypertable partitioned on recorded_at.  All DDL is guarded with
--            IF NOT EXISTS so the migration is safe to re-run.
--
-- Table: temperature_metrics
--   Stores raw body temperature readings ingested from devices and APIs.
--   The dedup_hash column (SHA-256 of user_id + device_source + timestamp + value)
--   enforces idempotent writes: duplicate submissions produce a unique-constraint
--   violation that the Kafka Connect sink connector is configured to ignore.
--
-- Dependencies:
--   - TimescaleDB extension must be installed in the target database.
--   - Run BEFORE 002_create_aggregation_views.sql and 003_aggregation_refresh_policy.sql.

-- Enable TimescaleDB if not already active.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Main raw measurements table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS temperature_metrics (
    -- Identification
    user_id         TEXT        NOT NULL,
    device_source   TEXT        NOT NULL,
    ingestion_source TEXT       NOT NULL,

    -- Measurement
    value           DOUBLE PRECISION NOT NULL,
    unit            TEXT        NOT NULL
        CONSTRAINT temperature_metrics_unit_check
            CHECK (unit IN ('CELSIUS', 'FAHRENHEIT')),

    -- Timestamps
    recorded_at     TIMESTAMPTZ NOT NULL,   -- partition dimension for hypertable
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Optional metadata
    measurement_method TEXT,               -- oral, axillary, tympanic, etc.

    -- Idempotency: SHA-256(user_id || device_source || timestamp_iso8601 || value)
    dedup_hash      TEXT        NOT NULL,

    CONSTRAINT temperature_metrics_dedup_hash_uq UNIQUE (dedup_hash)
);

COMMENT ON TABLE temperature_metrics IS
    'Raw body temperature readings ingested from smart devices and external APIs. '
    'Partitioned by recorded_at as a TimescaleDB hypertable. '
    'Duplicate submissions are prevented by the dedup_hash unique constraint.';

COMMENT ON COLUMN temperature_metrics.dedup_hash IS
    'SHA-256 hex digest of (user_id || device_source || timestamp_iso8601 || value). '
    'Computed by the ingestion service before publishing to Kafka. '
    'Used as the idempotency key to prevent duplicate stored records.';

-- ---------------------------------------------------------------------------
-- Indexes for common query patterns
-- ---------------------------------------------------------------------------

-- Primary access pattern: retrieve a user's readings in a date range.
CREATE INDEX IF NOT EXISTS temperature_metrics_user_recorded_idx
    ON temperature_metrics (user_id, recorded_at DESC);

-- Secondary filter: per-device breakdown within a user's readings.
CREATE INDEX IF NOT EXISTS temperature_metrics_user_device_recorded_idx
    ON temperature_metrics (user_id, device_source, recorded_at DESC);

-- ---------------------------------------------------------------------------
-- Convert to TimescaleDB hypertable
-- ---------------------------------------------------------------------------
-- The DO block is idempotent: it only calls create_hypertable when the table
-- is not already a hypertable (i.e. absent from timescaledb_information.hypertables).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   timescaledb_information.hypertables
        WHERE  hypertable_schema = current_schema()
          AND  hypertable_name   = 'temperature_metrics'
    ) THEN
        PERFORM create_hypertable(
            'temperature_metrics',
            'recorded_at',
            chunk_time_interval => INTERVAL '7 days',
            if_not_exists       => TRUE
        );
    END IF;
END;
$$;
