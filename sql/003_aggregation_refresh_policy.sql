-- Migration: 003_aggregation_refresh_policy.sql
-- Purpose:   Register TimescaleDB automatic refresh policies on the three
--            temperature continuous aggregate views.  Each policy is wrapped
--            in a DO block so that the migration is idempotent — calling
--            add_continuous_aggregate_policy when a policy already exists
--            raises an error, so we check first.
--
-- Refresh schedule rationale:
--   daily view   — refresh every 1 hour; lag 1 hour so in-flight writes settle.
--   weekly view  — refresh every 12 hours; lag 1 day.
--   monthly view — refresh every 1 day; lag 3 days so weekly view is stable.
--
-- Dependencies:
--   - 002_create_aggregation_views.sql must be applied first.

-- ---------------------------------------------------------------------------
-- Daily view refresh policy
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    -- Skip if a policy already exists for this view to remain idempotent.
    IF NOT EXISTS (
        SELECT 1
        FROM   timescaledb_information.jobs j
        JOIN   timescaledb_information.job_stats js USING (job_id)
        WHERE  j.hypertable_name = 'temperature_daily'
          AND  j.proc_name       = 'policy_refresh_continuous_aggregate'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            continuous_aggregate => 'temperature_daily',
            -- Refresh data from 2 hours in the past …
            start_offset         => INTERVAL '2 hours',
            -- … up to 1 hour ago (allow recent writes to settle).
            end_offset           => INTERVAL '1 hour',
            schedule_interval    => INTERVAL '1 hour',
            if_not_exists        => TRUE
        );
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- Weekly view refresh policy
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   timescaledb_information.jobs j
        JOIN   timescaledb_information.job_stats js USING (job_id)
        WHERE  j.hypertable_name = 'temperature_weekly'
          AND  j.proc_name       = 'policy_refresh_continuous_aggregate'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            continuous_aggregate => 'temperature_weekly',
            -- Refresh data from 2 days in the past …
            start_offset         => INTERVAL '2 days',
            -- … up to 1 day ago (allow daily aggregation to stabilise).
            end_offset           => INTERVAL '1 day',
            schedule_interval    => INTERVAL '12 hours',
            if_not_exists        => TRUE
        );
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- Monthly view refresh policy
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   timescaledb_information.jobs j
        JOIN   timescaledb_information.job_stats js USING (job_id)
        WHERE  j.hypertable_name = 'temperature_monthly'
          AND  j.proc_name       = 'policy_refresh_continuous_aggregate'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            continuous_aggregate => 'temperature_monthly',
            -- Refresh data from 5 days in the past …
            start_offset         => INTERVAL '5 days',
            -- … up to 3 days ago (allow weekly aggregation to stabilise).
            end_offset           => INTERVAL '3 days',
            schedule_interval    => INTERVAL '1 day',
            if_not_exists        => TRUE
        );
    END IF;
END;
$$;
