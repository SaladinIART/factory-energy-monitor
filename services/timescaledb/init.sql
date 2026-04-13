-- TimescaleDB schema for factory energy monitoring
-- Initialised automatically on first container start

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS meter_readings (
    time                TIMESTAMPTZ     NOT NULL,
    meter_id            TEXT            NOT NULL,
    zone_name           TEXT            NOT NULL,
    real_power_w        INTEGER,
    apparent_power_va   INTEGER,
    power_factor        NUMERIC(5,3),
    frequency_hz        NUMERIC(5,2),
    voltage_l1_v        NUMERIC(6,1),
    current_l1_a        NUMERIC(7,3),
    energy_kwh          NUMERIC(12,3)
);

-- Convert to a TimescaleDB hypertable (partitioned by time)
SELECT create_hypertable('meter_readings', 'time', if_not_exists => TRUE);

-- Index for fast zone/time queries
CREATE INDEX IF NOT EXISTS idx_meter_time ON meter_readings (meter_id, time DESC);

-- Continuous aggregate: hourly kWh and avg power per zone
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_energy
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    meter_id,
    zone_name,
    SUM(energy_kwh)             AS kwh_total,
    AVG(real_power_w) / 1000.0  AS avg_kw,
    AVG(power_factor)           AS avg_pf
FROM meter_readings
GROUP BY bucket, meter_id, zone_name
WITH NO DATA;
