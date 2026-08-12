# Sapphire Kafka Pipeline

A Kafka Connect pipeline for streaming health telemetry data from Kafka topics to a PostgreSQL timeseries database. This system processes health metrics including heart rate, blood pressure, glucose levels, SpO2, activity, sleep, workout, and **body temperature**.

## Overview

This repository contains Kafka Connect configurations for ingesting health telemetry data from multiple Kafka topics and persisting it to a PostgreSQL database. The pipeline uses three specialized connectors:

- **Device Sink Connector**: Manages device metadata and registration
- **Metrics Sink Connector**: Handles timeseries health metrics data
- **Temperature Sink Connector**: Handles body temperature telemetry with idempotent deduplication

## Architecture

```
Kafka Topics (health.metrics.* | temperature-events)
    ↓
Schema Registry (Avro)
    ↓
Kafka Connect
    ├── Device Sink Connector     → PostgreSQL (devices table)
    ├── Metrics Sink Connector    → PostgreSQL (metric-specific tables)
    └── Temperature Sink Connector → PostgreSQL (temperature_metrics hypertable)
```

### Data Flow

1. **Source**: Health metrics published to Kafka topics with Avro schema
2. **Transform**: Kafka Connect applies transformations (flatten, rename, route)
3. **Sink**: Data persisted to PostgreSQL timeseries tables
4. **Error Handling**: Failed records routed to dead letter queues

---

## Temperature Pipeline

### Overview

Body temperature events are ingested via `sapphire-event-ingestion-api`, published to the `temperature-events` Kafka topic in Avro format, and sinked into the `temperature_metrics` TimescaleDB hypertable. Three continuous aggregate views (`temperature_daily`, `temperature_weekly`, `temperature_monthly`) provide pre-computed min/max/avg values consumed by `sapphire-charting-api`.

### Avro Schema

Schema file: [`schemas/temperature-event-schema.avro`](schemas/temperature-event-schema.avro)

| Field | Type | Required | Description |
|---|---|---|---|
| `user_id` | string | Yes | Platform user identifier |
| `value` | double | Yes | Temperature value as submitted |
| `unit` | enum (`CELSIUS`, `FAHRENHEIT`) | Yes | Unit of the submitted value |
| `timestamp_iso8601` | string (ISO 8601) | Yes | Measurement timestamp with timezone |
| `device_source` | string | Yes | Device or API source identifier |
| `ingestion_source` | enum (`DEVICE_PUSH`, `API`) | Yes | How the event arrived |
| `measurement_method` | string or null | No | e.g. oral, axillary, tympanic |
| `created_at` | string (ISO 8601) | Yes | When the ingestion service created the event |
| `dedup_hash` | string | Yes | SHA-256(`user_id` \| `device_source` \| `timestamp_iso8601` \| `value`) |

Schema compatibility is set to **BACKWARD** — all future optional fields must default to `null`.

### Kafka Topic

| Property | Value |
|---|---|
| Topic name | `temperature-events` |
| Value format | Avro (registered in Schema Registry) |
| Key format | String (user_id) |
| Partitions | Match existing metric topics |
| DLQ topic | `dlq-postgres-sink-temperature` |

### Connector Config

Config file: [`connectors/postgres-sink-config-temperature.json`](connectors/postgres-sink-config-temperature.json)

Key properties:

| Property | Value | Reason |
|---|---|---|
| `insert.mode` | `insert` | Append-only timeseries writes |
| `pk.mode` | `record_value` | PK derived from the Avro record |
| `pk.fields` | `dedup_hash` | Idempotency: unique constraint on `dedup_hash` prevents duplicates |
| `table.name.format` | `temperature_metrics` | Fixed target table |
| `transforms` | `RenameTimestamp` | Renames `timestamp_iso8601` → `recorded_at` to match the DB column |
| `auto.create` | `false` | Table is pre-created by DDL migration |
| `errors.tolerance` | `all` | Duplicate inserts (unique constraint violations) routed to DLQ silently |

> **Deduplication note**: The `dedup_hash` field is the primary key in `temperature_metrics`. When the sink connector attempts to insert a duplicate record the database raises a unique constraint violation. Because `errors.tolerance=all` is set, the connector routes the failed record to `dlq-postgres-sink-temperature` and continues — this is the intended idempotency mechanism.

### DDL Migrations

Apply migrations in the following order before deploying the connector:

```bash
psql -U admin -d sapphire -f sql/001_create_temperature_hypertable.sql
psql -U admin -d sapphire -f sql/002_create_aggregation_views.sql
psql -U admin -d sapphire -f sql/003_aggregation_refresh_policy.sql
```

All migrations are idempotent (`IF NOT EXISTS` guards throughout).

| File | Purpose |
|---|---|
| `sql/001_create_temperature_hypertable.sql` | Creates `temperature_metrics` table, indexes, and TimescaleDB hypertable |
| `sql/002_create_aggregation_views.sql` | Creates `temperature_daily`, `temperature_weekly`, `temperature_monthly` continuous aggregate views |
| `sql/003_aggregation_refresh_policy.sql` | Registers automatic refresh policies for all three aggregate views |

---

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd sapphire-kafka-pipeline
```

### 2. Configure Database Connection

Update the connection details in the connector configuration files:

```json
"connection.url": "jdbc:postgresql://your-postgres-host:5432/sapphire",
"connection.user": "your-username",
"connection.password": "your-password"
```

### 3. Deploy Connectors

```bash
# Make the script executable
chmod +x scripts/deploy-connector.sh

# Deploy existing health metrics connectors
./scripts/deploy-connector.sh

# Deploy the temperature connector separately
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/postgres-sink-config-temperature.json \
  http://localhost:8083/connectors
```

### 4. Verify Deployment

```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-device-sink-connector/status
curl http://localhost:8083/connectors/postgres-sink-connector/status
curl http://localhost:8083/connectors/postgres-sink-config-temperature/status
```

### Local Integration Test Environment

Use the provided `docker-compose-test.yml` to spin up Kafka, Schema Registry, and PostgreSQL+TimescaleDB locally. The SQL migrations are automatically applied at container start via volume mounts into `docker-entrypoint-initdb.d`.

```bash
# Start all services
docker compose -f docker-compose-test.yml up -d

# Wait for healthchecks to pass, then register the temperature connector
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/postgres-sink-config-temperature.json \
  http://localhost:8083/connectors

# Verify the hypertable was created
psql -h localhost -U sapphire -d sapphire_test \
  -c "SELECT hypertable_name FROM timescaledb_information.hypertables;"

# Publish a test temperature event (replace with your Kafka client)
kafka-avro-console-producer \
  --bootstrap-server localhost:9092 \
  --topic temperature-events \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema.file=schemas/temperature-event-schema.avro

# Verify the record landed in PostgreSQL
psql -h localhost -U sapphire -d sapphire_test \
  -c "SELECT user_id, value, unit, recorded_at FROM temperature_metrics LIMIT 5;"

# Tear down
docker compose -f docker-compose-test.yml down -v
```

---

## Configuration Files

### [`config/postgres-device-sink-connector.json`](config/postgres-device-sink-connector.json)

Manages device metadata with the following characteristics:

- **Topics**: All health.metrics.* topics
- **Target Table**: `devices`
- **Insert Mode**: Upsert (based on device_id)
- **Key Fields**: device_id, device_type, device_manufacturer, device_model, user_id, app_version

### [`config/postgres-sink-connector.json`](config/postgres-sink-connector.json)

Handles timeseries health metrics data:

- **Topics**: All health.metrics.* topics
- **Target Tables**: Dynamically routed (heartrate, bloodpressure, glucose, etc.)
- **Insert Mode**: Insert (append-only for timeseries)
- **Primary Key**: Composite (time, user_id, metric_name)
- **Timestamp Handling**: Converts nanosecond Unix timestamps to PostgreSQL timestamps

### [`connectors/postgres-sink-config-temperature.json`](connectors/postgres-sink-config-temperature.json)

Handles body temperature telemetry:

- **Topic**: `temperature-events`
- **Target Table**: `temperature_metrics` (TimescaleDB hypertable)
- **Insert Mode**: Insert (append-only)
- **Primary Key**: `dedup_hash` (idempotent deduplication)
- **Timestamp Handling**: Renames `timestamp_iso8601` → `recorded_at`

---

## Monitored Health Metrics

The pipeline processes the following health metric types:

- **Heart Rate** (`health.metrics.heartrate`)
- **Blood Pressure** (`health.metrics.bloodpressure`)
- **Glucose** (`health.metrics.glucose`)
- **SpO2** (`health.metrics.spo2`)
- **Activity** (`health.metrics.activity`)
- **Sleep** (`health.metrics.sleep`)
- **Workout** (`health.metrics.workout`)
- **Body Temperature** (`temperature-events`) — TimescaleDB hypertable with daily/weekly/monthly aggregates

---

## Data Transformations

Both health metrics connectors apply several transformations:

1. **RegexRouter**: Routes messages to appropriate tables based on topic name
2. **Flatten**: Flattens nested Avro structures with underscore delimiter
3. **ReplaceField**: Renames fields to match database schema
4. **TimestampConverter**: Converts Unix nanosecond timestamps to SQL timestamps

The temperature connector applies a single `ReplaceField` rename (`timestamp_iso8601` → `recorded_at`). The ISO-8601 string is stored as-is; the `recorded_at TIMESTAMPTZ` column accepts the standard PostgreSQL ISO-8601 cast.

---

## Management Operations

### View Connector Status

```bash
curl http://localhost:8083/connectors/postgres-device-sink-connector/status 
curl http://localhost:8083/connectors/postgres-sink-connector/status 
curl http://localhost:8083/connectors/postgres-sink-config-temperature/status
```

### Pause a Connector

```bash
curl -X PUT http://localhost:8083/connectors/postgres-device-sink-connector/pause
```

### Resume a Connector

```bash
curl -X PUT http://localhost:8083/connectors/postgres-device-sink-connector/resume
```

### Delete a Connector

```bash
curl -X DELETE http://localhost:8083/connectors/postgres-device-sink-connector
```

### Update Connector Configuration

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  --data @config/postgres-device-sink-connector.json \
  http://localhost:8083/connectors/postgres-device-sink-connector/config
```


### Useful Endpoints

```bash
# List all connectors
curl http://localhost:8083/connectors

# Get connector configuration
curl http://localhost:8083/connectors/postgres-sink-connector/config

# Get connector tasks
curl http://localhost:8083/connectors/postgres-sink-connector/tasks
```

---

## Documentation

- [Setup Guide](docs/SETUP.md) - Detailed installation and configuration
- [Architecture](docs/ARCHITECTURE.md) - System design and data flow
- [Connector Configuration](docs/CONNECTOR_CONFIG.md) - Configuration reference
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions

---

## Database Schema

### Devices Table

```sql
CREATE TABLE devices (
    device_id VARCHAR PRIMARY KEY,
    device_type VARCHAR,
    device_manufacturer VARCHAR,
    device_model VARCHAR,
    user_id VARCHAR,
    app_version VARCHAR
);
```

### Metrics Tables (Example: Heart Rate)

```sql
CREATE TABLE heartrate (
    time TIMESTAMP,
    user_id VARCHAR,
    metric_name VARCHAR,
    device_id VARCHAR,
    metric_value DOUBLE PRECISION,
    unit VARCHAR,
    attributes JSONB,
    start_time BIGINT,
    ingestion_timestamp TIMESTAMP,
    ingestion_time TIMESTAMP,
    request_id VARCHAR,
    scope_name VARCHAR,
    scope_version VARCHAR,
    schema_version VARCHAR,
    PRIMARY KEY (time, user_id, metric_name)
);
```

### Temperature Metrics Table (TimescaleDB Hypertable)

```sql
CREATE TABLE temperature_metrics (
    user_id          TEXT NOT NULL,
    device_source    TEXT NOT NULL,
    ingestion_source TEXT NOT NULL,
    value            DOUBLE PRECISION NOT NULL,
    unit             TEXT NOT NULL,          -- 'CELSIUS' or 'FAHRENHEIT'
    recorded_at      TIMESTAMPTZ NOT NULL,   -- hypertable partition dimension
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    measurement_method TEXT,                 -- optional: oral, axillary, tympanic
    dedup_hash       TEXT NOT NULL,
    CONSTRAINT temperature_metrics_dedup_hash_uq UNIQUE (dedup_hash)
);
-- Converted to a TimescaleDB hypertable partitioned on recorded_at (7-day chunks)
```

See [`sql/001_create_temperature_hypertable.sql`](sql/001_create_temperature_hypertable.sql) for the full migration including indexes and hypertable conversion.
