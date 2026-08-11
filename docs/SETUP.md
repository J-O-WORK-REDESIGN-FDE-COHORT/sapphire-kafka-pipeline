# Setup Guide

This guide provides detailed instructions for setting up and deploying the Sapphire Kafka Pipeline.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Database Setup](#database-setup)
- [Kafka Connect Setup](#kafka-connect-setup)
- [Connector Deployment](#connector-deployment)
- [Verification](#verification)
- [Configuration Customization](#configuration-customization)

## Prerequisites

### Required Components

1. **Kafka Cluster** (v2.8.0 or higher)
   - Running and accessible
   - Topics created for health metrics

2. **Kafka Connect** (v2.8.0 or higher)
   - Confluent JDBC Sink Connector plugin installed
   - Running in distributed mode (recommended)

3. **Schema Registry** (v6.0.0 or higher)
   - Configured and accessible
   - Avro schema support enabled

4. **PostgreSQL Database** (v12.0 or higher)
   - Timeseries extension recommended (TimescaleDB)
   - Database `sapphire` created
   - User with appropriate permissions

5. **Tools**
   - `curl` for API interactions
   - `jq` for JSON parsing (optional but recommended)
   - Docker/Podman (if using containerized deployment)

### Network Requirements

Ensure the following network connectivity:
- Kafka Connect can reach Kafka brokers
- Kafka Connect can reach Schema Registry
- Kafka Connect can reach PostgreSQL database
- Your deployment machine can reach Kafka Connect REST API (port 8083)

## Environment Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd sapphire-kafka-pipeline
```

### 2. Verify Directory Structure

```
sapphire-kafka-pipeline/
├── config/
│   ├── postgres-device-sink-connector.json
│   └── postgres-sink-connector.json
├── scripts/
│   └── deploy-connector.sh
├── docs/
│   ├── SETUP.md
│   ├── ARCHITECTURE.md
│   ├── CONNECTOR_CONFIG.md
│   └── TROUBLESHOOTING.md
└── README.md
```

### 3. Set Environment Variables (Optional)

```bash
export KAFKA_CONNECT_URL="http://localhost:8083"
export POSTGRES_HOST="shared-postgres"
export POSTGRES_PORT="5432"
export POSTGRES_DB="sapphire"
export POSTGRES_USER="admin"
export POSTGRES_PASSWORD="admin123"
export SCHEMA_REGISTRY_URL="http://schema-registry:8081"
```

## Database Setup

### 1. Create Database

```sql
CREATE DATABASE sapphire;
```

### 2. Create Tables

#### Devices Table

```sql
CREATE TABLE devices (
    device_id VARCHAR(255) PRIMARY KEY,
    device_type VARCHAR(100),
    device_manufacturer VARCHAR(100),
    device_model VARCHAR(100),
    user_id VARCHAR(255),
    app_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for user lookups
CREATE INDEX idx_devices_user_id ON devices(user_id);
```

#### Metrics Tables

Create a table for each metric type. Example for heart rate:

```sql
CREATE TABLE heartrate (
    time TIMESTAMP NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    device_id VARCHAR(255),
    metric_value DOUBLE PRECISION,
    unit VARCHAR(50),
    attributes JSONB,
    start_time BIGINT,
    ingestion_timestamp TIMESTAMP,
    ingestion_time TIMESTAMP,
    request_id VARCHAR(255),
    scope_name VARCHAR(100),
    scope_version VARCHAR(50),
    schema_version VARCHAR(50),
    PRIMARY KEY (time, user_id, metric_name)
);

-- Index for time-based queries
CREATE INDEX idx_heartrate_time ON heartrate(time DESC);
CREATE INDEX idx_heartrate_user_time ON heartrate(user_id, time DESC);
```

Repeat for other metric types:
- `bloodpressure`
- `glucose`
- `spo2`
- `activity`
- `sleep`
- `workout`

### 3. Enable TimescaleDB (Optional but Recommended)

For better timeseries performance:

```sql
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Convert tables to hypertables
SELECT create_hypertable('heartrate', 'time');
SELECT create_hypertable('bloodpressure', 'time');
SELECT create_hypertable('glucose', 'time');
SELECT create_hypertable('spo2', 'time');
SELECT create_hypertable('activity', 'time');
SELECT create_hypertable('sleep', 'time');
SELECT create_hypertable('workout', 'time');
```

### 4. Grant Permissions

```sql
GRANT ALL PRIVILEGES ON DATABASE sapphire TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;
```

## Kafka Connect Setup

### 1. Verify Kafka Connect Installation

```bash
curl http://localhost:8083/
```

Expected response:
```json
{
  "version": "2.8.0",
  "commit": "...",
  "kafka_cluster_id": "..."
}
```

### 2. Verify JDBC Connector Plugin

```bash
curl http://localhost:8083/connector-plugins | jq
```

Look for `io.confluent.connect.jdbc.JdbcSinkConnector` in the output.

### 3. Check Available Connectors

```bash
curl http://localhost:8083/connectors | jq
```

## Connector Deployment

### Method 1: Using Deployment Script (Recommended)

```bash
# Make script executable
chmod +x scripts/deploy-connector.sh

# Deploy both connectors
./scripts/deploy-connector.sh
```

The script will:
1. Wait for Kafka Connect to be ready
2. Check for existing connectors
3. Delete existing connectors if found
4. Deploy both connectors
5. Verify deployment status

### Method 2: Manual Deployment

#### Deploy Device Sink Connector

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @config/postgres-device-sink-connector.json \
  http://localhost:8083/connectors
```

#### Deploy Metrics Sink Connector

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @config/postgres-sink-connector.json \
  http://localhost:8083/connectors
```

## Verification

### 1. Check Connector Status

```bash
# Device connector
curl http://localhost:8083/connectors/postgres-device-sink-connector/status | jq

# Metrics connector
curl http://localhost:8083/connectors/postgres-sink-connector/status | jq
```

Expected output for healthy connector:
```json
{
  "name": "postgres-device-sink-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "..."
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "..."
    }
  ]
}
```

### 2. Verify Data Flow

#### Check Kafka Topics

```bash
# List topics
kafka-topics --bootstrap-server localhost:9092 --list | grep health.metrics

# Check topic messages
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic health.metrics.heartrate \
  --from-beginning \
  --max-messages 1
```

#### Check Database Tables

```sql
-- Check devices table
SELECT COUNT(*) FROM devices;
SELECT * FROM devices LIMIT 5;

-- Check metrics tables
SELECT COUNT(*) FROM heartrate;
SELECT * FROM heartrate ORDER BY time DESC LIMIT 5;
```

### 3. Monitor Connector Logs

```bash
# If using Docker/Podman
docker logs -f kafka-connect

# Look for successful message processing
# Example: "WorkerSinkTask{id=postgres-sink-connector-0} Committing offsets"
```

## Configuration Customization

### Update Database Connection

Edit both connector configuration files:

```json
{
  "config": {
    "connection.url": "jdbc:postgresql://YOUR_HOST:YOUR_PORT/YOUR_DB",
    "connection.user": "YOUR_USER",
    "connection.password": "YOUR_PASSWORD"
  }
}
```

### Update Schema Registry URL

```json
{
  "config": {
    "value.converter.schema.registry.url": "http://YOUR_SCHEMA_REGISTRY:8081"
  }
}
```

### Adjust Task Parallelism

For higher throughput, increase the number of tasks:

```json
{
  "config": {
    "tasks.max": "3"
  }
}
```

### Modify Topic List

To add or remove topics:

```json
{
  "config": {
    "topics": "health.metrics.heartrate,health.metrics.bloodpressure,health.metrics.newmetric"
  }
}
```

### Update Connector Configuration

After modifying configuration files:

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  --data @config/postgres-device-sink-connector.json \
  http://localhost:8083/connectors/postgres-device-sink-connector/config
```

## Post-Deployment Tasks

### 1. Set Up Monitoring

- Configure connector metrics collection
- Set up alerts for connector failures
- Monitor dead letter queue topics

### 2. Create Indexes

Add indexes based on your query patterns:

```sql
-- Example: Index for device lookups in metrics
CREATE INDEX idx_heartrate_device_id ON heartrate(device_id);
```

### 3. Configure Retention Policies

For timeseries data:

```sql
-- Example: Drop data older than 90 days
SELECT add_retention_policy('heartrate', INTERVAL '90 days');
```

### 4. Set Up Backups

Configure regular database backups:

```bash
# Example: Daily backup
pg_dump -h localhost -U admin sapphire > backup_$(date +%Y%m%d).sql
```

## Troubleshooting

If you encounter issues during setup, refer to [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for common problems and solutions.

## Next Steps

- Review [Architecture Documentation](ARCHITECTURE.md)
- Understand [Connector Configuration](CONNECTOR_CONFIG.md)
- Set up monitoring and alerting
- Configure data retention policies
- Implement backup strategies