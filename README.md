# Sapphire Kafka Pipeline

A Kafka Connect pipeline for streaming health telemetry data from Kafka topics to a PostgreSQL timeseries database. This system processes health metrics including heart rate, blood pressure, glucose levels, SpO2, activity, sleep, and workout data.

## Overview

This repository contains Kafka Connect configurations for ingesting health telemetry data from multiple Kafka topics and persisting it to a PostgreSQL database. The pipeline uses two specialized connectors:

- **Device Sink Connector**: Manages device metadata and registration
- **Metrics Sink Connector**: Handles timeseries health metrics data

## Architecture

```
Kafka Topics (health.metrics.*) 
    ↓
Schema Registry (Avro)
    ↓
Kafka Connect
    ├── Device Sink Connector → PostgreSQL (devices table)
    └── Metrics Sink Connector → PostgreSQL (metric-specific tables)
```

### Data Flow

1. **Source**: Health metrics published to Kafka topics with Avro schema
2. **Transform**: Kafka Connect applies transformations (flatten, rename, route)
3. **Sink**: Data persisted to PostgreSQL timeseries tables
4. **Error Handling**: Failed records routed to dead letter queues



## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd sapphire-kafka-pipeline
```

### 2. Configure Database Connection

Update the connection details in both connector configuration files:

```json
"connection.url": "jdbc:postgresql://your-postgres-host:5432/sapphire",
"connection.user": "your-username",
"connection.password": "your-password"
```

### 3. Deploy Connectors

```bash
# Make the script executable
chmod +x scripts/deploy-connector.sh

# Deploy both connectors
./scripts/deploy-connector.sh
```

### 4. Verify Deployment

```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-device-sink-connector/status
curl http://localhost:8083/connectors/postgres-sink-connector/status
```

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

## Monitored Health Metrics

The pipeline processes the following health metric types:

- **Heart Rate** (`health.metrics.heartrate`)
- **Blood Pressure** (`health.metrics.bloodpressure`)
- **Glucose** (`health.metrics.glucose`)
- **SpO2** (`health.metrics.spo2`)
- **Activity** (`health.metrics.activity`)
- **Sleep** (`health.metrics.sleep`)
- **Workout** (`health.metrics.workout`)

## Data Transformations

Both connectors apply several transformations:

1. **RegexRouter**: Routes messages to appropriate tables based on topic name
2. **Flatten**: Flattens nested Avro structures with underscore delimiter
3. **ReplaceField**: Renames fields to match database schema
4. **TimestampConverter**: Converts Unix nanosecond timestamps to SQL timestamps


## Management Operations

### View Connector Status

```bash
curl http://localhost:8083/connectors/postgres-device-sink-connector/status 
curl http://localhost:8083/connectors/postgres-sink-connector/status 
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

## Documentation

- [Setup Guide](docs/SETUP.md) - Detailed installation and configuration
- [Architecture](docs/ARCHITECTURE.md) - System design and data flow
- [Connector Configuration](docs/CONNECTOR_CONFIG.md) - Configuration reference
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions

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