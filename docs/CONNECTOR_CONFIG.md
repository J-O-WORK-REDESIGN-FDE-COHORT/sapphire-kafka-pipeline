# Connector Configuration Reference

This document provides detailed information about the configuration parameters used in both Kafka Connect sink connectors.

## Table of Contents

- [Device Sink Connector](#device-sink-connector)
- [Metrics Sink Connector](#metrics-sink-connector)
- [Common Configuration Parameters](#common-configuration-parameters)
- [Transform Configuration](#transform-configuration)
- [Error Handling Configuration](#error-handling-configuration)
- [Performance Tuning](#performance-tuning)

## Device Sink Connector

**File**: [`config/postgres-device-sink-connector.json`](../config/postgres-device-sink-connector.json)

### Purpose

Manages device metadata by upserting device information into a single `devices` table. This connector ensures that device records are kept up-to-date with the latest information.

### Key Configuration

```json
{
  "name": "postgres-device-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "health.metrics.heartrate,health.metrics.bloodpressure,health.metrics.glucose,health.metrics.spo2,health.metrics.activity,health.metrics.sleep,health.metrics.workout",
    "connection.url": "jdbc:postgresql://shared-postgres:5432/sapphire",
    "connection.user": "admin",
    "connection.password": "admin123",
    "auto.create": "true",
    "auto.evolve": "false",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "device_id"
  }
}
```

### Parameter Details

| Parameter | Value | Description |
|-----------|-------|-------------|
| `connector.class` | `io.confluent.connect.jdbc.JdbcSinkConnector` | Confluent JDBC sink connector class |
| `tasks.max` | `1` | Number of parallel tasks (single task for device metadata) |
| `topics` | Multiple health.metrics topics | All health metric topics to extract device info from |
| `connection.url` | JDBC URL | PostgreSQL connection string |
| `auto.create` | `true` | Automatically create the devices table if it doesn't exist |
| `auto.evolve` | `false` | Do not automatically evolve schema (controlled evolution) |
| `insert.mode` | `upsert` | Update existing records or insert new ones |
| `pk.mode` | `record_value` | Primary key extracted from record value |
| `pk.fields` | `device_id` | Field(s) used as primary key |

### Field Whitelist

Only these fields are extracted and stored:

```json
"fields.whitelist": "device_id,device_type,device_manufacturer,device_model,user_id,app_version"
```

### Transforms

#### 1. RegexRouter Transform

Routes all messages to the `devices` table regardless of source topic:

```json
"transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
"transforms.route.replacement": "devices",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter"
```

**Example**: `health.metrics.heartrate` → `devices`

#### 2. Flatten Transform

Flattens nested Avro structures:

```json
"transforms.Flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
"transforms.Flatten.delimiter": "_"
```

**Example**: `resource.attributes.device_id` → `resource_attributes_device_id`

#### 3. ReplaceField Transform

Renames flattened fields to match database schema:

```json
"transforms.Rename.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
"transforms.Rename.renames": "resource_attributes_device_id:device_id,resource_attributes_device_type:device_type,resource_attributes_device_manufacturer:device_manufacturer,resource_attributes_device_model:device_model,resource_attributes_user_id:user_id,resource_attributes_app_version:app_version"
```

## Metrics Sink Connector

**File**: [`config/postgres-sink-connector.json`](../config/postgres-sink-connector.json)

### Purpose

Handles timeseries health metrics data by inserting records into metric-specific tables. Each metric type (heartrate, glucose, etc.) gets its own table.

### Key Configuration

```json
{
  "name": "postgres-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "health.metrics.heartrate,health.metrics.bloodpressure,health.metrics.glucose,health.metrics.spo2,health.metrics.activity,health.metrics.sleep,health.metrics.workout",
    "connection.url": "jdbc:postgresql://shared-postgres:5432/sapphire",
    "connection.user": "admin",
    "connection.password": "admin123",
    "auto.create": "false",
    "auto.evolve": "false",
    "insert.mode": "insert",
    "pk.mode": "record_value",
    "pk.fields": "time,user_id,metric_name"
  }
}
```

### Parameter Details

| Parameter | Value | Description |
|-----------|-------|-------------|
| `connector.class` | `io.confluent.connect.jdbc.JdbcSinkConnector` | Confluent JDBC sink connector class |
| `tasks.max` | `1` | Number of parallel tasks |
| `topics` | Multiple health.metrics topics | All health metric topics |
| `auto.create` | `false` | Tables must be pre-created (for schema control) |
| `auto.evolve` | `false` | Schema evolution disabled |
| `insert.mode` | `insert` | Append-only mode for timeseries data |
| `pk.mode` | `record_value` | Primary key from record value |
| `pk.fields` | `time,user_id,metric_name` | Composite primary key for uniqueness |

### Field Whitelist

Comprehensive field list for metrics data:

```json
"fields.whitelist": "request_id,device_id,user_id,scope_name,scope_version,metric_name,unit,attributes,start_time,time,metric_value,ingestion_timestamp,ingestion_time,schema_version"
```

### Transforms

#### 1. RegexRouter Transform

Routes messages to metric-specific tables:

```json
"transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
"transforms.route.replacement": "$3",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter"
```

**Example**: `health.metrics.heartrate` → `heartrate`

#### 2. Flatten Transform

Same as device connector:

```json
"transforms.Flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
"transforms.Flatten.delimiter": "_"
```

#### 3. ReplaceField Transform

Maps Avro fields to database columns:

```json
"transforms.Rename.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
"transforms.Rename.renames": "resource_attributes_device_id:device_id,resource_attributes_user_id:user_id,scope_name:scope_name,scope_version:scope_version,data_points_attributes:attributes,data_points_start_time_unix_nano:start_time,data_points_time_unix_nano:time,data_points_value:metric_value"
```

#### 4. TimestampConverter Transforms

Two timestamp conversions are applied:

**ConvertEnd**: Converts ingestion timestamp from nanoseconds to SQL timestamp:

```json
"transforms.ConvertEnd.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
"transforms.ConvertEnd.field": "ingestion_timestamp",
"transforms.ConvertEnd.target.type": "Timestamp",
"transforms.ConvertEnd.unix.precision": "nanoseconds"
```

**ConvertStart**: Creates additional timestamp field:

```json
"transforms.ConvertStart.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
"transforms.ConvertStart.field": "ingestion_timestamp",
"transforms.ConvertStart.target.type": "Timestamp",
"transforms.ConvertStart.target.field": "ingestion_time",
"transforms.ConvertStart.unix.precision": "nanoseconds"
```

## Common Configuration Parameters

### Connection Settings

Both connectors share these connection parameters:

```json
"connection.url": "jdbc:postgresql://shared-postgres:5432/sapphire",
"connection.user": "admin",
"connection.password": "admin123"
```

**Security Note**: In production, use environment variables or secrets management for credentials.

### Converter Settings

Both use Avro with Schema Registry:

```json
"key.converter": "org.apache.kafka.connect.storage.StringConverter",
"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081"
```

## Error Handling Configuration

Both connectors implement comprehensive error handling:

### Error Tolerance

```json
"errors.tolerance": "all"
```

Continues processing even when errors occur. Failed records are sent to DLQ.

### Error Logging

```json
"errors.log.enable": "true",
"errors.log.include.messages": "true"
```

Logs detailed error information including the failed message content.

### Dead Letter Queue

```json
"errors.deadletterqueue.topic.name": "dlq-postgres-device-sink",
"errors.deadletterqueue.topic.replication.factor": "1",
"errors.deadletterqueue.context.headers.enable": "true"
```

Failed records are sent to a dedicated DLQ topic with context headers for debugging.

**DLQ Topics**:
- Device connector: `dlq-postgres-device-sink`
- Metrics connector: `dlq-postgres-sink`

## Performance Tuning

### Increasing Throughput

#### 1. Increase Task Parallelism

```json
"tasks.max": "3"
```

More tasks = more parallel processing. Balance with available resources.

#### 2. Batch Size Configuration

```json
"batch.size": "3000"
```

Larger batches improve throughput but increase memory usage.

#### 3. Connection Pool Settings

```json
"connection.pool.size": "10",
"connection.pool.timeout.ms": "30000"
```

### Optimizing for Latency

#### 1. Reduce Batch Size

```json
"batch.size": "100"
```

Smaller batches reduce latency but may decrease throughput.

#### 2. Flush Interval

```json
"flush.interval.ms": "1000"
```

More frequent flushes reduce end-to-end latency.

### Memory Optimization

#### 1. Limit Buffer Size

```json
"buffer.count.records": "10000"
```

Prevents excessive memory usage during high load.

## Configuration Best Practices

### 1. Security

- Use secrets management for credentials
- Enable SSL/TLS for database connections
- Restrict connector permissions to minimum required

### 2. Monitoring

- Enable JMX metrics
- Monitor connector lag
- Track DLQ message counts
- Alert on connector failures

### 3. Schema Management

- Keep `auto.evolve` disabled in production
- Use explicit schema versions
- Test schema changes in non-production first

### 4. Error Handling

- Always configure DLQ topics
- Monitor DLQ for patterns
- Set up alerts for DLQ message spikes
- Implement DLQ replay mechanisms

### 5. Performance

- Start with conservative settings
- Monitor resource usage
- Tune based on actual workload
- Test changes in non-production

## Configuration Examples

### High Throughput Configuration

```json
{
  "tasks.max": "5",
  "batch.size": "5000",
  "connection.pool.size": "15",
  "flush.interval.ms": "10000"
}
```

### Low Latency Configuration

```json
{
  "tasks.max": "2",
  "batch.size": "100",
  "flush.interval.ms": "500"
}
```

### Production-Ready Configuration

```json
{
  "tasks.max": "3",
  "batch.size": "2000",
  "connection.pool.size": "10",
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq-connector",
  "errors.deadletterqueue.topic.replication.factor": "3",
  "connection.url": "${env:DB_URL}",
  "connection.user": "${env:DB_USER}",
  "connection.password": "${env:DB_PASSWORD}"
}
```

## Updating Configuration

### Via REST API

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  --data @config/postgres-sink-connector.json \
  http://localhost:8083/connectors/postgres-sink-connector/config
```

### Via Configuration File

1. Update the JSON configuration file
2. Delete the existing connector
3. Redeploy with updated configuration

```bash
curl -X DELETE http://localhost:8083/connectors/postgres-sink-connector
curl -X POST \
  -H "Content-Type: application/json" \
  --data @config/postgres-sink-connector.json \
  http://localhost:8083/connectors
```

## Validation

### Validate Configuration Before Deployment

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  --data @config/postgres-sink-connector.json \
  http://localhost:8083/connector-plugins/JdbcSinkConnector/config/validate
```

This returns validation results without actually deploying the connector.

## References

- [Confluent JDBC Sink Connector Documentation](https://docs.confluent.io/kafka-connect-jdbc/current/sink-connector/index.html)
- [Kafka Connect Transforms](https://kafka.apache.org/documentation/#connect_transforms)
- [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)