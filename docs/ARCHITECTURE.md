# Architecture Documentation

This document describes the architecture, data flow, and design decisions of the Sapphire Kafka Pipeline.

## Table of Contents

- [System Overview](#system-overview)
- [Architecture Diagram](#architecture-diagram)
- [Components](#components)
- [Data Flow](#data-flow)
- [Design Decisions](#design-decisions)
- [Scalability Considerations](#scalability-considerations)
- [Security Architecture](#security-architecture)

## System Overview

The Sapphire Kafka Pipeline is a real-time data ingestion system that streams health telemetry data from Kafka topics to a PostgreSQL timeseries database. The system uses Kafka Connect with two specialized sink connectors to handle different aspects of the data:

1. **Device metadata management** - Tracks device information
2. **Timeseries metrics storage** - Stores health measurements over time

### Key Characteristics

- **Real-time Processing**: Sub-second latency from Kafka to database
- **Schema Evolution**: Avro-based schema management with Schema Registry
- **Fault Tolerance**: Dead letter queue handling for failed records
- **Scalability**: Horizontal scaling through task parallelism
- **Data Integrity**: Upsert for devices, append-only for metrics

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Producers                            │
│  (Mobile Apps, Wearables, IoT Devices, Health Monitoring Apps)  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Publish Messages
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Kafka Cluster                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Topics:                                                  │  │
│  │  • health.metrics.heartrate                              │  │
│  │  • health.metrics.bloodpressure                          │  │
│  │  • health.metrics.glucose                                │  │
│  │  • health.metrics.spo2                                   │  │
│  │  • health.metrics.activity                               │  │
│  │  • health.metrics.sleep                                  │  │
│  │  • health.metrics.workout                                │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Consume Messages
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Schema Registry                             │
│              (Avro Schema Validation & Evolution)                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Validate & Deserialize
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Kafka Connect                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Device Sink Connector                                    │  │
│  │  • Extracts device metadata                              │  │
│  │  • Upserts to devices table                              │  │
│  │  • Transforms: Flatten, Rename, Route                    │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Metrics Sink Connector                                   │  │
│  │  • Processes timeseries data                             │  │
│  │  • Inserts to metric-specific tables                     │  │
│  │  • Transforms: Flatten, Rename, Route, Timestamp         │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Write Data
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PostgreSQL Database                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  devices (Device Metadata)                                │  │
│  │  • device_id (PK)                                        │  │
│  │  • device_type, manufacturer, model                      │  │
│  │  • user_id, app_version                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Timeseries Tables (heartrate, glucose, etc.)            │  │
│  │  • time, user_id, metric_name (Composite PK)            │  │
│  │  • metric_value, device_id, attributes                   │  │
│  │  • ingestion_timestamp, scope info                       │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘

                             │
                             │ Error Handling
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Dead Letter Queues                            │
│  • dlq-postgres-device-sink                                      │
│  • dlq-postgres-sink                                             │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Kafka Cluster

**Purpose**: Message broker for health telemetry data

**Topics**:
- `health.metrics.heartrate` - Heart rate measurements
- `health.metrics.bloodpressure` - Blood pressure readings
- `health.metrics.glucose` - Blood glucose levels
- `health.metrics.spo2` - Blood oxygen saturation
- `health.metrics.activity` - Physical activity data
- `health.metrics.sleep` - Sleep tracking data
- `health.metrics.workout` - Workout session data

**Configuration**:
- Replication factor: Configurable (typically 3 for production)
- Partitions: Multiple for parallel processing
- Retention: Based on business requirements

### 2. Schema Registry

**Purpose**: Centralized schema management and validation

**Responsibilities**:
- Store and version Avro schemas
- Validate message schemas on publish
- Enable schema evolution
- Provide schema compatibility checks

**Integration**:
- Producers register schemas before publishing
- Consumers retrieve schemas for deserialization
- Kafka Connect uses for Avro conversion

### 3. Kafka Connect

**Purpose**: Scalable, reliable data integration framework

**Deployment Mode**: Distributed (recommended for production)

**Connectors**:

#### Device Sink Connector
- **Class**: `io.confluent.connect.jdbc.JdbcSinkConnector`
- **Purpose**: Manage device metadata
- **Mode**: Upsert (idempotent updates)
- **Target**: Single `devices` table
- **Key Field**: `device_id`

#### Metrics Sink Connector
- **Class**: `io.confluent.connect.jdbc.JdbcSinkConnector`
- **Purpose**: Store timeseries metrics
- **Mode**: Insert (append-only)
- **Target**: Multiple metric-specific tables
- **Key Fields**: `time`, `user_id`, `metric_name`

### 4. PostgreSQL Database

**Purpose**: Persistent storage for health data

**Schema Design**:

#### Devices Table
```sql
devices (
    device_id VARCHAR PRIMARY KEY,
    device_type VARCHAR,
    device_manufacturer VARCHAR,
    device_model VARCHAR,
    user_id VARCHAR,
    app_version VARCHAR
)
```

**Characteristics**:
- Normalized device information
- Upsert pattern for updates
- Indexed on user_id for lookups

#### Metrics Tables
```sql
{metric_name} (
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
)
```

**Characteristics**:
- Timeseries optimized (consider TimescaleDB)
- Composite primary key for uniqueness
- Indexed on time for range queries
- JSONB for flexible attributes

### 5. Dead Letter Queues

**Purpose**: Capture and isolate failed records

**Topics**:
- `dlq-postgres-device-sink` - Device connector failures
- `dlq-postgres-sink` - Metrics connector failures

**Usage**:
- Monitor for error patterns
- Replay after fixing issues
- Alert on threshold breaches

## Data Flow

### 1. Message Production

```
Device/App → Kafka Producer → Avro Serialization → Schema Registry → Kafka Topic
```

**Steps**:
1. Health data collected by device/app
2. Data serialized to Avro format
3. Schema validated against Schema Registry
4. Message published to appropriate topic

### 2. Message Consumption (Device Connector)

```
Kafka Topic → Kafka Connect → Avro Deserialization → Transforms → PostgreSQL (devices)
```

**Transform Pipeline**:
1. **Flatten**: `resource.attributes.device_id` → `resource_attributes_device_id`
2. **Rename**: `resource_attributes_device_id` → `device_id`
3. **Route**: All topics → `devices` table
4. **Upsert**: Insert or update based on `device_id`

### 3. Message Consumption (Metrics Connector)

```
Kafka Topic → Kafka Connect → Avro Deserialization → Transforms → PostgreSQL (metric tables)
```

**Transform Pipeline**:
1. **Flatten**: Nested structures flattened with `_` delimiter
2. **Rename**: Map Avro fields to database columns
3. **Route**: `health.metrics.heartrate` → `heartrate` table
4. **Timestamp Convert**: Nanosecond Unix → SQL Timestamp
5. **Insert**: Append to timeseries table

### 4. Error Handling Flow

```
Failed Record → Error Handler → Dead Letter Queue → Monitoring/Alerting
```

**Process**:
1. Record processing fails (schema mismatch, constraint violation, etc.)
2. Error logged with full context
3. Record sent to DLQ with error headers
4. Connector continues processing
5. DLQ monitored for patterns
6. Failed records replayed after fix

## Design Decisions

### 1. Dual Connector Architecture

**Decision**: Use separate connectors for devices and metrics

**Rationale**:
- **Separation of Concerns**: Device metadata vs. timeseries data
- **Different Write Patterns**: Upsert vs. append-only
- **Independent Scaling**: Scale each based on load
- **Failure Isolation**: Issues in one don't affect the other

**Trade-offs**:
- More complex deployment
- Duplicate topic consumption
- Higher resource usage

### 2. Upsert for Devices, Insert for Metrics

**Decision**: Device connector uses upsert, metrics uses insert

**Rationale**:
- **Devices**: Latest information always current (idempotent)
- **Metrics**: Historical data preserved (immutable)
- **Data Integrity**: No duplicate metrics, current device info

### 3. Topic-to-Table Routing

**Decision**: Route by topic name using regex

**Rationale**:
- **Automatic Routing**: No manual configuration per metric
- **Scalability**: Easy to add new metric types
- **Consistency**: Predictable table names

**Pattern**: `health.metrics.{metric}` → `{metric}` table

### 4. Composite Primary Key for Metrics

**Decision**: Use (time, user_id, metric_name) as primary key

**Rationale**:
- **Uniqueness**: Prevents duplicate measurements
- **Query Optimization**: Efficient time-range queries
- **Data Integrity**: Enforces one measurement per user per time

### 5. Avro with Schema Registry

**Decision**: Use Avro serialization with centralized schema management

**Rationale**:
- **Schema Evolution**: Backward/forward compatibility
- **Validation**: Catch errors at publish time
- **Efficiency**: Compact binary format
- **Documentation**: Schema serves as contract

### 6. Error Tolerance with DLQ

**Decision**: Continue processing on errors, send failures to DLQ

**Rationale**:
- **Availability**: Don't block pipeline on single failures
- **Observability**: Centralized error tracking
- **Recovery**: Replay capability after fixes
- **Debugging**: Full context preserved

## Scalability Considerations

### Horizontal Scaling

**Kafka Connect**:
- Deploy multiple workers in distributed mode
- Increase `tasks.max` for parallel processing
- Balance tasks across workers automatically

**Kafka**:
- Increase topic partitions
- Add more brokers to cluster
- Scale consumer groups

**PostgreSQL**:
- Read replicas for query load
- Partitioning for large tables (TimescaleDB)
- Connection pooling (PgBouncer)

### Vertical Scaling

**Kafka Connect**:
- Increase heap size for larger batches
- More CPU cores for parallel tasks
- Faster storage for local state

**PostgreSQL**:
- More memory for caching
- Faster storage (SSD/NVMe)
- More CPU for query processing

### Performance Optimization

**Batch Processing**:
```json
"batch.size": "3000",
"flush.interval.ms": "10000"
```

**Connection Pooling**:
```json
"connection.pool.size": "10"
```

**Task Parallelism**:
```json
"tasks.max": "5"
```

## Security Architecture

### Data in Transit

- **Kafka**: SSL/TLS encryption
- **Schema Registry**: HTTPS
- **PostgreSQL**: SSL connections

### Authentication

- **Kafka**: SASL (PLAIN, SCRAM, or Kerberos)
- **Schema Registry**: Basic auth or OAuth
- **PostgreSQL**: Password or certificate-based

### Authorization

- **Kafka**: ACLs for topic access
- **Schema Registry**: Schema-level permissions
- **PostgreSQL**: Role-based access control

### Secrets Management

- Use environment variables for credentials
- Integrate with secrets managers (Vault, AWS Secrets Manager)
- Rotate credentials regularly

### Network Security

- Private network for internal communication
- Firewall rules for external access
- VPN for remote administration

## Monitoring and Observability

### Key Metrics

**Kafka Connect**:
- Connector state (RUNNING/FAILED)
- Task state and error count
- Records processed per second
- Lag between Kafka and sink

**Kafka**:
- Consumer lag
- Message throughput
- Partition distribution

**PostgreSQL**:
- Connection count
- Query performance
- Table sizes
- Index usage

### Logging

- Structured logging (JSON format)
- Centralized log aggregation (ELK, Splunk)
- Log levels: ERROR, WARN, INFO, DEBUG
- Correlation IDs for tracing

### Alerting

- Connector failures
- High error rates
- DLQ message spikes
- Database connection issues
- Disk space warnings

## Disaster Recovery

### Backup Strategy

**Kafka**:
- Topic replication (factor ≥ 3)
- Regular offset backups
- Mirror clusters for DR

**PostgreSQL**:
- Daily full backups
- Continuous WAL archiving
- Point-in-time recovery capability
- Backup retention policy

### Recovery Procedures

1. **Connector Failure**: Restart connector, replay from last offset
2. **Database Failure**: Restore from backup, replay Kafka messages
3. **Data Corruption**: Identify bad records in DLQ, fix and replay
4. **Complete Outage**: Failover to DR site, resume from checkpoints

## Future Enhancements

### Potential Improvements

1. **Stream Processing**: Add Kafka Streams for real-time analytics
2. **Data Lake Integration**: Archive to S3/HDFS for long-term storage
3. **Multi-Region**: Deploy across regions for global availability
4. **Machine Learning**: Real-time anomaly detection on metrics
5. **API Layer**: REST API for querying historical data
6. **Visualization**: Grafana dashboards for metrics monitoring

### Scalability Roadmap

1. **Phase 1**: Current architecture (single region, moderate load)
2. **Phase 2**: Multi-region deployment with replication
3. **Phase 3**: Hybrid storage (hot/warm/cold tiers)
4. **Phase 4**: Real-time analytics and ML integration