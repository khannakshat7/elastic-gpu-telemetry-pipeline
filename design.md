# Elastic GPU Telemetry Pipeline - Design Document

## Requirements Summary

### Functional Requirements

#### 1. Telemetry Streamer
- Read telemetry from CSV file
- Stream telemetry data periodically over custom message queue
- Support dynamic scale up/down of streamer instances
- Use ingestion time as timestamp (ignore CSV timestamp column)
- Loop CSV data to simulate continuous stream of telemetry

#### 2. Telemetry Collector
- Consume telemetry from custom message queue
- Parse telemetry data
- Persist telemetry to storage
- Support dynamic scale up/down of collector instances

#### 3. Custom Message Queue
- Custom implementation (no ZeroMQ, RabbitMQ, Kafka, etc.)
- Connect streamers with collectors
- Designed for scale, performance, and availability
- Support up to 10 streamer/collector instances

#### 4. API Gateway
- REST API exposing telemetry data
- Auto-generate OpenAPI specification
- **Endpoints:**
  - `GET /api/v1/gpus` - List all GPUs
  - `GET /api/v1/gpus/{id}/telemetry` - Get telemetry for specific GPU
  - Support optional `start_time` and `end_time` query parameters (inclusive)

### Non-Functional Requirements

1. **Scalability**: Support up to 10 streamer/collector instances
2. **Deployment**: Kubernetes with Helm charts
3. **Testing**: Unit tests required; system tests bonus
4. **Code Coverage**: Measure and display via Makefile
5. **OpenAPI**: Auto-generate spec via Makefile command
6. **Observability**: Logging and error handling
7. **Code Quality**: Clean, idiomatic Go; well-documented
8. **AI Documentation**: Document all prompts and where manual intervention was needed

## Architecture Diagram
![GPU Telemetry Pipeline](Elastic-gpu-pipeline.jpg)

## Main Components

1. **Telemetry Streamer Service** (`cmd/streamer/`)
2. **Telemetry Collector Service** (`cmd/collector/`)
3. **Custom Message Queue** (library or service)
4. **API Gateway Service** (`cmd/api-gateway/`)
5. **Queue Service** (if queue is a separate service) (`cmd/queue-service/`)
6. **Data Storage Layer** (for collector persistence)

## Key Entities / Domain Objects

### 1. GPU Entity
- **UUID** (unique identifier)
- **GPU ID** (per-host index)
- **Device name**
- **Model name**
- **Hostname**

### 2. Telemetry Record
- **GPU UUID** (foreign key to GPU)
- **Metric name**
- **Metric value**
- **Ingestion timestamp** (system time when processed)
- **Optional**: container, pod, namespace (from CSV)

### 3. Message Queue Message
- Raw CSV line or parsed telemetry data
- Metadata (source streamer ID, sequence number, etc.)

## CSV Column Analysis

| Column Name | Description | Data Type | Notes |
|------------|-------------|-----------|-------|
| `timestamp` | Original telemetry timestamp | ISO 8601 datetime | **IGNORED** - use ingestion time instead |
| `metric_name` | DCGM metric identifier | String | e.g., "DCGM_FI_DEV_GPU_UTIL" |
| `gpu_id` | GPU index on host | String/Integer | Per-host identifier (0, 1, 2, etc.) |
| `device` | Device name | String | e.g., "nvidia0", "nvidia1" |
| `uuid` | Unique GPU identifier | UUID String | **Globally unique** across all hosts |
| `modelName` | GPU model | String | e.g., "NVIDIA H100 80GB HBM3" |
| `Hostname` | Host machine identifier | String | e.g., "mtv5-dgx1-hgpu-031" |
| `container` | Container identifier | String | Often empty in sample data |
| `pod` | Kubernetes pod identifier | String | Often empty in sample data |
| `namespace` | Kubernetes namespace | String | Often empty in sample data |
| `value` | Metric value | String/Number | Numeric value as string |
| `labels_raw` | Raw Prometheus-style labels | String | Additional metadata in key-value format |

### Metric Types Observed
- `DCGM_FI_DEV_GPU_UTIL` - GPU utilization percentage
- `DCGM_FI_DEV_DEC_UTIL` - Decoder utilization
- `DCGM_FI_DEV_ENC_UTIL` - Encoder utilization
- `DCGM_FI_DEV_FB_FREE` - Frame buffer free memory
- `DCGM_FI_DEV_FB_USED` - Frame buffer used memory
- `DCGM_FI_DEV_GPU_TEMP` - GPU temperature
- `DCGM_FI_DEV_MEM_CLOCK` - Memory clock speed
- `DCGM_FI_DEV_MEM_COPY_UTIL` - Memory copy utilization
- `DCGM_FI_DEV_POWER_USAGE` - Power consumption
- `DCGM_FI_DEV_SM_CLOCK` - SM (Streaming Multiprocessor) clock speed

### Data Characteristics
- **247 unique GPUs** (by UUID)
- **31 unique hosts**
- **10 different metric types**
- Each metric appears once per GPU in the sample dataset
- Total records: ~2,471 (247 GPUs × 10 metrics)

## Unique/Composite Key Analysis

### For GPU Entity
- **Primary Key**: `uuid` (globally unique)
- **Alternative composite key**: `(Hostname, gpu_id)` (unique per host)
- **Recommendation**: Use `uuid` as primary key

### For Telemetry Record
- **Composite key**: `(uuid, metric_name, ingestion_timestamp)`
  - `uuid`: Identifies the GPU
  - `metric_name`: Identifies the metric type
  - `ingestion_timestamp`: Time when record was processed (replaces CSV timestamp)
- **Rationale**:
  - Same GPU can have multiple metrics at the same time
  - Same GPU+metric can have multiple readings over time
  - Ingestion timestamp ensures uniqueness even if CSV is looped

## Assumptions

1. **Storage**: Use a relational database (PostgreSQL) or document store (MongoDB) for persistence
2. **Queue design**: Implement as a service (separate process) rather than embedded library for better scalability
3. **Queue protocol**: Use gRPC or HTTP for queue operations
4. **Queue persistence**: In-memory with optional persistence for durability
5. **API GPU ID**: Use `uuid` as the `{id}` in `/api/v1/gpus/{id}/telemetry`
6. **Time format**: ISO 8601 for API time parameters
7. **CSV looping**: Streamers loop the CSV file continuously to simulate real-time data
8. **Concurrency**: Multiple streamers can read the same CSV file concurrently
9. **Message format**: JSON for queue messages (parsed from CSV)
10. **Error handling**: Failed messages should be logged; retry logic may be needed
11. **Health checks**: All services should expose health check endpoints for Kubernetes
12. **Configuration**: Use environment variables or config files for service configuration

## System Architecture Summary

The system is a distributed telemetry pipeline:

1. **Streamers** read CSV data and publish telemetry messages to a custom message queue
2. **Message Queue** buffers and routes messages from streamers to collectors
3. **Collectors** consume messages, parse them, and persist to storage
4. **API Gateway** queries storage and exposes REST endpoints for telemetry data

All components are containerized and deployable to Kubernetes via Helm, supporting horizontal scaling up to 10 instances per component.

---

## High-Level Architecture

### Architecture Overview

The system follows a **microservices architecture** with clear separation of concerns. Each component is independently scalable and communicates through well-defined interfaces.

### Main Components

1. **Telemetry Streamer(s)** - Stateless service that reads CSV and publishes to queue
2. **Telemetry Collector(s)** - Stateless service that consumes from queue and persists
3. **Custom Message Queue** - Central messaging service (HTTP/gRPC based)
4. **Storage Abstraction** - Interface-based storage layer (in-memory → PostgeSQL)
5. **API Gateway** - HTTP service exposing REST endpoints

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Cluster                          │
│                                                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │   Streamer   │      │   Streamer   │      │   Streamer   │     │
│  │  Instance 1  │      │  Instance 2  │      │  Instance N  │     │
│  └──────┬───────┘      └──────┬───────┘      └──────┬───────┘     │
│         │                      │                      │             │
│         └──────────────────────┼──────────────────────┘             │
│                                │                                    │
│                         ┌──────▼──────┐                             │
│                         │   Message   │                             │
│                         │    Queue    │                             │
│                         │   Service   │                             │
│                         └──────┬──────┘                             │
│                                │                                    │
│         ┌──────────────────────┼──────────────────────┐             │
│         │                      │                      │             │
│  ┌──────▼───────┐      ┌──────▼───────┐      ┌──────▼───────┐     │
│  │  Collector   │      │  Collector    │      │  Collector   │     │
│  │  Instance 1  │      │  Instance 2  │      │  Instance N  │     │
│  └──────┬───────┘      └──────┬───────┘      └──────┬───────┘     │
│         │                      │                      │             │
│         └──────────────────────┼──────────────────────┘             │
│                                │                                    │
│                         ┌──────▼──────┐                             │
│                         │   Storage   │                             │
│                         │  (In-Memory │                             │
│                         │   /Postgres)│                             │
│                         └──────┬──────┘                             │
│                                │                                    │
│                         ┌──────▼──────┐                             │
│                         │ API Gateway │                             │
│                         │  (HTTP REST)│                             │
│                         └──────┬──────┘                             │
│                                │                                    │
└────────────────────────────────┼────────────────────────────────────┘
                                 │
                         ┌───────▼────────┐
                         │  API Clients   │
                         │  (External)    │
                         └────────────────┘
```

### Sequence Diagram

```
CSV File    Streamer      Queue        Collector     Storage      API Client
   │           │            │              │            │              │
   │           │            │              │            │              │
   │──read────>│            │              │            │              │
   │           │            │              │            │              │
   │           │──publish──>│              │            │              │
   │           │  (JSON)    │              │            │              │
   │           │            │              │            │              │
   │           │            │──consume────>│            │              │
   │           │            │  (JSON)      │            │              │
   │           │            │              │            │              │
   │           │            │              │──parse────>│              │
   │           │            │              │            │              │
   │           │            │              │──persist──>│              │
   │           │            │              │            │              │
   │           │            │              │            │              │
   │           │            │              │            │<──query──────│
   │           │            │              │            │              │
   │           │            │              │            │──response───>│
   │           │            │              │            │  (JSON)      │
   │           │            │              │            │              │
```

### Package/Module Layout

```
elastic-gpu-telemetry-pipeline/
├── cmd/
│   ├── streamer/          # Streamer service entry point
│   │   └── main.go
│   ├── collector/         # Collector service entry point
│   │   └── main.go
│   ├── api-gateway/       # API Gateway service entry point
│   │   └── main.go
│   └── queue-service/     # Message Queue service entry point
│       └── main.go
│
├── pkg/
│   ├── domain/            # Domain entities and interfaces
│   │   ├── gpu.go         # GPU entity
│   │   ├── telemetry.go   # Telemetry record entity
│   │   └── message.go     # Queue message entity
│   │
│   ├── mq/                # Message Queue implementation
│   │   ├── client.go      # Queue client interface
│   │   ├── server.go      # Queue server implementation
│   │   ├── publisher.go   # Publisher interface/impl
│   │   ├── subscriber.go  # Subscriber interface/impl
│   │   └── broker.go      # Message broker (in-memory)
│   │
│   ├── storage/           # Storage abstraction layer
│   │   ├── interface.go  # Storage interface
│   │   ├── memory/        # In-memory implementation
│   │       └── store.go
│   │   
│   │
│   ├── telemetry/         # Telemetry processing
│   │   ├── parser.go      # CSV parser
│   │   ├── validator.go   # Data validator
│   │   └── transformer.go # Data transformer
│   │
│   ├── api/               # API Gateway handlers
│   │   ├── handlers.go    # HTTP handlers
│   │   ├── routes.go      # Route definitions
│   │   └── middleware.go # Middleware (logging, etc.)
│   │
│   ├── config/            # Configuration management
│   │   └── config.go
│   │
│   └── utils/             # Shared utilities
│       ├── logger.go
│       └── errors.go
│
├── internal/              # Internal packages (not exported)
│   └── health/            # Health check handlers
│
├── docs/                  # Documentation
│   └── design.md
│
├── deployments/           # Deployment configurations
│   └── helm/              # Helm charts
│
├── scripts/               # Build/test scripts
│
├── go.mod
├── go.sum
└── Makefile
```

## Design Principles & Patterns

### SOLID Principles Application

#### 1. Single Responsibility Principle (SRP)
- **Streamer**: Only responsible for reading CSV and publishing to queue
- **Collector**: Only responsible for consuming from queue and persisting
- **Queue Service**: Only responsible for message routing and buffering
- **API Gateway**: Only responsible for HTTP request handling and response
- **Storage**: Only responsible for data persistence

#### 2. Open/Closed Principle (OCP)
- **Storage Interface**: Open for extension (Memory → PostgreSQL) without modifying existing code
- **Queue Client Interface**: Open for different queue implementations
- **Parser Interface**: Open for different data format parsers (CSV → JSON, etc.)

#### 3. Liskov Substitution Principle (LSP)
- Any storage implementation (Memory, Postgres) can substitute the storage interface
- Any queue client implementation can substitute the queue client interface
- Any parser implementation can substitute the parser interface

#### 4. Interface Segregation Principle (ISP)
- Separate interfaces for:
  - `Publisher` (publish only)
  - `Subscriber` (subscribe only)
  - `StorageReader` (read operations)
  - `StorageWriter` (write operations)
  - `Storage` (combines reader + writer)

#### 5. Dependency Inversion Principle (DIP)
- High-level modules (Streamer, Collector) depend on abstractions (interfaces)
- Low-level modules (MemoryStorage, QueueClient) implement interfaces
- Dependency injection via constructors/factories

### Design Patterns

#### 1. Repository Pattern
**Location**: `pkg/storage/interface.go`
- Abstracts data access layer
- Allows swapping storage backends without changing business logic
- Example:
  ```go
  type Repository interface {
      SaveTelemetry(ctx context.Context, record *TelemetryRecord) error
      GetTelemetryByGPU(ctx context.Context, gpuUUID string, start, end time.Time) ([]*TelemetryRecord, error)
      ListGPUs(ctx context.Context) ([]*GPU, error)
  }
  ```

#### 2. Factory Pattern
**Location**: `pkg/storage/` and `pkg/mq/`
- Factory functions to create storage instances
- Factory functions to create queue clients
- Example:
  ```go
  func NewStorage(backend string, config Config) (Repository, error) {
      switch backend {
      case "memory":
          return memory.NewStore(), nil
      case "postgres":
          return postgres.NewStore(config.Postgres), nil
      default:
          return nil, ErrUnknownBackend
      }
  }
  ```

#### 3. Strategy Pattern
**Location**: `pkg/telemetry/parser.go`
- Different parsing strategies (CSV, JSON, etc.)
- Different validation strategies
- Example:
  ```go
  type Parser interface {
      Parse(data []byte) (*TelemetryRecord, error)
  }
  
  type CSVParser struct{}
  type JSONParser struct{}
  ```

#### 4. Publisher-Subscriber Pattern
**Location**: `pkg/mq/`
- Queue service implements pub/sub pattern
- Streamers are publishers
- Collectors are subscribers
- Decouples producers from consumers

#### 5. Observer Pattern
**Location**: `pkg/mq/subscriber.go`
- Collectors observe queue for new messages
- Can have multiple observers (collectors) for same queue
- Queue notifies all subscribers when messages arrive

#### 6. Adapter Pattern
**Location**: `pkg/storage/memory/`
- Adapters convert between domain models and storage models
- Memory adapter converts domain entities to in-memory structures

#### 7. Dependency Injection
**Location**: All `cmd/*/main.go` files
- Services receive dependencies via constructors
- Enables easy testing with mocks
- Example:
  ```go
  func NewStreamer(csvPath string, queueClient mq.Publisher, logger Logger) *Streamer
  ```

## Scalability & Extensibility Design

### Supporting Multiple Streamer/Collector Instances

#### 1. Stateless Design
- **Streamers**: No shared state, can run multiple instances
- **Collectors**: No shared state, can run multiple instances
- Each instance processes independently

#### 2. Queue-Based Load Distribution
- Queue service distributes messages across collector instances
- Round-robin or work-stealing distribution
- Each collector instance processes messages independently

#### 3. Concurrent Processing
- Streamers use goroutines for concurrent CSV reading
- Collectors use worker pools for concurrent message processing
- Queue service handles concurrent publish/subscribe operations

#### 4. Horizontal Scaling
- Kubernetes HPA (Horizontal Pod Autoscaler) can scale based on:
  - Queue depth (for collectors)
  - CPU/Memory usage
  - Custom metrics

### Scaling Up to ~10 Instances

#### Queue Service Design
- **In-memory broker** with mutex-protected data structures
- **Channel-based** message distribution for Go concurrency
- **Connection pooling** for HTTP/gRPC clients
- **Bounded queues** to prevent memory overflow

#### Performance Considerations
- Queue uses buffered channels for message buffering
- Batch processing in collectors (process N messages at once)
- Connection pooling for storage operations
- Efficient data structures (maps for O(1) lookups)

#### Resource Management
- Memory limits per pod
- CPU limits per pod
- Graceful shutdown handling
- Health checks for Kubernetes liveness/readiness probes

### Storage Backend Abstraction

#### Interface-Based Design
```go
// pkg/storage/interface.go
type Repository interface {
    // GPU operations
    SaveGPU(ctx context.Context, gpu *GPU) error
    GetGPU(ctx context.Context, uuid string) (*GPU, error)
    ListGPUs(ctx context.Context) ([]*GPU, error)
    
    // Telemetry operations
    SaveTelemetry(ctx context.Context, record *TelemetryRecord) error
    GetTelemetryByGPU(ctx context.Context, gpuUUID string, start, end time.Time) ([]*TelemetryRecord, error)
}
```

#### Current Implementation: In-Memory
- Fast for development/testing
- Simple key-value store (map-based)
- No persistence (data lost on restart)

#### Future Implementation: Postgres
- Horizontal scaling support
- Persistent storage
- Indexed queries for performance

#### Migration Path
1. Start with in-memory storage
2. Implement postgres adapter implementing same interface
3. Switch via configuration/environment variable
4. No code changes in Streamer/Collector/API Gateway

### Observability & Logging

#### Structured Logging
**Location**: `pkg/utils/logger.go`
- Structured JSON logging
- Log levels (DEBUG, INFO, WARN, ERROR)
- Contextual logging (request IDs, trace IDs)

#### Metrics (Future)
- Prometheus metrics endpoint
- Metrics for:
  - Messages published/consumed
  - Processing latency
  - Error rates
  - Queue depth
  - Storage operation latency

#### Distributed Tracing (Future)
- OpenTelemetry integration
- Trace requests across services
- Identify bottlenecks

#### Health Checks
**Location**: `internal/health/`
- `/health` endpoint for liveness
- `/ready` endpoint for readiness
- Kubernetes integration

#### Error Handling
- Structured error types
- Error wrapping with context
- Error logging with stack traces
- Retry logic for transient failures

## Component Interaction Details

### Streamer → Queue
- **Protocol**: HTTP REST or gRPC
- **Message Format**: JSON
- **Method**: POST /api/v1/messages
- **Payload**: 
  ```json
  {
    "gpu_uuid": "GPU-xxx",
    "metric_name": "DCGM_FI_DEV_GPU_UTIL",
    "value": "100",
    "metadata": {...}
  }
  ```

### Queue → Collector
- **Protocol**: HTTP REST or gRPC
- **Method**: GET /api/v1/messages (polling) or WebSocket (push)
- **Response**: Array of messages
- **Acknowledgment**: POST /api/v1/messages/ack

### Collector → Storage
- **Interface**: `pkg/storage.Repository`
- **Operations**: SaveTelemetry, SaveGPU
- **Transaction**: Batch writes for performance

### API Gateway → Storage
- **Interface**: `pkg/storage.Repository`
- **Operations**: ListGPUs, GetTelemetryByGPU
- **Caching**: Optional Redis cache for frequently accessed data

## Configuration Management

### Environment Variables
- `QUEUE_SERVICE_URL` - Queue service endpoint
- `STORAGE_BACKEND` - "memory" or "postgres"
- `STORAGE_URI` - Storage connection string
- `CSV_FILE_PATH` - Path to CSV file
- `LOG_LEVEL` - Logging level
- `API_PORT` - API Gateway port

### Configuration File (Optional)
- YAML/JSON config file
- Environment-specific configs (dev, staging, prod)
- Secrets management via Kubernetes secrets

## Testing Strategy

### Unit Tests
- Test each package independently
- Mock dependencies (storage, queue client)
- High code coverage target (>80%)

### Integration Tests
- Test component interactions
- Test with real in-memory storage
- Test queue publish/subscribe flow

### System Tests (Bonus)
- End-to-end tests
- Test with multiple streamer/collector instances
- Load testing

## Deployment Architecture

### Kubernetes Deployment
- Each service as separate Deployment
- Service objects for service discovery
- ConfigMaps for configuration
- Secrets for sensitive data
- Helm charts for easy deployment

### Service Discovery
- Kubernetes DNS-based service discovery
- Services: `queue-service`, `api-gateway`
- Internal cluster communication

### Scaling
- HorizontalPodAutoscaler for auto-scaling
- Manual scaling via `kubectl scale`
- Resource limits and requests defined
