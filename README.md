# 🚀 Elastic GPU Telemetry Pipeline

A scalable, production-ready telemetry pipeline for AI clusters that collects, processes, and exposes GPU telemetry data through a custom message queue architecture.

## 📋 Table of Contents

- 🎯 [Overview](#-overview)
- 🏗️ [Architecture](#️-architecture)
- ✨ [Key Features](#-key-features)
- 📦 [Prerequisites](#-prerequisites)
- 🚀 [Quick Start](#-quick-start)
  - [Local Development (No Kubernetes)](#local-development-no-kubernetes)
  - [Kubernetes Deployment (kind)](#kubernetes-deployment-kind)
- 🔨 [Build and Packaging](#-build-and-packaging)
- 📚 [API Documentation](#-api-documentation)
- 🧪 [Testing](#-testing)
- ⚙️ [Configuration](#️-configuration)
- 🔧 [Troubleshooting](#-troubleshooting)
- 🤖 [AI Assistance](#-ai-assistance)

## 🎯 Overview

The Elastic GPU Telemetry Pipeline is designed to handle telemetry data from AI clusters containing multiple hosts, each potentially hosting multiple GPUs. The system processes GPU metrics (utilization, temperature, memory, power, etc.) through a distributed pipeline that supports horizontal scaling.

### What This System Does

1. **📥 Ingests** GPU telemetry data from CSV files (simulating real-time streams)
2. **⚙️ Processes** telemetry through a custom message queue
3. **💾 Stores** processed data in a queryable storage layer
4. **🌐 Exposes** telemetry data via RESTful APIs with OpenAPI documentation

### Key Design Principles

- **📈 Scalability**: Support up to 10 instances of streamers and collectors
- **🔌 Extensibility**: Pluggable storage backends (in-memory → PostgreSQL)
- **👁️ Observability**: Comprehensive logging and error handling
- **🏭 Production-Ready**: Kubernetes-native with Helm charts
- **🏗️ Clean Architecture**: SOLID principles, repository pattern, dependency injection

## 🏗️ Architecture

### System Components

The pipeline consists of five main services:

1. **📡 Telemetry Streamer** (`cmd/streamer/`)
   - Reads telemetry data from CSV files
   - Publishes messages to the message queue
   - Supports multiple concurrent instances
   - Loops CSV data to simulate continuous streams

2. **📬 Custom Message Queue** (`cmd/queue-service/`)
   - Custom-built message queue (no external dependencies)
   - HTTP-based publish/subscribe interface
   - Supports fan-out to multiple collectors
   - In-memory implementation with optional persistence

3. **📥 Telemetry Collector** (`cmd/collector/`)
   - Consumes messages from the queue
   - Parses and validates telemetry data
   - Persists data to storage
   - Supports multiple concurrent instances

4. **💾 Storage Service** (`cmd/storage-service/`)
   - Centralized storage abstraction
   - In-memory implementation (extensible to Postgres)
   - Provides HTTP API for data access
   - Indexed queries for efficient retrieval

5. **🌐 API Gateway** (`cmd/api-gateway/`)
   - RESTful HTTP API
   - OpenAPI/Swagger documentation
   - Query endpoints for GPU telemetry
   - Health checks and observability

### Architecture Diagram

![alt text](Elastic-gpu-pipeline.jpg)

### Data Flow

1. **📥 Ingestion**: Streamers read CSV rows and publish to queue
2. **📦 Buffering**: Queue service buffers messages for collectors
3. **⚙️ Processing**: Collectors consume, parse, and validate messages
4. **💾 Storage**: Processed data is persisted to storage service
5. **🔍 Query**: API Gateway queries storage and serves HTTP responses

### Design Considerations

- **📬 Message Queue**: Custom implementation using Go channels and HTTP, designed for up to 10 producer/consumer instances
- **🗄️ Storage Abstraction**: Repository pattern allows swapping in-memory storage for PostgreSQL/MongoDB without code changes
- **📈 Scalability**: All services are stateless and horizontally scalable
- **🛡️ Fault Tolerance**: Graceful shutdown, error handling, and health checks
- **👁️ Observability**: Structured logging, request tracing, and health endpoints

## ✨ Key Features

- ✅ **Custom Message Queue**: No external dependencies (Kafka, RabbitMQ, etc.)
- ✅ **Horizontal Scaling**: Support for multiple streamer/collector instances
- ✅ **RESTful API**: OpenAPI/Swagger documentation
- ✅ **Kubernetes Native**: Helm charts for easy deployment
- ✅ **Comprehensive Testing**: Unit tests with 89.7% code coverage
- ✅ **Production Ready**: Docker images, health checks, graceful shutdown
- ✅ **Extensible**: Pluggable storage backends

## 📦 Prerequisites

### Required Tools

- **Go 1.22+**: [Installation Guide](https://go.dev/doc/install)
- **Docker** (for creating images and local cluster): [Installation Guide](https://docs.docker.com/get-docker/)
- **kind** (for local Kubernetes deployment): Kubernetes in Docker for local testing
  ```bash
  # macOS
  brew install kind
  
  # Linux
  curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
  chmod +x ./kind
  sudo mv ./kind /usr/local/bin/kind
  ```
- **Helm 3.0+** (for Kubernetes deployment): [Installation Guide](https://helm.sh/docs/intro/install/)
  ```bash
  # macOS
  brew install helm
  ```
- **kubectl** (for Kubernetes deployment): [Installation Guide](https://kubernetes.io/docs/tasks/tools/)

### Verify Installation

```bash
go version        # Should be 1.22 or higher
docker --version
kind --version
helm version
kubectl version --client
```

## 🚀 Quick Start

### Kubernetes Deployment (kind)

Deploy the entire stack to a local Kubernetes cluster with a single command:

```bash
make deploy-local
```

This command will:
- ✅ Check all dependencies
- ✅ Create a kind cluster (if it doesn't exist)
- ✅ Build all Docker images
- ✅ Load images into the cluster
- ✅ Deploy all services via Helm
- ✅ Wait for pods to be ready
- ✅ Start port forwarding to API Gateway

After deployment completes, access:
- **API Gateway**: http://localhost:8081
- **Swagger UI**: http://localhost:8081/swagger/index.html

#### Stop Port Forwarding

```bash
make port-forward-stop-local
```

#### Cleanup

```bash
make cleanup-local
```
### Local Development (No Kubernetes)

Perfect for development and testing without Docker or Kubernetes! 🎯

#### Step 1: Build All Services

```bash
make build
```

This creates binaries in the `bin/` directory:
- `bin/streamer`
- `bin/collector`
- `bin/api-gateway`
- `bin/queue-service`
- `bin/storage-service`

#### Step 2: Start Services in Separate Terminals

**Terminal 1 - Queue Service:**
```bash
make run-queue
# Or: go run ./cmd/queue-service
```

**Terminal 2 - Storage Service:**
```bash
make run-storage
# Or: go run ./cmd/storage-service
```

**Terminal 3 - Collector:**
```bash
make run-collector
# Or: go run ./cmd/collector
```

**Terminal 4 - Streamer:**
```bash
make run-streamer
# Or: go run ./cmd/streamer
```

**Terminal 5 - API Gateway:**
```bash
make run-api
# Or: go run ./cmd/api-gateway
```

#### Step 3: Access the API

Once all services are running:

- **API Gateway**: http://localhost:8081
- **Swagger UI**: http://localhost:8081/swagger/index.html

#### Quick Test

```bash
# List all GPUs
curl http://localhost:8081/api/v1/gpus

# Get telemetry for a specific GPU (replace UUID)
curl http://localhost:8081/api/v1/gpus/GPU-<uuid>/telemetry
```

#### Stop Services

Press `Ctrl+C` in each terminal to stop the services.

## 🔨 Build and Packaging

### Build Binaries

```bash
make build
```

Binaries will be created in the `bin/` directory.

### Build Docker Images

Build all Docker images:
```bash
make docker-all
```

Or build individually:
```bash
make docker-streamer
make docker-collector
make docker-api-gateway
make docker-queue-service
make docker-storage-service
```

### Custom Image Tags

```bash
make docker-all DOCKER_TAG=v1.0.0
```

### Clean Build Artifacts

```bash
make clean          # Remove binaries
make docker-clean   # Remove Docker images
```

## 📚 API Documentation

### Generate OpenAPI Specification

```bash
make swagger
# Or: make openapi
```

This generates:
- `docs/swagger/swagger.json` - OpenAPI 3.0 JSON
- `docs/swagger/swagger.yaml` - OpenAPI 3.0 YAML
- `docs/swagger/docs.go` - Generated Go code

### View API Documentation

#### Option 1: Swagger UI (Interactive) 🌐

1. Start the API Gateway (local or via port-forward)
2. Open in browser: http://localhost:8081/swagger/index.html

#### Option 2: View Generated Files

```bash
cat docs/swagger/swagger.json
cat docs/swagger/swagger.yaml
```

### API Endpoints

#### List All GPUs

```bash
GET /api/v1/gpus
```

**Response:**
```json
{
  "gpus": [
    {
      "uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
      "gpu_index": "0",
      "device_id": "nvidia0",
      "model_name": "NVIDIA H100 80GB HBM3",
      "hostname": "mtv5-dgx1-hgpu-031",
      "container": "gpu-workload",
      "pod": "pod-1",
      "namespace": "team1"
    }
  ],
  "count": 1
}
```

#### Get Telemetry by GPU

```bash
GET /api/v1/gpus/{uuid}/telemetry?start_time=2025-01-01T00:00:00Z&end_time=2025-01-01T23:59:59Z
```

**Query Parameters:**
- `start_time` (optional): ISO 8601 timestamp (inclusive)
- `end_time` (optional): ISO 8601 timestamp (inclusive)

**Response:**
```json
{
  "gpu_uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "telemetry": [
    {
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "value": "85.5",
      "ingestion_time": "2025-01-01T12:00:00Z"
    }
  ],
  "count": 1
}
```

## 🧪 Testing

### Run Unit Tests

Run all unit tests with race detection:
```bash
make test
```

### Run System/End-to-End Tests

Run comprehensive system tests that start all services and test the complete pipeline flow:
```bash
make test-system
```

**Note:** System tests will:
- ✅ Start all services (Queue, Storage, Collector, Streamer, API Gateway)
- ✅ Test the complete data flow from CSV ingestion to API responses
- ✅ Verify data integrity across the pipeline
- ✅ Test multiple streamer/collector instances
- ✅ Automatically clean up all services after tests

**Test Coverage:**
- End-to-end pipeline flow
- Multiple streamer instances
- Data integrity verification
- Time range filtering

### Generate Coverage Report

Generate HTML coverage report:
```bash
make cover
```

Open `coverage.html` in your browser to view detailed coverage.

### View Coverage by Function

```bash
make cover-func
```

### Test Coverage

- **Unit Tests**: **89.7%** code coverage
- **System Tests**: Complete end-to-end pipeline validation

## ⚙️ Configuration

### Environment Variables

#### Streamer

- `CSV_FILE_PATH`: Path to CSV file (default: `/app/csv/dcgm_metrics_20250718_134233.csv`)
- `STREAM_INTERVAL_MS`: Interval between messages in milliseconds (default: `100`)
- `STREAMER_INSTANCE_ID`: Unique instance identifier
- `QUEUE_SERVICE_URL`: Queue service URL (default: `http://localhost:8080`)

#### Collector

- `COLLECTOR_BATCH_SIZE`: Batch size for processing messages (default: `10`)
- `COLLECTOR_INSTANCE_ID`: Unique instance identifier
- `QUEUE_SERVICE_URL`: Queue service URL (default: `http://localhost:8080`)
- `STORAGE_SERVICE_URL`: Storage service URL (default: `http://localhost:8082`)

#### API Gateway

- `API_PORT`: HTTP server port (default: `8081`)
- `STORAGE_SERVICE_URL`: Storage service URL (default: `http://localhost:8082`)

#### Queue Service

- `QUEUE_PORT`: HTTP server port (default: `8080`)
- `QUEUE_BUFFER_SIZE`: Message buffer size (default: `1000`)

#### Storage Service

- `STORAGE_PORT`: HTTP server port (default: `8082`)

### Helm Configuration

For Kubernetes deployments, edit `charts/elastic-gpu-telemetry/values.yaml` or use `--set` flags:

```bash
helm install elastic-gpu-telemetry ./charts/elastic-gpu-telemetry \
  --set streamer.replicaCount=3 \
  --set collector.replicaCount=2 \
  --set streamer.env.STREAM_INTERVAL_MS=50
```

### Scaling Services (Kubernetes)

```bash
helm upgrade elastic-gpu-telemetry ./charts/elastic-gpu-telemetry \
  --set streamer.replicaCount=5 \
  --set collector.replicaCount=3
```

## 🔧 Troubleshooting
### Kubernetes Deployment Issues

#### Pods Not Starting

```bash
# Check pod status
kubectl get pods

# Describe pod for details
kubectl describe pod <pod-name>

# Check events
kubectl get events --sort-by='.lastTimestamp'
```

#### Images Not Found

Ensure images are loaded into kind:
```bash
docker images | grep elastic-gpu-telemetry
make kind-load-images-local
```

#### Service Not Accessible

```bash
# Check service endpoints
kubectl get endpoints

# Check service details
kubectl describe svc api-gateway

# Restart port forwarding
make port-forward-stop-local
make port-forward-bg-local
```

#### Empty API Responses

1. **Check if streamer is running:**
   ```bash
   kubectl logs -l app.kubernetes.io/component=streamer
   ```

2. **Check if collector is running:**
   ```bash
   kubectl logs -l app.kubernetes.io/component=collector
   ```

3. **Verify services are connected:**
   - Streamer → Queue Service
   - Collector → Queue Service
   - Collector → Storage Service
   - API Gateway → Storage Service

4. **Check environment variables:**
   ```bash
   kubectl get deployment streamer -o yaml | grep -A 10 env
   ```

#### Port Forward Issues

```bash
# Stop existing port forward
make port-forward-stop-local

# Check if port is in use
lsof -i :8081

# Start fresh port forward
make port-forward-bg-local
```
### Local Development Issues

#### Services Not Starting

1. **Check if ports are already in use:**
   ```bash
   lsof -i :8080  # Queue Service
   lsof -i :8081  # API Gateway
   lsof -i :8082  # Storage Service
   ```

2. **Verify CSV file exists:**
   ```bash
   ls -la csv/dcgm_metrics_20250718_134233.csv
   ```

3. **Check service logs** for error messages in the terminal where you started each service

#### API Returns Empty Data

1. **Verify all services are running:**
   - Queue Service (port 8080)
   - Storage Service (port 8082)
   - Collector
   - Streamer
   - API Gateway (port 8081)

2. **Check service connectivity:**
   - Streamer → Queue Service
   - Collector → Queue Service
   - Collector → Storage Service
   - API Gateway → Storage Service

3. **Verify CSV file path** matches the `CSV_FILE_PATH` environment variable

## 📝 User Workflow Example

### Complete Workflow (Kubernetes)

1. **Deploy the system:**
   ```bash
   make deploy-local
   ```

2. **Check service status:**
   ```bash
   kubectl get pods
   kubectl get svc
   helm status elastic-gpu-telemetry
   ```

3. **View service logs:**
   ```bash
   kubectl logs -l app.kubernetes.io/component=streamer -f
   kubectl logs -l app.kubernetes.io/component=collector -f
   kubectl logs -l app.kubernetes.io/component=api-gateway -f
   ```

4. **Cleanup:**
   ```bash
   make cleanup-local
   ```

---

### Complete Workflow (Local Development)

1. **Build services:**
   ```bash
   make build
   ```

2. **Start services** (in separate terminals):
   ```bash
   make run-queue      # Terminal 1
   make run-storage    # Terminal 2
   make run-collector  # Terminal 3
   make run-streamer   # Terminal 4
   make run-api        # Terminal 5
   ```

3. **Generate API documentation:**
   ```bash
   make swagger
   ```

4. **Access Swagger UI:**
   - Open: http://localhost:8081/swagger/index.html

5. **Test API endpoints:**
   ```bash
   # List all GPUs
   curl http://localhost:8081/api/v1/gpus
   
   # Get telemetry for a specific GPU
   GPU_UUID="GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
   curl "http://localhost:8081/api/v1/gpus/${GPU_UUID}/telemetry"
   
   # Get telemetry with time filter
   curl "http://localhost:8081/api/v1/gpus/${GPU_UUID}/telemetry?start_time=2025-01-01T00:00:00Z&end_time=2025-01-01T23:59:59Z"
   ```

6. **Run tests:**
   ```bash
   # Run unit tests
   make test
   
   # Run system/end-to-end tests
   make test-system
   
   # Generate coverage report
   make cover
   ```

## 🤖 AI Assistance

This project utilized AI assistance for several key improvements and features:

### 1. **System/End-to-End Test Development**
   - Created comprehensive end-to-end test suite (`tests/system/e2e_test.go`)
   - Implemented test scenarios covering:
     - Complete pipeline flow (Streamer → Queue → Collector → Storage → API Gateway)
     - Multiple streamer instances publishing to the same queue
     - Data integrity verification across the pipeline
   - Set up test infrastructure including:
     - Temporary CSV file generation
     - Service startup/shutdown helpers
     - HTTP client configuration with proper timeouts
     - Proper URL encoding for query parameters

### 2. **Bug Fixes and Corrections**
   - Fixed storage backend type issues (changed from "http" to "memory" with storage_service_url)
   - Corrected API response type usage (TelemetryResponse vs GetTelemetryResponse)
   - Fixed URL encoding for time range query parameters
   - Resolved service startup timing issues

### 3. **Documentation Enhancement**
   - Enhanced README.md with:
     - Emojis for better visual clarity
     - Better organization and structure
     - Local development section (no Kubernetes required)
     - System testing documentation
     - Improved troubleshooting sections
   - Removed repetitive commands and consolidated information

### 4. **Build System Improvements**
   - Added Makefile targets
   - Integrated system tests into the development workflow

### 5. **Testing Best Practices**
   - Implemented proper test isolation (separate ports for each test)
   - Added health check verification before running tests
   - Implemented graceful cleanup of services
   - Used appropriate timeouts and error handling

### 6. **Code Quality**
   - Ensured all tests pass successfully
   - Fixed linting errors
   - Maintained consistency with existing code patterns
   - Used proper Go testing conventions

**Happy coding! 🎉**
---

<p align="center">
  <em>Built with ❤️ by Akshat Khanna</em>
</p>

