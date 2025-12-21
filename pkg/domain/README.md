# Domain Models and Query Patterns

## Domain Models

### GPU
- **Primary Key**: `UUID` (globally unique)
- **CSV Mapping**: `uuid` column
- **API Usage**: Used in `/api/v1/gpus/{uuid}` endpoint

### TelemetryRecord
- **Composite Key**: `(GPUUUID, MetricName, IngestionTime)`
- **CSV Mapping**: Multiple columns (see struct comments)
- **Key Components**:
  - `GPUUUID`: Links to GPU entity
  - `MetricName`: Type of metric (e.g., "DCGM_FI_DEV_GPU_UTIL")
  - `IngestionTime`: System time when record was processed (NOT from CSV)

## Query Patterns

### 1. List All GPUs
**Endpoint**: `GET /api/v1/gpus`

**Storage Query**:
```go
gpus, err := storage.ListGPUs(ctx)
```

**Key Usage**: No key filtering - returns all GPUs indexed by UUID

### 2. Get Telemetry by GPU
**Endpoint**: `GET /api/v1/gpus/{uuid}/telemetry?start_time=...&end_time=...`

**Storage Query**:
```go
records, err := storage.GetTelemetryByGPU(ctx, gpuUUID, startTime, endTime)
```

**Key Usage**:
- **Primary Filter**: `GPUUUID` (from path parameter `{uuid}`)
- **Time Range Filter**: `IngestionTime` between `start_time` and `end_time` (inclusive)
- **Ordering**: Results ordered by `IngestionTime` ascending

**Query Logic**:
1. Filter by `GPUUUID == {uuid}` (required)
2. Filter by `IngestionTime >= start_time` (if provided)
3. Filter by `IngestionTime <= end_time` (if provided)
4. Sort by `IngestionTime` ASC

**Example**:
```
GET /api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2025-01-01T00:00:00Z&end_time=2025-01-01T23:59:59Z
```

This query will:
- Find all telemetry records where `GPUUUID = "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"`
- AND `IngestionTime >= 2025-01-01T00:00:00Z`
- AND `IngestionTime <= 2025-01-01T23:59:59Z`
- Return results ordered by `IngestionTime` ascending

## Composite Key Rationale

The composite key `(GPUUUID, MetricName, IngestionTime)` ensures:
1. **Uniqueness**: Same GPU can have multiple metrics at the same time
2. **Temporal Uniqueness**: Same GPU+metric can have multiple readings over time
3. **CSV Looping**: Ingestion timestamp ensures uniqueness even when CSV is looped
4. **Query Efficiency**: Allows efficient filtering by GPU and time range

## Storage Indexing Strategy

For optimal query performance:
- **Primary Index**: `GPUUUID` (for filtering by GPU)
- **Secondary Index**: `IngestionTime` (for time range queries)
- **Composite Index**: `(GPUUUID, IngestionTime)` (for combined queries)

