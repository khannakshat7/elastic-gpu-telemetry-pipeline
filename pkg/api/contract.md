# API Gateway - API Contract

## Framework Choice: Gin

**Justification:**
- Already included in project dependencies (used in queue service)
- Fast and lightweight HTTP web framework
- Excellent middleware support for logging, CORS, authentication
- Built-in JSON binding and validation
- Good OpenAPI/Swagger integration via swaggo
- Widely used in Go community with extensive documentation
- Simple, intuitive API that's easy to learn and maintain

## API Endpoints

### 1. List All GPUs

**Endpoint:** `GET /api/v1/gpus`

**Description:** Returns all GPUs for which telemetry data exists.

**Path Parameters:** None

**Query Parameters:** None

**Request Body:** None

**Response:** `200 OK`
```json
{
  "gpus": [
    {
      "uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
      "gpu_id": "0",
      "device": "nvidia0",
      "model": "NVIDIA H100 80GB HBM3",
      "hostname": "mtv5-dgx1-hgpu-031"
    }
  ],
  "count": 1
}
```

**Error Responses:**
- `500 Internal Server Error`: Server error occurred
```json
{
  "error": "Internal server error: ...",
  "code": "INTERNAL_ERROR",
  "timestamp": "2025-01-01T00:00:00Z"
}
```

---

### 2. Get Telemetry by GPU

**Endpoint:** `GET /api/v1/gpus/{id}/telemetry`

**Description:** Returns all telemetry entries for a specific GPU, ordered by ingestion time (oldest first). Supports optional time window filtering.

**Path Parameters:**
- `id` (string, required): GPU UUID
  - Example: `GPU-5fd4f087-86f3-7a43-b711-4771313afc50`
  - Must be a valid GPU UUID that exists in the system

**Query Parameters:**
- `start_time` (string, optional): Start time filter (inclusive)
  - Format: RFC3339 (e.g., `2025-01-01T00:00:00Z`)
  - If provided, only returns records with `ingestion_time >= start_time`
- `end_time` (string, optional): End time filter (inclusive)
  - Format: RFC3339 (e.g., `2025-01-01T23:59:59Z`)
  - If provided, only returns records with `ingestion_time <= end_time`
- Both `start_time` and `end_time` can be provided together
- If `start_time > end_time`, returns `400 Bad Request`

**Request Body:** None

**Response:** `200 OK`
```json
{
  "gpu_uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "records": [
    {
      "gpu_uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "value": "100",
      "ingestion_time": "2025-01-01T00:00:00Z",
      "hostname": "mtv5-dgx1-hgpu-031",
      "model_name": "NVIDIA H100 80GB HBM3"
    }
  ],
  "count": 1,
  "start_time": "2025-01-01T00:00:00Z",
  "end_time": "2025-01-01T23:59:59Z"
}
```

**Error Responses:**

- `400 Bad Request`: Invalid GPU UUID or time range
```json
{
  "error": "Invalid GPU UUID",
  "code": "INVALID_GPU_UUID",
  "timestamp": "2025-01-01T00:00:00Z"
}
```

```json
{
  "error": "start_time must be before or equal to end_time",
  "code": "INVALID_TIME_RANGE",
  "timestamp": "2025-01-01T00:00:00Z"
}
```

- `404 Not Found`: GPU not found
```json
{
  "error": "GPU with UUID 'GPU-123' not found",
  "code": "GPU_NOT_FOUND",
  "timestamp": "2025-01-01T00:00:00Z"
}
```

- `500 Internal Server Error`: Server error occurred
```json
{
  "error": "Internal server error: ...",
  "code": "INTERNAL_ERROR",
  "timestamp": "2025-01-01T00:00:00Z"
}
```

## Key Design Decisions

### 1. GPU ID Usage
- **Path Parameter `{id}`**: Uses GPU UUID (not gpu_id)
- **Rationale**: UUID is globally unique across all hosts, making it the correct identifier for the API
- **Example**: `/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry`

### 2. DTOs (Data Transfer Objects)
- **Purpose**: Separate API contract from internal domain models
- **Benefits**:
  - API can evolve independently from domain models
  - Hides internal implementation details
  - Allows for API versioning
  - Easier to document and generate OpenAPI specs

### 3. Time Format
- **Format**: RFC3339 (ISO 8601)
- **Example**: `2025-01-01T00:00:00Z`
- **Rationale**: Standard, unambiguous, timezone-aware format

### 4. Error Response Format
- **Consistent Structure**: All errors follow the same format
- **Includes**: Error message, error code, timestamp
- **Benefits**: Easier for clients to handle errors programmatically

### 5. Ordering
- **Telemetry Records**: Always ordered by `ingestion_time` ascending (oldest first)
- **GPUs**: Ordered by UUID (alphabetical) for consistent results

## Handler Design

### Handler Interface Pattern
Handlers follow this pattern:
1. **Extract Parameters**: Path params, query params
2. **Validate Input**: Validate UUID, time formats, ranges
3. **Call Repository**: Use storage repository interface
4. **Transform to DTOs**: Convert domain models to response DTOs
5. **Return Response**: JSON response with appropriate status code

### Dependency Injection
- Handlers receive `storage.Repository` interface via constructor
- Allows for easy testing with mock repositories
- Follows dependency inversion principle

### Error Handling
- Validation errors → 400 Bad Request
- Not found errors → 404 Not Found
- Repository errors → 500 Internal Server Error
- All errors use consistent `ErrorResponse` format

