package domain

import "time"

// TelemetryRecord represents a single telemetry data point.
// Composite Key: (GPUUUID, MetricName, IngestionTime)
//
// CSV Column Mapping:
//   - GPUUUID: maps to CSV column "uuid" (foreign key to GPU)
//   - MetricName: maps to CSV column "metric_name" (e.g., "DCGM_FI_DEV_GPU_UTIL")
//   - Value: maps to CSV column "value" (numeric value as string, e.g., "100", "0")
//   - IngestionTime: NOT from CSV - set to time.Now() when record is processed
//   - Container: maps to CSV column "container" (often empty)
//   - Pod: maps to CSV column "pod" (often empty)
//   - Namespace: maps to CSV column "namespace" (often empty)
//   - Hostname: maps to CSV column "Hostname" (denormalized for query efficiency)
//   - ModelName: maps to CSV column "modelName" (denormalized for query efficiency)
//   - GPUID: maps to CSV column "gpu_id" (denormalized for GPU entity creation)
//   - Device: maps to CSV column "device" (denormalized for GPU entity creation)
//
// Note: CSV column "timestamp" is IGNORED - we use ingestion time instead.
type TelemetryRecord struct {
	// GPUUUID is the UUID of the GPU this telemetry belongs to.
	// Part of composite key. Maps to CSV column "uuid".
	GPUUUID string `json:"gpu_uuid"`

	// MetricName is the DCGM metric identifier.
	// Part of composite key. Maps to CSV column "metric_name".
	// Examples: "DCGM_FI_DEV_GPU_UTIL", "DCGM_FI_DEV_GPU_TEMP", etc.
	MetricName string `json:"metric_name"`

	// Value is the metric value as a string.
	// Maps to CSV column "value". Can be converted to numeric types as needed.
	// Examples: "100", "0", "97", "98.5"
	Value string `json:"value"`

	// IngestionTime is the timestamp when this record was processed/ingested.
	// NOT from CSV - set to time.Now() when the record is created.
	// Part of composite key. Ensures uniqueness even when CSV is looped.
	IngestionTime time.Time `json:"ingestion_time"`

	// Container is the container identifier (if applicable).
	// Maps to CSV column "container". Often empty in sample data.
	Container string `json:"container,omitempty"`

	// Pod is the Kubernetes pod identifier (if applicable).
	// Maps to CSV column "pod". Often empty in sample data.
	Pod string `json:"pod,omitempty"`

	// Namespace is the Kubernetes namespace (if applicable).
	// Maps to CSV column "namespace". Often empty in sample data.
	Namespace string `json:"namespace,omitempty"`

	// Hostname is the host machine identifier (denormalized for efficiency).
	// Maps to CSV column "Hostname". Stored here to avoid joins in queries.
	Hostname string `json:"hostname,omitempty"`

	// ModelName is the GPU model name (denormalized for efficiency).
	// Maps to CSV column "modelName". Stored here to avoid joins in queries.
	ModelName string `json:"model_name,omitempty"`

	// GPUID is the GPU index on the host (denormalized for efficiency).
	// Maps to CSV column "gpu_id". Stored here to populate GPU entity in collector.
	// Example: "0", "1", "2"
	GPUID string `json:"gpu_id,omitempty"`

	// Device is the device name (denormalized for efficiency).
	// Maps to CSV column "device". Stored here to populate GPU entity in collector.
	// Example: "nvidia0", "nvidia1"
	Device string `json:"device,omitempty"`
}
