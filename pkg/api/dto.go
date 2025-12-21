package api

import "time"

// GPUResponse represents a GPU in API responses.
// This DTO separates the API contract from internal domain models.
type GPUResponse struct {
	UUID      string `json:"uuid" example:"GPU-5fd4f087-86f3-7a43-b711-4771313afc50"`
	DeviceID  string `json:"device_id" example:"nvidia0"`
	GPUIndex  string `json:"gpu_index" example:"0"`
	ModelName string `json:"model_name" example:"NVIDIA H100 80GB HBM3"`
	Hostname  string `json:"hostname" example:"mtv5-dgx1-hgpu-031"`
	Container string `json:"container,omitempty" example:"gpu-workload"`
	Pod       string `json:"pod,omitempty" example:"pod-1"`
	Namespace string `json:"namespace,omitempty" example:"team1"`
}

// TelemetryResponse represents a telemetry record in API responses.
// This DTO separates the API contract from internal domain models.
type TelemetryResponse struct {
	GPUUUID       string    `json:"gpu_uuid" example:"GPU-5fd4f087-86f3-7a43-b711-4771313afc50"`
	MetricName    string    `json:"metric_name" example:"DCGM_FI_DEV_GPU_UTIL"`
	Value         string    `json:"value" example:"100"`
	IngestionTime time.Time `json:"ingestion_time" example:"2025-01-01T00:00:00Z"`
	Container     string    `json:"container,omitempty"`
	Pod           string    `json:"pod,omitempty"`
	Namespace     string    `json:"namespace,omitempty"`
	Hostname      string    `json:"hostname,omitempty" example:"mtv5-dgx1-hgpu-031"`
	ModelName     string    `json:"model_name,omitempty" example:"NVIDIA H100 80GB HBM3"`
}

// ListGPUsResponse represents the response for GET /api/v1/gpus
type ListGPUsResponse struct {
	GPUs  []GPUResponse `json:"gpus"`
	Count int           `json:"count"`
}

// GetTelemetryResponse represents the response for GET /api/v1/gpus/{id}/telemetry
type GetTelemetryResponse struct {
	GPUUUID   string              `json:"gpu_uuid" example:"GPU-5fd4f087-86f3-7a43-b711-4771313afc50"`
	Records   []TelemetryResponse `json:"records"`
	Count     int                 `json:"count"`
	StartTime *time.Time          `json:"start_time,omitempty"`
	EndTime   *time.Time          `json:"end_time,omitempty"`
}

// ErrorResponse represents an error in API responses
type ErrorResponse struct {
	Error     string    `json:"error" example:"GPU not found"`
	Message   string    `json:"message,omitempty" example:"The requested GPU UUID does not exist"`
	Code      string    `json:"code,omitempty" example:"GPU_NOT_FOUND"`
	Timestamp time.Time `json:"timestamp" example:"2025-01-01T00:00:00Z"`
}
