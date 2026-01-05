package postgres

import "errors"

var (
	// ErrInvalidGPU is returned when a GPU entity is nil or invalid
	ErrInvalidGPU = errors.New("invalid GPU entity")

	// ErrInvalidGPUUUID is returned when a GPU UUID is empty or invalid
	ErrInvalidGPUUUID = errors.New("invalid GPU UUID")

	// ErrInvalidTelemetryRecord is returned when a telemetry record is nil or invalid
	ErrInvalidTelemetryRecord = errors.New("invalid telemetry record")
)
