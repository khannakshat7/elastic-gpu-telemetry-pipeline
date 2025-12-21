package domain

import "errors"

var (
	// ErrInvalidGPUUUID is returned when a GPU UUID is invalid or missing
	ErrInvalidGPUUUID = errors.New("invalid or missing GPU UUID")

	// ErrInvalidTimeRange is returned when start_time is after end_time
	ErrInvalidTimeRange = errors.New("start_time must be before or equal to end_time")

	// ErrGPUNotFound is returned when a GPU is not found
	ErrGPUNotFound = errors.New("GPU not found")

	// ErrInvalidMetricType is returned when a metric type is invalid
	ErrInvalidMetricType = errors.New("invalid metric type")
)
