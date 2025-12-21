package domain

import "time"

// TelemetryQuery represents query parameters for retrieving telemetry data.
// Used in API endpoint: GET /api/v1/gpus/{uuid}/telemetry?start_time=...&end_time=...
type TelemetryQuery struct {
	// GPUUUID is the UUID of the GPU to query telemetry for.
	// Extracted from URL path parameter: /api/v1/gpus/{uuid}
	GPUUUID string

	// StartTime is the inclusive start time for the time window filter.
	// Optional query parameter: ?start_time=2025-01-01T00:00:00Z
	// If not provided, no lower bound on time.
	StartTime *time.Time

	// EndTime is the inclusive end time for the time window filter.
	// Optional query parameter: ?end_time=2025-01-01T23:59:59Z
	// If not provided, no upper bound on time.
	EndTime *time.Time
}

// Validate validates the query parameters
func (q *TelemetryQuery) Validate() error {
	if q.GPUUUID == "" {
		return ErrInvalidGPUUUID
	}
	if q.StartTime != nil && q.EndTime != nil && q.StartTime.After(*q.EndTime) {
		return ErrInvalidTimeRange
	}
	return nil
}

// Matches checks if a telemetry record matches the query criteria
func (q *TelemetryQuery) Matches(record *TelemetryRecord) bool {
	if record.GPUUUID != q.GPUUUID {
		return false
	}
	if q.StartTime != nil && record.IngestionTime.Before(*q.StartTime) {
		return false
	}
	if q.EndTime != nil && record.IngestionTime.After(*q.EndTime) {
		return false
	}
	return true
}
