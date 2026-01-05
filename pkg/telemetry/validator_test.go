package telemetry

import (
	"testing"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewValidator(t *testing.T) {
	validator := NewValidator()
	assert.NotNil(t, validator)
}

func TestValidator_Validate(t *testing.T) {
	validator := NewValidator()

	record := &domain.TelemetryRecord{
		GPUUUID:    "GPU-123",
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Value:      "100",
	}

	err := validator.Validate(record)
	assert.NoError(t, err)
}

func TestValidator_Validate_NilRecord(t *testing.T) {
	validator := NewValidator()

	err := validator.Validate(nil)
	// Currently returns nil, but should handle nil gracefully
	assert.NoError(t, err)
}

func TestValidator_Validate_EmptyRecord(t *testing.T) {
	validator := NewValidator()

	record := &domain.TelemetryRecord{}

	err := validator.Validate(record)
	assert.NoError(t, err)
}
