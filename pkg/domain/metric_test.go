package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricType_IsValid(t *testing.T) {
	tests := []struct {
		name      string
		metric    MetricType
		wantValid bool
	}{
		{"GPU Util", MetricGPUUtil, true},
		{"Dec Util", MetricDecUtil, true},
		{"Enc Util", MetricEncUtil, true},
		{"FB Free", MetricFBFree, true},
		{"FB Used", MetricFBUsed, true},
		{"GPU Temp", MetricGPUTemp, true},
		{"Mem Clock", MetricMemClock, true},
		{"Mem Copy Util", MetricMemCopyUtil, true},
		{"Power Usage", MetricPowerUsage, true},
		{"SM Clock", MetricSMClock, true},
		{"Invalid", MetricType("INVALID_METRIC"), false},
		{"Empty", MetricType(""), false},
		{"Random", MetricType("RANDOM_STRING"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantValid, tt.metric.IsValid())
		})
	}
}

func TestMetricType_String(t *testing.T) {
	tests := []struct {
		name     string
		metric   MetricType
		expected string
	}{
		{"GPU Util", MetricGPUUtil, "DCGM_FI_DEV_GPU_UTIL"},
		{"Dec Util", MetricDecUtil, "DCGM_FI_DEV_DEC_UTIL"},
		{"Empty", MetricType(""), ""},
		{"Custom", MetricType("CUSTOM_METRIC"), "CUSTOM_METRIC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.metric.String())
		})
	}
}

func TestAllMetricTypes(t *testing.T) {
	allTypes := AllMetricTypes()

	assert.Len(t, allTypes, 10)
	assert.Contains(t, allTypes, MetricGPUUtil)
	assert.Contains(t, allTypes, MetricDecUtil)
	assert.Contains(t, allTypes, MetricEncUtil)
	assert.Contains(t, allTypes, MetricFBFree)
	assert.Contains(t, allTypes, MetricFBUsed)
	assert.Contains(t, allTypes, MetricGPUTemp)
	assert.Contains(t, allTypes, MetricMemClock)
	assert.Contains(t, allTypes, MetricMemCopyUtil)
	assert.Contains(t, allTypes, MetricPowerUsage)
	assert.Contains(t, allTypes, MetricSMClock)

	// Verify all returned types are valid
	for _, metricType := range allTypes {
		assert.True(t, metricType.IsValid(), "AllMetricTypes should only return valid metrics")
	}
}
