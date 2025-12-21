package domain

// MetricType represents the type of DCGM metric.
// These constants correspond to the metric_name values in the CSV.
type MetricType string

const (
	// MetricGPUUtil represents GPU utilization percentage
	MetricGPUUtil MetricType = "DCGM_FI_DEV_GPU_UTIL"

	// MetricDecUtil represents decoder utilization
	MetricDecUtil MetricType = "DCGM_FI_DEV_DEC_UTIL"

	// MetricEncUtil represents encoder utilization
	MetricEncUtil MetricType = "DCGM_FI_DEV_ENC_UTIL"

	// MetricFBFree represents frame buffer free memory
	MetricFBFree MetricType = "DCGM_FI_DEV_FB_FREE"

	// MetricFBUsed represents frame buffer used memory
	MetricFBUsed MetricType = "DCGM_FI_DEV_FB_USED"

	// MetricGPUTemp represents GPU temperature
	MetricGPUTemp MetricType = "DCGM_FI_DEV_GPU_TEMP"

	// MetricMemClock represents memory clock speed
	MetricMemClock MetricType = "DCGM_FI_DEV_MEM_CLOCK"

	// MetricMemCopyUtil represents memory copy utilization
	MetricMemCopyUtil MetricType = "DCGM_FI_DEV_MEM_COPY_UTIL"

	// MetricPowerUsage represents power consumption
	MetricPowerUsage MetricType = "DCGM_FI_DEV_POWER_USAGE"

	// MetricSMClock represents SM (Streaming Multiprocessor) clock speed
	MetricSMClock MetricType = "DCGM_FI_DEV_SM_CLOCK"
)

// IsValid checks if the metric type is a known DCGM metric
func (m MetricType) IsValid() bool {
	switch m {
	case MetricGPUUtil, MetricDecUtil, MetricEncUtil, MetricFBFree,
		MetricFBUsed, MetricGPUTemp, MetricMemClock, MetricMemCopyUtil,
		MetricPowerUsage, MetricSMClock:
		return true
	default:
		return false
	}
}

// String returns the string representation of the metric type
func (m MetricType) String() string {
	return string(m)
}

// AllMetricTypes returns a slice of all known metric types
func AllMetricTypes() []MetricType {
	return []MetricType{
		MetricGPUUtil,
		MetricDecUtil,
		MetricEncUtil,
		MetricFBFree,
		MetricFBUsed,
		MetricGPUTemp,
		MetricMemClock,
		MetricMemCopyUtil,
		MetricPowerUsage,
		MetricSMClock,
	}
}
