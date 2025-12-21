package api

import (
	"fmt"
	"strings"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// validateGPUUUID validates that the GPU UUID parameter is valid
// GPU UUID should:
// - Not be empty
// - Start with "GPU-" prefix (based on CSV data format)
// - Have reasonable length
func validateGPUUUID(gpuUUID string) error {
	if gpuUUID == "" {
		return domain.ErrInvalidGPUUUID
	}

	// Trim whitespace
	gpuUUID = strings.TrimSpace(gpuUUID)
	if gpuUUID == "" {
		return domain.ErrInvalidGPUUUID
	}

	// Basic format validation: should start with "GPU-" and have reasonable length
	// This is based on the actual format seen in CSV: "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	if !strings.HasPrefix(gpuUUID, "GPU-") {
		return fmt.Errorf("%w: GPU UUID should start with 'GPU-'", domain.ErrInvalidGPUUUID)
	}

	if len(gpuUUID) < 10 || len(gpuUUID) > 100 {
		return fmt.Errorf("%w: GPU UUID has invalid length", domain.ErrInvalidGPUUUID)
	}

	return nil
}
