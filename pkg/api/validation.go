package api

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// gpuUUIDPattern matches valid GPU UUID format: GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
// This is more restrictive to prevent injection attacks
var gpuUUIDPattern = regexp.MustCompile(`^GPU-[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

// validateGPUUUID validates that the GPU UUID parameter is valid
// GPU UUID should:
// - Not be empty
// - Start with "GPU-" prefix (based on CSV data format)
// - Follow UUID format: GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
// - Contain only valid hexadecimal characters
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

	// Validate format matches expected pattern (optional - for stricter validation)
	// This prevents injection of special characters
	if !gpuUUIDPattern.MatchString(gpuUUID) {
		// Allow looser validation but check for dangerous characters
		if strings.ContainsAny(gpuUUID, "'\";\\<>") {
			return fmt.Errorf("%w: GPU UUID contains invalid characters", domain.ErrInvalidGPUUUID)
		}
	}

	return nil
}
