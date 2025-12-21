package api

import (
	"testing"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
)

func TestValidateGPUUUID_Valid(t *testing.T) {
	tests := []struct {
		name    string
		gpuUUID string
		wantErr bool
	}{
		{
			name:    "valid GPU UUID",
			gpuUUID: "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
			wantErr: false,
		},
		{
			name:    "valid GPU UUID with minimum length",
			gpuUUID: "GPU-12345678",
			wantErr: false,
		},
		{
			name:    "valid GPU UUID with maximum length",
			gpuUUID: "GPU-5fd4f087-86f3-7a43-b711-4771313afc50-extra-long-uuid-string",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGPUUUID(tt.gpuUUID)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateGPUUUID_Empty(t *testing.T) {
	err := validateGPUUUID("")
	assert.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidGPUUUID)
}

func TestValidateGPUUUID_WhitespaceOnly(t *testing.T) {
	tests := []string{
		" ",
		"  ",
		"\t",
		"\n",
		"   \t\n   ",
	}

	for _, gpuUUID := range tests {
		t.Run("whitespace_"+gpuUUID, func(t *testing.T) {
			err := validateGPUUUID(gpuUUID)
			assert.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrInvalidGPUUUID)
		})
	}
}

func TestValidateGPUUUID_NoPrefix(t *testing.T) {
	tests := []string{
		"5fd4f087-86f3-7a43-b711-4771313afc50",
		"gpu-5fd4f087-86f3-7a43-b711-4771313afc50", // lowercase
		"GPU5fd4f087-86f3-7a43-b711-4771313afc50",  // missing dash
		"INVALID-5fd4f087-86f3-7a43-b711-4771313afc50",
	}

	for _, gpuUUID := range tests {
		t.Run("no_prefix_"+gpuUUID, func(t *testing.T) {
			err := validateGPUUUID(gpuUUID)
			assert.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrInvalidGPUUUID)
			assert.Contains(t, err.Error(), "GPU-")
		})
	}
}

func TestValidateGPUUUID_InvalidLength(t *testing.T) {
	tests := []struct {
		name    string
		gpuUUID string
	}{
		{
			name:    "too short",
			gpuUUID: "GPU-123", // Less than 10 chars
		},
		{
			name:    "too long",
			gpuUUID: "GPU-" + string(make([]byte, 100)), // More than 100 chars
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGPUUUID(tt.gpuUUID)
			assert.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrInvalidGPUUUID)
			assert.Contains(t, err.Error(), "length")
		})
	}
}

func TestValidateGPUUUID_TrimsWhitespace(t *testing.T) {
	tests := []struct {
		name    string
		gpuUUID string
		wantErr bool
	}{
		{
			name:    "leading whitespace",
			gpuUUID: "  GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
			wantErr: false,
		},
		{
			name:    "trailing whitespace",
			gpuUUID: "GPU-5fd4f087-86f3-7a43-b711-4771313afc50  ",
			wantErr: false,
		},
		{
			name:    "both sides whitespace",
			gpuUUID: "  GPU-5fd4f087-86f3-7a43-b711-4771313afc50  ",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGPUUUID(tt.gpuUUID)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
