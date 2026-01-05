package telemetry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTransformer(t *testing.T) {
	transformer := NewTransformer()
	assert.NotNil(t, transformer)
}

func TestTransformer_TransformCSVRow(t *testing.T) {
	transformer := NewTransformer()

	headers := []string{"gpu_id", "metric_name", "value"}
	row := []string{"0", "DCGM_FI_DEV_GPU_UTIL", "100"}

	record, err := transformer.TransformCSVRow(row, headers)
	assert.NoError(t, err)
	assert.NotNil(t, record)
	assert.WithinDuration(t, time.Now(), record.IngestionTime, 1*time.Second)
}

func TestTransformer_TransformCSVRow_EmptyRow(t *testing.T) {
	transformer := NewTransformer()

	headers := []string{"gpu_id", "metric_name", "value"}
	row := []string{}

	record, err := transformer.TransformCSVRow(row, headers)
	assert.NoError(t, err)
	assert.NotNil(t, record)
}

func TestTransformer_TransformCSVRow_NilRow(t *testing.T) {
	transformer := NewTransformer()

	headers := []string{"gpu_id", "metric_name", "value"}
	var row []string = nil

	record, err := transformer.TransformCSVRow(row, headers)
	assert.NoError(t, err)
	assert.NotNil(t, record)
}

func TestTransformer_TransformCSVRow_EmptyHeaders(t *testing.T) {
	transformer := NewTransformer()

	headers := []string{}
	row := []string{"0", "DCGM_FI_DEV_GPU_UTIL", "100"}

	record, err := transformer.TransformCSVRow(row, headers)
	assert.NoError(t, err)
	assert.NotNil(t, record)
}
