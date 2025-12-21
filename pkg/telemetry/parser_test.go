package telemetry

import (
	"strings"
	"testing"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVParser_Parse(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"
"2025-07-18T20:42:35Z","DCGM_FI_DEV_GPU_TEMP","1","nvidia1","GPU-456","NVIDIA H100 80GB HBM3","host-2","container-1","pod-1","ns-1","75","labels"`

	parser := NewCSVParser()
	reader := strings.NewReader(csvData)

	records, gpus, err := parser.Parse(reader)
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Len(t, gpus, 2)

	// Verify first record
	record1 := records[0]
	assert.Equal(t, "GPU-123", record1.GPUUUID)
	assert.Equal(t, "DCGM_FI_DEV_GPU_UTIL", record1.MetricName)
	assert.Equal(t, "100", record1.Value)
	assert.Equal(t, "host-1", record1.Hostname)
	assert.Equal(t, "NVIDIA H100 80GB HBM3", record1.ModelName)
	assert.True(t, record1.IngestionTime.IsZero(), "IngestionTime should be zero until set")

	// Verify second record
	record2 := records[1]
	assert.Equal(t, "GPU-456", record2.GPUUUID)
	assert.Equal(t, "DCGM_FI_DEV_GPU_TEMP", record2.MetricName)
	assert.Equal(t, "75", record2.Value)
	assert.Equal(t, "container-1", record2.Container)
	assert.Equal(t, "pod-1", record2.Pod)
	assert.Equal(t, "ns-1", record2.Namespace)

	// Verify GPUs
	gpuMap := make(map[string]*domain.GPU)
	for _, gpu := range gpus {
		gpuMap[gpu.UUID] = gpu
	}

	gpu1, exists := gpuMap["GPU-123"]
	require.True(t, exists)
	assert.Equal(t, "0", gpu1.GPUID)
	assert.Equal(t, "nvidia0", gpu1.Device)
	assert.Equal(t, "host-1", gpu1.Hostname)

	gpu2, exists := gpuMap["GPU-456"]
	require.True(t, exists)
	assert.Equal(t, "1", gpu2.GPUID)
	assert.Equal(t, "nvidia1", gpu2.Device)
	assert.Equal(t, "host-2", gpu2.Hostname)
}

func TestCSVParser_Parse_EmptyFile(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw`

	parser := NewCSVParser()
	reader := strings.NewReader(csvData)

	records, gpus, err := parser.Parse(reader)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Empty(t, gpus)
}

func TestCSVParser_Parse_InvalidHeader(t *testing.T) {
	csvData := `invalid,header`

	parser := NewCSVParser()
	reader := strings.NewReader(csvData)

	_, _, err := parser.Parse(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected CSV header")
}

func TestCSVParser_Parse_MissingColumns(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0"`

	parser := NewCSVParser()
	reader := strings.NewReader(csvData)

	_, _, err := parser.Parse(reader)
	assert.Error(t, err)
	// CSV reader returns "wrong number of fields" error
	assert.Contains(t, err.Error(), "wrong number of fields")
}

func TestCSVParser_Parse_EmptyUUID(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	parser := NewCSVParser()
	reader := strings.NewReader(csvData)

	_, _, err := parser.Parse(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "uuid is empty")
}

func TestCSVParser_Parse_QuotedValues(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	parser := NewCSVParser()
	reader := strings.NewReader(csvData)

	records, gpus, err := parser.Parse(reader)
	require.NoError(t, err)
	require.Len(t, records, 1)

	record := records[0]
	assert.Equal(t, "GPU-123", record.GPUUUID)
	assert.Equal(t, "DCGM_FI_DEV_GPU_UTIL", record.MetricName)
	assert.Equal(t, "100", record.Value)

	require.Len(t, gpus, 1)
	assert.Equal(t, "GPU-123", gpus[0].UUID)
}

func TestCSVParser_Parse_DuplicateGPUs(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"
"2025-07-18T20:42:35Z","DCGM_FI_DEV_GPU_TEMP","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","75","labels"`

	parser := NewCSVParser()
	reader := strings.NewReader(csvData)

	records, gpus, err := parser.Parse(reader)
	require.NoError(t, err)
	require.Len(t, records, 2)
	// Should only have one unique GPU
	require.Len(t, gpus, 1)
	assert.Equal(t, "GPU-123", gpus[0].UUID)
}
