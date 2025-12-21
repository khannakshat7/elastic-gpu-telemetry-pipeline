package telemetry

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// Parser interface for parsing telemetry data
type Parser interface {
	Parse(reader io.Reader) ([]*domain.TelemetryRecord, []*domain.GPU, error)
}

// CSVParser implements Parser for CSV format
type CSVParser struct{}

// NewCSVParser creates a new CSV parser
func NewCSVParser() Parser {
	return &CSVParser{}
}

// CSV column indices (after parsing header)
const (
	colTimestamp = iota
	colMetricName
	colGPUID
	colDevice
	colUUID
	colModelName
	colHostname
	colContainer
	colPod
	colNamespace
	colValue
	colLabelsRaw
)

// Parse parses CSV data into telemetry records and GPU entities.
// Returns telemetry records and unique GPUs found in the CSV.
// Note: CSV timestamp column is ignored - IngestionTime will be set when publishing.
func (p *CSVParser) Parse(reader io.Reader) ([]*domain.TelemetryRecord, []*domain.GPU, error) {
	csvReader := csv.NewReader(reader)

	// Read and validate header
	header, err := csvReader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Validate header columns
	expectedHeader := []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "modelName", "Hostname", "container", "pod", "namespace", "value", "labels_raw"}
	if len(header) != len(expectedHeader) {
		return nil, nil, fmt.Errorf("unexpected CSV header length: got %d, expected %d", len(header), len(expectedHeader))
	}

	// Track unique GPUs by UUID
	gpuMap := make(map[string]*domain.GPU)
	var records []*domain.TelemetryRecord

	// Read all rows
	rowNum := 1 // Start at 1 since we already read the header
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read CSV row %d: %w", rowNum, err)
		}

		if len(row) != len(expectedHeader) {
			return nil, nil, fmt.Errorf("row %d has unexpected column count: got %d, expected %d", rowNum, len(row), len(expectedHeader))
		}

		// Parse row
		record, gpu, err := p.parseRow(row, rowNum)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse row %d: %w", rowNum, err)
		}

		records = append(records, record)

		// Store unique GPU (will overwrite if duplicate UUID, which is fine)
		if gpu != nil {
			gpuMap[gpu.UUID] = gpu
		}

		rowNum++
	}

	// Convert GPU map to slice
	gpus := make([]*domain.GPU, 0, len(gpuMap))
	for _, gpu := range gpuMap {
		gpus = append(gpus, gpu)
	}

	return records, gpus, nil
}

// parseRow parses a single CSV row into a TelemetryRecord and GPU.
// Note: IngestionTime is set to zero - it should be set to time.Now() when publishing.
func (p *CSVParser) parseRow(row []string, rowNum int) (*domain.TelemetryRecord, *domain.GPU, error) {
	// Helper to trim quotes and whitespace
	trim := func(s string) string {
		s = strings.TrimSpace(s)
		// Remove surrounding quotes if present
		if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
			s = s[1 : len(s)-1]
		}
		return s
	}

	uuid := trim(row[colUUID])
	if uuid == "" {
		return nil, nil, fmt.Errorf("row %d: uuid is empty", rowNum)
	}

	// Create GPU entity
	gpu := &domain.GPU{
		UUID:     uuid,
		GPUID:    trim(row[colGPUID]),
		Device:   trim(row[colDevice]),
		Model:    trim(row[colModelName]),
		Hostname: trim(row[colHostname]),
	}

	// Create TelemetryRecord
	// Note: IngestionTime is zero - will be set when publishing
	record := &domain.TelemetryRecord{
		GPUUUID:    uuid,
		MetricName: trim(row[colMetricName]),
		Value:      trim(row[colValue]),
		// IngestionTime will be set to time.Now() when publishing
		Container: trim(row[colContainer]),
		Pod:       trim(row[colPod]),
		Namespace: trim(row[colNamespace]),
		Hostname:  trim(row[colHostname]),
		ModelName: trim(row[colModelName]),
	}

	return record, gpu, nil
}
