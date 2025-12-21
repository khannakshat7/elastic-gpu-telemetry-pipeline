package telemetry

import (
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// Transformer transforms raw data into domain entities
type Transformer struct{}

// NewTransformer creates a new transformer
func NewTransformer() *Transformer {
	return &Transformer{}
}

// TransformCSVRow transforms a CSV row into a TelemetryRecord
func (t *Transformer) TransformCSVRow(row []string, headers []string) (*domain.TelemetryRecord, error) {
	// TODO: Implement CSV row transformation
	// Use ingestion time instead of CSV timestamp
	record := &domain.TelemetryRecord{
		IngestionTime: time.Now(),
	}
	return record, nil
}

