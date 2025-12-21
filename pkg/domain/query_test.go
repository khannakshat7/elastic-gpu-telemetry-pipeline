package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTelemetryQuery_Validate(t *testing.T) {
	tests := []struct {
		name    string
		query   TelemetryQuery
		wantErr bool
	}{
		{
			name: "valid query with GPU UUID only",
			query: TelemetryQuery{
				GPUUUID: "GPU-123",
			},
			wantErr: false,
		},
		{
			name: "valid query with start time",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: timePtr(time.Now()),
			},
			wantErr: false,
		},
		{
			name: "valid query with end time",
			query: TelemetryQuery{
				GPUUUID: "GPU-123",
				EndTime: timePtr(time.Now()),
			},
			wantErr: false,
		},
		{
			name: "valid query with both times",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: timePtr(time.Now().Add(-1 * time.Hour)),
				EndTime:   timePtr(time.Now()),
			},
			wantErr: false,
		},
		{
			name: "invalid - empty GPU UUID",
			query: TelemetryQuery{
				GPUUUID: "",
			},
			wantErr: true,
		},
		{
			name: "invalid - start after end",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: timePtr(time.Now()),
				EndTime:   timePtr(time.Now().Add(-1 * time.Hour)),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTelemetryQuery_Matches(t *testing.T) {
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)
	endTime := now.Add(1 * time.Hour)

	tests := []struct {
		name   string
		query  TelemetryQuery
		record *TelemetryRecord
		want   bool
	}{
		{
			name: "matches - same GPU UUID",
			query: TelemetryQuery{
				GPUUUID: "GPU-123",
			},
			record: &TelemetryRecord{
				GPUUUID: "GPU-123",
			},
			want: true,
		},
		{
			name: "does not match - different GPU UUID",
			query: TelemetryQuery{
				GPUUUID: "GPU-123",
			},
			record: &TelemetryRecord{
				GPUUUID: "GPU-456",
			},
			want: false,
		},
		{
			name: "matches - within time range",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: &startTime,
				EndTime:   &endTime,
			},
			record: &TelemetryRecord{
				GPUUUID:       "GPU-123",
				IngestionTime: now,
			},
			want: true,
		},
		{
			name: "does not match - before start time",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: &startTime,
				EndTime:   &endTime,
			},
			record: &TelemetryRecord{
				GPUUUID:       "GPU-123",
				IngestionTime: startTime.Add(-1 * time.Minute),
			},
			want: false,
		},
		{
			name: "does not match - after end time",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: &startTime,
				EndTime:   &endTime,
			},
			record: &TelemetryRecord{
				GPUUUID:       "GPU-123",
				IngestionTime: endTime.Add(1 * time.Minute),
			},
			want: false,
		},
		{
			name: "matches - exactly at start time",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: &startTime,
				EndTime:   &endTime,
			},
			record: &TelemetryRecord{
				GPUUUID:       "GPU-123",
				IngestionTime: startTime,
			},
			want: true,
		},
		{
			name: "matches - exactly at end time",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: &startTime,
				EndTime:   &endTime,
			},
			record: &TelemetryRecord{
				GPUUUID:       "GPU-123",
				IngestionTime: endTime,
			},
			want: true,
		},
		{
			name: "matches - only start time filter",
			query: TelemetryQuery{
				GPUUUID:   "GPU-123",
				StartTime: &startTime,
			},
			record: &TelemetryRecord{
				GPUUUID:       "GPU-123",
				IngestionTime: now,
			},
			want: true,
		},
		{
			name: "matches - only end time filter",
			query: TelemetryQuery{
				GPUUUID: "GPU-123",
				EndTime: &endTime,
			},
			record: &TelemetryRecord{
				GPUUUID:       "GPU-123",
				IngestionTime: now,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.query.Matches(tt.record)
			assert.Equal(t, tt.want, result)
		})
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}
