package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// Store implements both GPURepository and TelemetryRepository interfaces
// using in-memory data structures (Go maps and slices).
// It is thread-safe for concurrent read/write operations.
//
// Store implements the storage.Repository interface (which combines
// GPURepository and TelemetryRepository) through Go's implicit interface
// satisfaction - no explicit import of storage package needed.
type Store struct {
	// mu protects all data structures below
	mu sync.RWMutex

	// gpus stores GPU entities by UUID
	gpus map[string]*domain.GPU

	// telemetry stores all telemetry records
	telemetry []*domain.TelemetryRecord

	// gpuTelemetryIndex maps GPU UUID to indices in the telemetry slice
	// This index allows efficient queries by GPU UUID
	gpuTelemetryIndex map[string][]int

	// gpusWithTelemetry tracks which GPUs have telemetry data
	// Used for efficient ListGPUs implementation
	gpusWithTelemetry map[string]bool
}

// NewStore creates a new in-memory store.
// Returns a Store that implements both GPURepository and TelemetryRepository.
// The return type is *Store, which implicitly satisfies storage.Repository interface.
func NewStore() *Store {
	return &Store{
		gpus:              make(map[string]*domain.GPU),
		telemetry:         make([]*domain.TelemetryRecord, 0),
		gpuTelemetryIndex: make(map[string][]int),
		gpusWithTelemetry: make(map[string]bool),
	}
}

// SaveGPU saves or updates a GPU entity.
// Thread-safe: uses write lock for concurrent safety.
func (s *Store) SaveGPU(ctx context.Context, gpu *domain.GPU) error {
	if gpu == nil {
		return ErrInvalidGPU
	}
	if gpu.UUID == "" {
		return ErrInvalidGPUUUID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.gpus[gpu.UUID] = gpu
	return nil
}

// GetGPU retrieves a GPU by its UUID.
// Thread-safe: uses read lock for concurrent access.
func (s *Store) GetGPU(ctx context.Context, uuid string) (*domain.GPU, error) {
	if uuid == "" {
		return nil, ErrInvalidGPUUUID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	gpu, exists := s.gpus[uuid]
	if !exists {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return &domain.GPU{
		UUID:      gpu.UUID,
		GPUID:     gpu.GPUID,
		Device:    gpu.Device,
		Model:     gpu.Model,
		Hostname:  gpu.Hostname,
		Container: gpu.Container,
		Pod:       gpu.Pod,
		Namespace: gpu.Namespace,
	}, nil
}

// ListGPUs returns all GPUs that have telemetry data.
// Thread-safe: uses read lock for concurrent access.
func (s *Store) ListGPUs(ctx context.Context) ([]*domain.GPU, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Only return GPUs that have telemetry data
	gpus := make([]*domain.GPU, 0, len(s.gpusWithTelemetry))
	for uuid := range s.gpusWithTelemetry {
		if gpu, exists := s.gpus[uuid]; exists {
			// Return a copy to prevent external modifications
			gpus = append(gpus, &domain.GPU{
				UUID:      gpu.UUID,
				GPUID:     gpu.GPUID,
				Device:    gpu.Device,
				Model:     gpu.Model,
				Hostname:  gpu.Hostname,
				Container: gpu.Container,
				Pod:       gpu.Pod,
				Namespace: gpu.Namespace,
			})
		}
	}

	// Sort by UUID for consistent ordering
	sort.Slice(gpus, func(i, j int) bool {
		return gpus[i].UUID < gpus[j].UUID
	})

	return gpus, nil
}

// SaveTelemetry saves a telemetry record.
// Thread-safe: uses write lock for concurrent safety.
// Updates the GPU telemetry index for efficient queries.
func (s *Store) SaveTelemetry(ctx context.Context, record *domain.TelemetryRecord) error {
	if record == nil {
		return ErrInvalidTelemetryRecord
	}
	if record.GPUUUID == "" {
		return ErrInvalidGPUUUID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Append to telemetry slice
	index := len(s.telemetry)
	s.telemetry = append(s.telemetry, record)

	// Update index
	s.gpuTelemetryIndex[record.GPUUUID] = append(s.gpuTelemetryIndex[record.GPUUUID], index)

	// Mark GPU as having telemetry
	s.gpusWithTelemetry[record.GPUUUID] = true

	return nil
}

// GetTelemetryByGPU retrieves telemetry records for a specific GPU.
// Results are ordered by IngestionTime in ascending order (oldest first).
// Thread-safe: uses read lock for concurrent access.
func (s *Store) GetTelemetryByGPU(ctx context.Context, gpuUUID string, start, end *time.Time) ([]*domain.TelemetryRecord, error) {
	if gpuUUID == "" {
		return nil, ErrInvalidGPUUUID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get indices for this GPU from the index
	indices, exists := s.gpuTelemetryIndex[gpuUUID]
	if !exists || len(indices) == 0 {
		return []*domain.TelemetryRecord{}, nil
	}

	// Collect records using indices
	results := make([]*domain.TelemetryRecord, 0, len(indices))
	for _, idx := range indices {
		if idx >= len(s.telemetry) {
			continue // Index out of bounds (shouldn't happen, but be safe)
		}
		record := s.telemetry[idx]

		// Apply time filters if provided
		if start != nil && record.IngestionTime.Before(*start) {
			continue
		}
		if end != nil && record.IngestionTime.After(*end) {
			continue
		}

		// Create a copy to prevent external modifications
		results = append(results, &domain.TelemetryRecord{
			GPUUUID:       record.GPUUUID,
			MetricName:    record.MetricName,
			Value:         record.Value,
			IngestionTime: record.IngestionTime,
			Container:     record.Container,
			Pod:           record.Pod,
			Namespace:     record.Namespace,
			Hostname:      record.Hostname,
			ModelName:     record.ModelName,
		})
	}

	// Sort by IngestionTime ascending (oldest first)
	sort.Slice(results, func(i, j int) bool {
		return results[i].IngestionTime.Before(results[j].IngestionTime)
	})

	return results, nil
}
