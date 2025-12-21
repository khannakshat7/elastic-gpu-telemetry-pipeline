package storage

import (
	"context"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// GPURepository defines the interface for GPU storage operations.
// This interface allows for different storage backends (in-memory, MongoDB, PostgreSQL, etc.)
// to be plugged in via dependency injection.
//
// Example MongoDB implementation:
//
//	type MongoGPURepository struct {
//	    collection *mongo.Collection
//	}
//	func (r *MongoGPURepository) SaveGPU(ctx context.Context, gpu *domain.GPU) error {
//	    _, err := r.collection.ReplaceOne(ctx, bson.M{"uuid": gpu.UUID}, gpu, options.Replace().SetUpsert(true))
//	    return err
//	}
type GPURepository interface {
	// SaveGPU saves or updates a GPU entity.
	// If a GPU with the same UUID exists, it will be updated.
	SaveGPU(ctx context.Context, gpu *domain.GPU) error

	// GetGPU retrieves a GPU by its UUID.
	// Returns nil if the GPU is not found.
	GetGPU(ctx context.Context, uuid string) (*domain.GPU, error)

	// ListGPUs returns all GPUs that have telemetry data.
	// The result should only include GPUs for which at least one telemetry record exists.
	ListGPUs(ctx context.Context) ([]*domain.GPU, error)
}

// TelemetryRepository defines the interface for telemetry storage operations.
// This interface allows for different storage backends to be plugged in.
//
// Example MongoDB implementation:
//
//	type MongoTelemetryRepository struct {
//	    collection *mongo.Collection
//	}
//	func (r *MongoTelemetryRepository) SaveTelemetry(ctx context.Context, record *domain.TelemetryRecord) error {
//	    _, err := r.collection.InsertOne(ctx, record)
//	    return err
//	}
//	func (r *MongoTelemetryRepository) GetTelemetryByGPU(ctx context.Context, gpuUUID string, start, end *time.Time) ([]*domain.TelemetryRecord, error) {
//	    filter := bson.M{"gpu_uuid": gpuUUID}
//	    if start != nil {
//	        filter["ingestion_time"] = bson.M{"$gte": *start}
//	    }
//	    if end != nil {
//	        if filter["ingestion_time"] == nil {
//	            filter["ingestion_time"] = bson.M{"$lte": *end}
//	        } else {
//	            filter["ingestion_time"].(bson.M)["$lte"] = *end
//	        }
//	    }
//	    cursor, err := r.collection.Find(ctx, filter, options.Find().SetSort(bson.M{"ingestion_time": 1}))
//	    // ... process cursor
//	}
type TelemetryRepository interface {
	// SaveTelemetry saves a telemetry record.
	// The composite key (GPUUUID, MetricName, IngestionTime) ensures uniqueness.
	SaveTelemetry(ctx context.Context, record *domain.TelemetryRecord) error

	// GetTelemetryByGPU retrieves telemetry records for a specific GPU.
	// Results are ordered by IngestionTime in ascending order (oldest first).
	//
	// Parameters:
	//   - gpuUUID: The UUID of the GPU to query (required)
	//   - start: Optional start time filter (inclusive). Pass nil for no lower bound.
	//   - end: Optional end time filter (inclusive). Pass nil for no upper bound.
	//
	// Returns:
	//   - []*domain.TelemetryRecord: Telemetry records ordered by IngestionTime ASC
	//   - error: Any error that occurred during the query
	GetTelemetryByGPU(ctx context.Context, gpuUUID string, start, end *time.Time) ([]*domain.TelemetryRecord, error)
}

// Repository combines GPURepository and TelemetryRepository for convenience.
// Most implementations will provide both repositories together.
type Repository interface {
	GPURepository
	TelemetryRepository
}
