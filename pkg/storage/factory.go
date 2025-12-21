package storage

import (
	"fmt"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage/memory"
)

// BackendType represents the storage backend type
type BackendType string

const (
	BackendMemory BackendType = "memory"
	BackendMongo  BackendType = "mongo"
)

// NewRepository creates a new storage repository based on the backend type
func NewRepository(backend BackendType, config map[string]string) (Repository, error) {
	switch backend {
	case BackendMemory:
		// Check if storage service URL is provided for HTTP client
		if storageURL, ok := config["storage_service_url"]; ok && storageURL != "" {
			return NewHTTPRepository(storageURL), nil
		}
		return memory.NewStore(), nil
	case BackendMongo:
		return nil, fmt.Errorf("MongoDB storage not yet implemented")
	default:
		return nil, fmt.Errorf("unknown storage backend: %s", backend)
	}
}
