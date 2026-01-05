package storage

import (
	"fmt"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage/memory"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage/postgres"
)

// BackendType represents the storage backend type
type BackendType string

const (
	BackendMemory   BackendType = "memory"
	BackendMongo    BackendType = "mongo"
	BackendPostgres BackendType = "postgres"
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
	case BackendPostgres:
		// PostgreSQL requires a connection string
		connectionString, ok := config["connection_string"]
		if !ok || connectionString == "" {
			// Try to build from individual components
			host := config["host"]
			port := config["port"]
			user := config["user"]
			password := config["password"]
			dbname := config["dbname"]
			sslmode := config["sslmode"]
			if sslmode == "" {
				sslmode = "disable"
			}

			if host == "" || port == "" || user == "" || password == "" || dbname == "" {
				return nil, fmt.Errorf("PostgreSQL requires connection_string or (host, port, user, password, dbname) in config")
			}

			connectionString = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
				host, port, user, password, dbname, sslmode)
		}
		return postgres.NewStore(connectionString)
	case BackendMongo:
		return nil, fmt.Errorf("MongoDB storage not yet implemented")
	default:
		return nil, fmt.Errorf("unknown storage backend: %s", backend)
	}
}
