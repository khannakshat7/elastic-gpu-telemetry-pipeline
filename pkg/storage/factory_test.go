package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRepository_Memory(t *testing.T) {
	repo, err := NewRepository(BackendMemory, map[string]string{})
	require.NoError(t, err)
	assert.NotNil(t, repo)
}

func TestNewRepository_Memory_WithStorageServiceURL(t *testing.T) {
	config := map[string]string{
		"storage_service_url": "http://localhost:8082",
	}
	repo, err := NewRepository(BackendMemory, config)
	require.NoError(t, err)
	assert.NotNil(t, repo)
	// Should return HTTP repository
	_, ok := repo.(*HTTPRepository)
	assert.True(t, ok, "Expected HTTPRepository when storage_service_url is provided")
}

func TestNewRepository_Postgres_WithConnectionString(t *testing.T) {
	// This will fail to connect, but tests the connection string path
	config := map[string]string{
		"connection_string": "host=invalid port=5432 user=test password=test dbname=test sslmode=disable",
	}
	_, err := NewRepository(BackendPostgres, config)
	// Should fail to connect, but not fail on config parsing
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to")
}

func TestNewRepository_Postgres_WithIndividualComponents(t *testing.T) {
	config := map[string]string{
		"host":     "localhost",
		"port":     "5432",
		"user":     "postgres",
		"password": "postgres",
		"dbname":   "test",
		"sslmode":  "disable",
	}
	// This will fail to connect, but tests the connection string building
	_, err := NewRepository(BackendPostgres, config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to")
}

func TestNewRepository_Postgres_WithIndividualComponents_DefaultSSLMode(t *testing.T) {
	config := map[string]string{
		"host":     "localhost",
		"port":     "5432",
		"user":     "postgres",
		"password": "postgres",
		"dbname":   "test",
		// sslmode not provided, should default to "disable"
	}
	_, err := NewRepository(BackendPostgres, config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to")
}

func TestNewRepository_Postgres_MissingRequiredFields(t *testing.T) {
	// Missing host
	config := map[string]string{
		"port":     "5432",
		"user":     "postgres",
		"password": "postgres",
		"dbname":   "test",
	}
	_, err := NewRepository(BackendPostgres, config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PostgreSQL requires")
}

func TestNewRepository_Postgres_EmptyConnectionString(t *testing.T) {
	config := map[string]string{
		"connection_string": "",
	}
	_, err := NewRepository(BackendPostgres, config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PostgreSQL requires")
}

func TestNewRepository_Mongo_NotImplemented(t *testing.T) {
	_, err := NewRepository(BackendMongo, map[string]string{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}

func TestNewRepository_UnknownBackend(t *testing.T) {
	_, err := NewRepository(BackendType("unknown"), map[string]string{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown storage backend")
}

func TestBackendType_Constants(t *testing.T) {
	assert.Equal(t, BackendType("memory"), BackendMemory)
	assert.Equal(t, BackendType("postgres"), BackendPostgres)
	assert.Equal(t, BackendType("mongo"), BackendMongo)
}
