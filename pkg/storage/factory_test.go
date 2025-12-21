package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRepository_Memory(t *testing.T) {
	repo, err := NewRepository(BackendMemory, map[string]string{})

	assert.NoError(t, err)
	assert.NotNil(t, repo)
}

func TestNewRepository_Memory_WithStorageServiceURL(t *testing.T) {
	config := map[string]string{
		"storage_service_url": "http://localhost:8082",
	}
	repo, err := NewRepository(BackendMemory, config)

	assert.NoError(t, err)
	assert.NotNil(t, repo)
	// Should return HTTP repository
	_, ok := repo.(*HTTPRepository)
	assert.True(t, ok)
}

func TestNewRepository_Mongo_NotImplemented(t *testing.T) {
	repo, err := NewRepository(BackendMongo, map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, repo)
	assert.Contains(t, err.Error(), "not yet implemented")
}

func TestNewRepository_UnknownBackend(t *testing.T) {
	repo, err := NewRepository(BackendType("unknown"), map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, repo)
	assert.Contains(t, err.Error(), "unknown storage backend")
}

func TestBackendType_Constants(t *testing.T) {
	assert.Equal(t, BackendType("memory"), BackendMemory)
	assert.Equal(t, BackendType("mongo"), BackendMongo)
}
