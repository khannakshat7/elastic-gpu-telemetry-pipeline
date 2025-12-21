package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetupLogger(t *testing.T) {
	// Logger should be nil initially (or from previous test)
	Logger = nil

	SetupLogger()

	assert.NotNil(t, Logger)
}

func TestSetupLogger_MultipleCalls(t *testing.T) {
	Logger = nil

	SetupLogger()
	firstLogger := Logger

	SetupLogger()
	secondLogger := Logger

	// Both should be valid loggers
	assert.NotNil(t, firstLogger)
	assert.NotNil(t, secondLogger)
}
