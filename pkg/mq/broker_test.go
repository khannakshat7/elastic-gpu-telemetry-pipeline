package mq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBroker(t *testing.T) {
	broker := NewBroker()

	assert.NotNil(t, broker)
	assert.IsType(t, &Broker{}, broker)
}
