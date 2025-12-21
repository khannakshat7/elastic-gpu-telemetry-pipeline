package mq

import "github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"

// Publisher interface for publishing messages
type Publisher interface {
	Publish(message *domain.Message) error
}

