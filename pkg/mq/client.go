package mq

import "github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"

// Client represents a message queue client interface
type Client interface {
	Publish(topic string, message *domain.Message) error
	Subscribe(topic string) (<-chan *domain.Message, error)
	Close() error
}

