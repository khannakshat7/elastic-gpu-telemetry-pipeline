package mq

import "github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"

// Subscriber interface for subscribing to messages
type Subscriber interface {
	Subscribe() (<-chan *domain.Message, error)
	Unsubscribe() error
}

