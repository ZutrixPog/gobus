package pubsub

import (
	"context"
	"time"
)

type Message struct {
	Data    []byte
	Arrival time.Time

	Ack  func() error
	Nack func() error
}

type SubOptions struct {
	AckWait     time.Duration
	MaxDelivers int
	Backoff     []time.Duration
}

// Fan-out methods: Publish, Subscribe - all subscribers receive the message
// Work-queue methods: PublishOnce, SubscribeOnce - only one subscriber receives (load-balanced)
type PubSub interface {
	Connected() bool

	Publish(ctx context.Context, topic string, data []byte) error
	Subscribe(ctx context.Context, topic string, opts SubOptions) (chan Message, error)

	WorkQueue
}

type WorkQueue interface {
	PublishOnce(ctx context.Context, topic string, data []byte) error
	SubscribeOnce(ctx context.Context, topic string, opts SubOptions) (chan Message, error)
}
