package dedup

import (
	"context"
	"time"
)

type Store interface {
	Check(ctx context.Context, key string) (bool, error)
	Mark(ctx context.Context, key string, ttl time.Duration) error
	Close() error
}

type Provider interface {
	NewDedupStore(ctx context.Context, instanceID string) (Store, error)
}
