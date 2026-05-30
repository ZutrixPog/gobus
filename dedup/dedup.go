package dedup

import (
	"context"
	"time"
)

type Store interface {
	CheckAndMark(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Close() error
}

type Provider interface {
	NewDedupStore(ctx context.Context, instanceID string) (Store, error)
}
