package dedup

import (
	"context"
	"sync"
	"time"
)

type MemoryStore struct {
	mu   sync.RWMutex
	keys map[string]time.Time
	done chan struct{}
}

func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{
		keys: make(map[string]time.Time),
		done: make(chan struct{}),
	}
	return s
}

func (s *MemoryStore) Check(_ context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	expiry, ok := s.keys[key]
	return ok && time.Now().Before(expiry), nil
}

func (s *MemoryStore) Mark(_ context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.keys[key] = time.Now().Add(ttl)
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}
