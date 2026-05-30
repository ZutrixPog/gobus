package dedup

import (
	"context"
	"sync"
	"time"
)

type MemoryStore struct {
	mu     sync.RWMutex
	keys   map[string]time.Time
	done   chan struct{}
}

func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{
		keys: make(map[string]time.Time),
		done: make(chan struct{}),
	}
	return s
}

func (s *MemoryStore) CheckAndMark(_ context.Context, key string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if expiry, ok := s.keys[key]; ok && now.Before(expiry) {
		return false, nil
	}

	s.keys[key] = now.Add(ttl)
	return true, nil
}

func (s *MemoryStore) Close() error {
	return nil
}
