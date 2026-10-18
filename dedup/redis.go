package dedup

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
	prefix string
}

func NewRedisStore(client *redis.Client, prefix string) *RedisStore {
	return &RedisStore{
		client: client,
		prefix: prefix,
	}
}

func (s *RedisStore) Check(ctx context.Context, key string) (bool, error) {
	n, err := s.client.Exists(ctx, fmt.Sprintf("%s:%s", s.prefix, key)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *RedisStore) Mark(ctx context.Context, key string, ttl time.Duration) error {
	return s.client.Set(ctx, fmt.Sprintf("%s:%s", s.prefix, key), "1", ttl).Err()
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}
