package dedup

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client     *redis.Client
	prefix     string
}

func NewRedisStore(client *redis.Client, prefix string) *RedisStore {
	return &RedisStore{
		client: client,
		prefix: prefix,
	}
}

func (s *RedisStore) CheckAndMark(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	redisKey := fmt.Sprintf("%s:%s", s.prefix, key)

	ok, err := s.client.SetNX(ctx, redisKey, "1", ttl).Result()
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}
