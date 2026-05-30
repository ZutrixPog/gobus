package dedup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsStore struct {
	kv   nats.KeyValue
}

func NewNatsStore(js nats.JetStreamContext, bucket string, ttl time.Duration) (*NatsStore, error) {
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: bucket,
		TTL:    ttl,
	})
	if err != nil {
		kv, err = js.KeyValue(bucket)
		if err != nil {
			return nil, fmt.Errorf("failed to create/open KV bucket %s: %w", bucket, err)
		}
	}

	return &NatsStore{kv: kv}, nil
}

func (s *NatsStore) CheckAndMark(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	_, err := s.kv.Create(key, []byte("1"))
	if err == nil {
		return true, nil
	}

	if errors.Is(err, nats.ErrKeyExists) {
		return false, nil
	}

	return false, err
}

func (s *NatsStore) Close() error {
	return nil
}
