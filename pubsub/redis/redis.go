package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zutrixpog/gobus/dedup"
	"github.com/zutrixpog/gobus/pubsub"
)

var (
	_ pubsub.PubSub = (*RedisPubSub)(nil)
)

type RedisPubSub struct {
	client     *redis.Client
	stream     string
	consumer   string
	bufferSize int
}

type RedisConfig struct {
	Stream     string
	Consumer   string
	BufferSize int
}

func NewRedisPubSub(client *redis.Client, cfg RedisConfig) (*RedisPubSub, error) {
	if cfg.Stream == "" {
		cfg.Stream = "gobus"
	}
	if cfg.Consumer == "" {
		cfg.Consumer = "consumer"
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 1024
	}

	return &RedisPubSub{
		client:     client,
		stream:     cfg.Stream,
		consumer:   cfg.Consumer,
		bufferSize: cfg.BufferSize,
	}, nil
}

func (r *RedisPubSub) Connected() bool {
	return r.client.Ping(context.Background()).Err() == nil
}

func (r *RedisPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	return r.client.Publish(ctx, topic, data).Err()
}

func (r *RedisPubSub) Subscribe(ctx context.Context, topic string, opts pubsub.SubOptions) (chan pubsub.Message, error) {
	ch := make(chan pubsub.Message, r.bufferSize)

	pubsubConn := r.client.Subscribe(ctx, topic)
	go func() {
		defer close(ch)
		defer pubsubConn.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pubsubConn.Channel():
				select {
				case ch <- pubsub.Message{
					Data:    []byte(msg.Payload),
					Arrival: time.Now(),
					Ack:     func() error { return nil },
					Nack:    func() error { return nil },
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

func (r *RedisPubSub) PublishOnce(ctx context.Context, topic string, data []byte) error {
	streamKey := fmt.Sprintf("%s:%s", r.stream, topic)
	return r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 10000,
		Approx: true,
		Values: map[string]interface{}{
			"data": data,
		},
	}).Err()
}

func (r *RedisPubSub) SubscribeOnce(ctx context.Context, topic string, opts pubsub.SubOptions) (chan pubsub.Message, error) {
	streamKey := fmt.Sprintf("%s:%s", r.stream, topic)
	group := fmt.Sprintf("%s-group", r.consumer)

	err := r.client.XGroupCreateMkStream(ctx, streamKey, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer group: %w", err)
		}
	}

	ch := make(chan pubsub.Message, r.bufferSize)

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    group,
					Consumer: r.consumer,
					Streams:  []string{streamKey, ">"},
					Count:    1,
					Block:    time.Second,
				}).Result()

				if err != nil && err != redis.Nil {
					time.Sleep(time.Second)
					continue
				}

				for _, stream := range streams {
					for _, msg := range stream.Messages {
						data, ok := msg.Values["data"].(string)
						if !ok {
							r.client.XAck(ctx, streamKey, group, msg.ID)
							continue
						}

						msgID := msg.ID
						select {
						case ch <- pubsub.Message{
							Data:    []byte(data),
							Arrival: time.Now(),
							Ack: func() error {
								return r.client.XAck(ctx, streamKey, group, msgID).Err()
							},
							Nack: func() error {
								return r.client.XClaim(ctx, &redis.XClaimArgs{
									Stream:   streamKey,
									Group:    group,
									Consumer: r.consumer,
									MinIdle:  time.Second * 30,
									Messages: []string{msgID},
								}).Err()
							},
						}:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}
	}()

	return ch, nil
}

func (r *RedisPubSub) NewDedupStore(_ context.Context, instanceID string) (dedup.Store, error) {
	prefix := fmt.Sprintf("gobus:dedup:%s", instanceID)
	return dedup.NewRedisStore(r.client, prefix), nil
}
