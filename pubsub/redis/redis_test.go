package redis_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zutrixpog/gobus/pubsub"
	"github.com/zutrixpog/gobus/pubsub/redis"

	"github.com/stretchr/testify/require"
)

var redisClient *redis.RedisPubSub

func TestMain(m *testing.M) {
	client, stopRedis := redis.RunContainer()

	var err error
	redisClient, err = redis.NewRedisPubSub(client, redis.RedisConfig{
		Stream:   "gobus-test",
		Consumer: "test-consumer",
	})
	if err != nil {
		fmt.Printf("failed to initialize redis: %s\n", err.Error())
		os.Exit(1)
	}

	code := m.Run()

	stopRedis()
	os.Exit(code)
}

func TestRedisPubSub(t *testing.T) {
	ctx := context.Background()
	topic := "gobus.test.redis"

	msgChan1 := make(<-chan pubsub.Message)
	msgChan2 := make(<-chan pubsub.Message)
	t.Run("subscribe to a topic", func(t *testing.T) {
		var err error
		msgChan1, err = redisClient.Subscribe(ctx, topic, pubsub.SubOptions{})
		require.Nil(t, err)
		msgChan2, err = redisClient.Subscribe(ctx, topic, pubsub.SubOptions{})
		require.Nil(t, err)
	})

	t.Run("publish to the topic", func(t *testing.T) {
		err := redisClient.Publish(ctx, topic, []byte("hello from redis"))
		require.Nil(t, err)
	})

	t.Run("all subscribers should receive the publication", func(t *testing.T) {
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*2)
		defer cancel()
		msgs := make([]string, 0, 2)
		for {
			select {
			case msg := <-msgChan1:
				msg.Ack()
				msgs = append(msgs, string(msg.Data))
			case msg := <-msgChan2:
				msg.Ack()
				msgs = append(msgs, string(msg.Data))
			case <-ctxTimeout.Done():
				if len(msgs) == 2 {
					return
				}
				t.Log("failed to receive messages in time")
				t.Fail()
				return
			}
		}
	})
}

func TestRedisPubSubOnce(t *testing.T) {
	ctx := context.Background()
	topic := "gobus.test.redis.once"

	ch1, err := redisClient.SubscribeOnce(ctx, topic, pubsub.SubOptions{})
	require.Nil(t, err)
	ch2, err := redisClient.SubscribeOnce(ctx, topic, pubsub.SubOptions{})
	require.Nil(t, err)

	t.Run("publish to the topic", func(t *testing.T) {
		err := redisClient.PublishOnce(ctx, topic, []byte("hello from redis streams"))
		require.Nil(t, err)
	})

	t.Run("only one subscriber should receive the publication", func(t *testing.T) {
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		received := 0
		for {
			select {
			case msg := <-ch1:
				msg.Ack()
				received++
			case msg := <-ch2:
				msg.Ack()
				received++
			case <-ctxTimeout.Done():
				if received == 1 {
					return
				}
				t.Fatalf("expected 1 message, got %d", received)
				return
			}
		}
	})
}
