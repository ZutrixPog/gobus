package pubsub_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zutrixpog/gobus/pubsub"
)

func TestMockPubSub(t *testing.T) {
	mock := pubsub.NewMockPubSub()
	defer mock.Close()

	t.Run("connected", func(t *testing.T) {
		require.True(t, mock.Connected())
		mock.SetConnected(false)
		require.False(t, mock.Connected())
		mock.SetConnected(true)
		require.True(t, mock.Connected())
	})

	t.Run("reset", func(t *testing.T) {
		mock.Publish(context.Background(), "topic", []byte("test"))
		require.NotEmpty(t, mock.PublishedMessages())
		mock.Reset()
		require.Empty(t, mock.PublishedMessages())
	})

	t.Run("subscriber count", func(t *testing.T) {
		require.Equal(t, 0, mock.SubscriberCount("empty"))
		mock.Subscribe(context.Background(), "topic1", pubsub.SubOptions{})
		require.Equal(t, 1, mock.SubscriberCount("topic1"))
	})
}

func TestMockPubSubWorkQueue(t *testing.T) {
	mock := pubsub.NewMockPubSub()
	defer mock.Close()
	ctx := context.Background()

	topic := "test.workqueue"
	ch1, err := mock.SubscribeOnce(ctx, topic, pubsub.SubOptions{})
	require.Nil(t, err)
	require.NotNil(t, ch1)

	err = mock.PublishOnce(ctx, topic, []byte("work queue msg"))
	require.Nil(t, err)

	select {
	case msg := <-ch1:
		require.Equal(t, "work queue msg", string(msg.Data))
		msg.Ack()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}
