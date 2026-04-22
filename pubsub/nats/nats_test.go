package nats_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zutrixpog/gobus/pubsub"
	"github.com/zutrixpog/gobus/pubsub/nats"

	"github.com/stretchr/testify/require"
)

var (
	natsps any

	subOpts = pubsub.SubOptions{AckWait: time.Second * 5, MaxDelivers: 2, Backoff: []time.Duration{3 * time.Second, 6 * time.Second}}
)

func TestMain(m *testing.M) {
	natsConn, stopContainer := nats.RunContainer()

	var err error
	natsps, err = nats.NewNatsPubSub(natsConn, "gobus-test", []string{"gobus.*"}, time.Second*40)
	if err != nil {
		fmt.Printf("failed to initialize pubsub: %s\n", err.Error())
		os.Exit(1)
	}

	code := m.Run()

	stopContainer()
	os.Exit(code)
}

func TestPubSub(t *testing.T) {
	ctx := context.Background()
	ps, ok := natsps.(pubsub.PubSub)
	require.True(t, ok)

	topic := "gobus.test"

	msgChan1 := make(<-chan pubsub.Message)
	msgChan2 := make(<-chan pubsub.Message)
	t.Run("subscribe to a topic", func(t *testing.T) {
		var err error
		msgChan1, err = ps.Subscribe(ctx, topic, subOpts)
		require.Nil(t, err)
		msgChan2, err = ps.Subscribe(ctx, topic, subOpts)
		require.Nil(t, err)
	})

	t.Run("publish to the topic", func(t *testing.T) {
		err := ps.Publish(ctx, topic, []byte("yo"))
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

func TestPubSubOnce(t *testing.T) {
	ctx := context.Background()
	ps, ok := natsps.(pubsub.PubSub)
	require.True(t, ok)

	topic := "gobus.test-queue"

	var msgChan1 <-chan pubsub.Message
	var msgChan2 <-chan pubsub.Message
	var msgChan3 <-chan pubsub.Message
	t.Run("subscribe to a topic", func(t *testing.T) {
		var err error
		msgChan1, err = ps.SubscribeOnce(ctx, topic, subOpts)
		require.Nil(t, err)
		msgChan2, err = ps.SubscribeOnce(ctx, topic, subOpts)
		require.Nil(t, err)
		msgChan3, err = ps.Subscribe(ctx, topic, subOpts)
		require.Nil(t, err)
	})

	t.Run("publish to the topic", func(t *testing.T) {
		err := ps.Publish(ctx, topic, []byte("yo"))
		require.Nil(t, err)
	})

	t.Run("only two of the subscribers should receive the publication", func(t *testing.T) {
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
			case msg := <-msgChan3:
				msg.Ack()
				msgs = append(msgs, string(msg.Data))
			case <-ctxTimeout.Done():
				if len(msgs) == 2 {
					return
				} else if len(msgs) > 2 {
					t.Logf("more than two subscriber received the msg %v", msgs)
					t.Fail()
					return
				}
				t.Log("failed to receive message in time")
				t.Fail()
				return
			}
		}
	})
}

func TestAckNack(t *testing.T) {
	ctx := context.Background()
	ps, _ := natsps.(pubsub.PubSub)
	topic := "gobus.testacknack"

	ch1, err := ps.SubscribeOnce(ctx, topic, subOpts)
	require.Nil(t, err)
	ch2, err := ps.SubscribeOnce(ctx, topic, subOpts)
	require.Nil(t, err)

	t.Run("never ack", func(t *testing.T) {
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Millisecond*100)
		defer cancel()
		ps.PublishOnce(ctx, topic, []byte("yo"))

		msgs := make([]string, 0, 2)
		for {
			select {
			case msg := <-ch1:
				msgs = append(msgs, string(msg.Data))
				msg.Nack()
			case msg := <-ch2:
				msgs = append(msgs, string(msg.Data))
				msg.Nack()
			case <-ctxTimeout.Done():
				if len(msgs) == 1 {
					t.Fatalf("Expected both subscribers to receive msg because of the Nacks.")
				}
				return
			}
		}
	})
}
