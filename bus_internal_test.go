package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zutrixpog/gobus/pubsub"
)

type flakyPubSub struct {
	*pubsub.MockPubSub
	failures int
}

func (f *flakyPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("publish failed")
	}
	return f.MockPubSub.Publish(ctx, topic, data)
}

func TestPublishRetrySendsEnvelope(t *testing.T) {
	ps := &flakyPubSub{MockPubSub: pubsub.NewMockPubSub(), failures: 1}
	b := &Bus{
		ps:         ps,
		serializer: DefaultSerializer(),
		cfg:        BusConfig{},
		pubQueue:   make(chan Event, 1),
		logCh:      make(chan LogEntry, 16),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.startPublisher(ctx)

	b.pubQueue <- Event{
		Topic:   "test.topic",
		Payload: []byte("raw-inner-payload"),
		ctx:     context.Background(),
		retry:   1,
		backoff: time.Millisecond,
	}

	require.Eventually(t, func() bool {
		return len(ps.PublishedMessages()) >= 1
	}, 2*time.Second, 10*time.Millisecond)

	msgs := ps.PublishedMessages()
	require.Len(t, msgs, 1)

	var evt Event
	require.NoError(t, b.serializer.Decode(msgs[0].Data, &evt), "retried payload must be a full Event envelope")
	require.Equal(t, "test.topic", evt.Topic)
}

func TestPublishSyncBypassesQueue(t *testing.T) {
	ps := pubsub.NewMockPubSub()
	b := &Bus{
		ps:         ps,
		serializer: DefaultSerializer(),
		cfg:        BusConfig{},
		pubQueue:   make(chan Event, 1),
		logCh:      make(chan LogEntry, 16),
		ctx:        context.Background(),
		started:    true,
	}
	old := _bus
	_bus = b
	defer func() { _bus = old }()

	require.NoError(t, PublishSync(context.Background(), "test.sync", map[string]string{"a": "b"}))
	require.Empty(t, b.pubQueue)
	require.Len(t, ps.PublishedMessages(), 1)
}
