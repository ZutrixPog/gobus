package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bus "github.com/zutrixpog/gobus"
	"github.com/zutrixpog/gobus/pubsub/nats"
)

func TestZPersistConfig(t *testing.T) {
	conn, stop := nats.RunContainer()
	defer stop()
	ps, err := nats.NewNatsPubSub(conn, "hamgram-z", []string{"hamgram.*"}, 10*time.Second)
	require.NoError(t, err)
	bus.Init(ps, bus.BusConfig{ConsumerGroup: "persist"})
	ch := make(chan struct{}, 1)
	bus.Handle("hamgram.persist", func(ctx context.Context, evt TestEvent, meta bus.Event) error {
		ch <- struct{}{}
		return nil
	}, bus.WithDedup[TestEvent](24*time.Hour), bus.WithRetry(5, 5*time.Second))
	require.NoError(t, bus.Start())
	require.NoError(t, bus.Publish(context.Background(), "hamgram.persist", TestEvent{Name: "x"}, bus.WithEventID("1")))
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery")
	}
}
