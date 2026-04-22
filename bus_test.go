package bus_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bus "github.com/zutrixpog/gobus"
	"github.com/zutrixpog/gobus/pubsub"
	"github.com/zutrixpog/gobus/pubsub/nats"
)

var ps any

type TestEvent struct {
	Name string
	Age  int
}

type Addition struct {
	A int `json:"a"`
	B int `json:"b"`
}

func TestMain(m *testing.M) {
	natsConn, stopNats := nats.RunContainer()

	var err error
	ps, err = nats.NewNatsPubSub(natsConn, "gobus-test", []string{"gobus.*"}, 10*time.Second)
	if err != nil {
		fmt.Printf("failed to start nats: %s\n", err)
		return
	}

	m.Run()

	stopNats()
}

func TestID(t *testing.T) {
	id1 := bus.ID()
	id2 := bus.ID()
	require.NotEmpty(t, id1)
	require.NotEqual(t, id1, id2)
}

func TestStatusMethods(t *testing.T) {
	bus.Init(ps.(pubsub.PubSub), bus.BusConfig{})

	t.Run("before start", func(t *testing.T) {
		require.False(t, bus.IsRunning())
		require.False(t, bus.IsDraining())
		require.Zero(t, bus.StartedAt())
		require.Zero(t, bus.HandlerCount())
	})

	bus.Handle("test.topic", func(ctx context.Context, evt TestEvent, meta bus.Event) error {
		return nil
	})

	require.Equal(t, 1, bus.HandlerCount())
	require.True(t, bus.HasHandler("test.topic"))
	require.True(t, bus.HasHandler("test.topic"))
	require.True(t, bus.HasHandler("test.topic"))

	handlers := bus.Handlers()
	require.Contains(t, handlers, "test.topic")

	require.True(t, bus.RemoveHandler("test.topic"))
	require.False(t, bus.RemoveHandler("test.topic"))
	require.False(t, bus.HasHandler("test.topic"))
	require.Equal(t, 0, bus.HandlerCount())
}

func TestSerialization(t *testing.T) {
	serializer := bus.DefaultSerializer()

	type TestPayload struct {
		Name string
		Age  int
	}

	payload := TestPayload{Name: "John", Age: 30}

	encoded, err := serializer.Encode(payload)
	require.Nil(t, err)
	require.NotEmpty(t, encoded)

	var decoded TestPayload
	err = serializer.Decode(encoded, &decoded)
	require.Nil(t, err)
	require.Equal(t, payload.Name, decoded.Name)
	require.Equal(t, payload.Age, decoded.Age)
}

func TestCorrelationIDs(t *testing.T) {
	topic := "gobus.test-correlation"

	var capturedEvent bus.Event
	bus.Init(ps.(pubsub.PubSub), bus.BusConfig{})

	bus.Handle(topic, func(ctx context.Context, evt TestEvent, meta bus.Event) error {
		capturedEvent = meta
		return nil
	}, bus.WithExactlyOnce())

	require.Nil(t, bus.Start())

	correlationID := "test-correlation-id-12345"
	traceID := "trace-abc-67890"

	err := bus.Publish(context.Background(), topic, TestEvent{Name: "test"}, bus.WithCorrelationID(correlationID), bus.WithTraceID(traceID))
	require.Nil(t, err)

	time.Sleep(500 * time.Millisecond)
	require.Equal(t, correlationID, capturedEvent.CorrelationID)
	require.Equal(t, traceID, capturedEvent.TraceID)
}

func TestRpcWithTimeout(t *testing.T) {
	ctx := context.Background()
	fname := "timeout-test-v2"

	bus.Init(ps.(pubsub.PubSub), bus.BusConfig{})

	require.Nil(t, bus.Serve(fname, func(ctx context.Context, req Addition) (Addition, error) {
		return Addition{A: req.A + req.B}, nil
	}))

	res, err := bus.Call[Addition, Addition](ctx, fname, Addition{A: 1, B: 2}, bus.WithRpcTimeout(500*time.Millisecond))
	require.Nil(t, err)
	require.Equal(t, 3, res.A)
}

func TestHandlerOptions(t *testing.T) {
	bus.Init(ps.(pubsub.PubSub), bus.BusConfig{})

	handler := func(ctx context.Context, evt TestEvent, meta bus.Event) error {
		return nil
	}

	topic := "gobus.test-new"
	bus.Handle(topic+"1", handler, bus.WithConcurrency(4))
	bus.Handle(topic+"2", handler, bus.WithRetry(5, 2*time.Second))
	bus.Handle(topic+"3", handler, bus.WithBackPressure())

	require.GreaterOrEqual(t, bus.HandlerCount(), 3)
}

func TestMessageHandling(t *testing.T) {
	topic := "gobus.test-handle"
	ctx := context.Background()
	bus.Init(ps.(pubsub.PubSub), bus.BusConfig{})

	midValues := make([]int, 0, 4)
	expectedMidValue := (1 + 2) * (3 + 4)
	mid1 := func(next bus.HandlerFunc[TestEvent]) bus.HandlerFunc[TestEvent] {
		return func(ctx context.Context, evt TestEvent, meta bus.Event) error {
			midValues = append(midValues, 1)
			err := next(ctx, evt, meta)
			midValues = append(midValues, 3)
			return err
		}
	}

	mid2 := func(next bus.HandlerFunc[TestEvent]) bus.HandlerFunc[TestEvent] {
		return func(ctx context.Context, evt TestEvent, meta bus.Event) error {
			midValues = append(midValues, 2)
			err := next(ctx, evt, meta)
			midValues = append(midValues, 4)
			return err
		}
	}

	var received TestEvent
	c := 0
	bus.Handle(topic, func(ctx context.Context, evt TestEvent, meta bus.Event) error {
		if c < 2 {
			c += 1
			return fmt.Errorf("failed")
		}
		received = evt
		return nil
	}, bus.WithConcurrency(2), bus.WithRetry(3, 10*time.Second), bus.WithMiddleware(mid1, mid2))

	require.Nil(t, bus.Start())

	t.Run("handle message", func(t *testing.T) {
		evt := TestEvent{Name: "taco", Age: 5}

		err := bus.Publish(ctx, topic, evt, bus.WithRetryQueue(10, time.Second))
		require.Nil(t, err)

		time.Sleep(time.Second * 3)
		require.Equal(t, evt.Name, received.Name)
		require.Equal(t, evt.Age, received.Age)

		require.Greater(t, len(midValues), 4)
		require.Equal(t, expectedMidValue, (midValues[0]+midValues[1])*(midValues[2]+midValues[3]))
	})
}

func TestLogChannel(t *testing.T) {
	logCh := bus.Logs()

	received := false
	for i := 0; i < 10; i++ {
		select {
		case entry := <-logCh:
			t.Logf("Log entry: %s - %s", entry.Level, entry.Message)
			received = true
		case <-time.After(100 * time.Millisecond):
		}
		if received {
			break
		}
	}
	require.True(t, received, "should have received at least one log entry")
}

func TestRPC(t *testing.T) {
	ctx := context.Background()
	fname := "add"
	bus.Init(ps.(pubsub.PubSub), bus.BusConfig{})

	require.Nil(t, bus.Serve(fname, func(ctx context.Context, req Addition) (Addition, error) {
		if req.A > 100 {
			return Addition{}, fmt.Errorf("first number is too big")
		}

		return Addition{
			A: req.A + req.B,
			B: 0,
		}, nil
	}))

	t.Run("make RPC", func(t *testing.T) {
		res, err := bus.Call[Addition, Addition](ctx, fname, Addition{
			A: 11,
			B: 31,
		})

		require.Nil(t, err)
		require.Equal(t, 42, res.A)
	})

	t.Run("make RPC but fail", func(t *testing.T) {
		_, err := bus.Call[Addition, Addition](ctx, fname, Addition{
			A: 110,
			B: 31,
		})

		require.NotNil(t, err)
	})
}
