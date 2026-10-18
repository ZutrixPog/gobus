package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zutrixpog/gobus/dedup"
	"github.com/zutrixpog/gobus/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var (
	_ pubsub.PubSub = (*NatsPubSub)(nil)
)

type NatsPubSub struct {
	conn      *nats.Conn
	js        nats.JetStreamContext
	stream    string
	retention time.Duration
}

func NewNatsPubSub(conn *nats.Conn, stream string, subjects []string, retention time.Duration) (*NatsPubSub, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}

	_, err = js.StreamInfo(stream)
	if err == nats.ErrStreamNotFound {
		cfg := &nats.StreamConfig{
			Name:      stream,
			Subjects:  subjects,
			Retention: nats.LimitsPolicy,
			MaxAge:    retention,
			Storage:   nats.FileStorage,
			Replicas:  1,
		}
		if _, err := js.AddStream(cfg); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	return &NatsPubSub{conn: conn, js: js, stream: stream, retention: retention}, nil
}

func (n *NatsPubSub) Connected() bool {
	return n.conn.Status() != nats.CLOSED
}

func (n *NatsPubSub) EnsureStream(name string, subjects []string, retention time.Duration, workQueue bool) error {
	_, err := n.js.StreamInfo(name)
	if err == nil {
		return nil
	}
	if err != nats.ErrStreamNotFound {
		return err
	}

	retentionPolicy := nats.LimitsPolicy
	if workQueue {
		retentionPolicy = nats.WorkQueuePolicy
	}
	_, err = n.js.AddStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Retention: retentionPolicy,
		MaxAge:    retention,
		Storage:   nats.FileStorage,
		Replicas:  1,
	})
	return err
}

func deliverOption(policy pubsub.DeliverPolicy) nats.SubOpt {
	switch policy {
	case pubsub.DeliverNew:
		return nats.DeliverNew()
	case pubsub.DeliverLast:
		return nats.DeliverLast()
	default:
		return nats.DeliverAll()
	}
}

func messageDeliveries(msg *nats.Msg) uint64 {
	meta, err := msg.Metadata()
	if err != nil {
		return 0
	}
	return meta.NumDelivered
}

func normalizeAckWait(opts pubsub.SubOptions) pubsub.SubOptions {
	if len(opts.Backoff) > 0 && opts.Backoff[len(opts.Backoff)-1] >= opts.AckWait {
		opts.AckWait = opts.Backoff[len(opts.Backoff)-1] + time.Second
	}
	return opts
}

func (n *NatsPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	msg := &nats.Msg{Subject: topic, Data: data, Header: nats.Header{}}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))
	if _, err := n.js.PublishMsg(msg); err != nil {
		return fmt.Errorf("Publish failed on %s: %s", topic, err)
	}

	return nil
}

func (n *NatsPubSub) Subscribe(ctx context.Context, topic string, opts pubsub.SubOptions) (chan pubsub.Message, error) {
	opts = normalizeAckWait(opts)
	appCh := make(chan pubsub.Message)
	queueBuf := 1024
	if opts.Backpressure {
		queueBuf = 0
	}
	queueCh := make(chan pubsub.Message, queueBuf)

	subOpts := []nats.SubOpt{
		nats.ManualAck(),
		nats.AckWait(opts.AckWait),
		nats.MaxDeliver(opts.MaxDelivers),
		nats.BackOff(opts.Backoff),
		deliverOption(opts.Deliver),
	}
	if opts.Durable != "" {
		subOpts = append(subOpts, nats.Durable(opts.Durable))
	}

	sub, err := n.js.Subscribe(topic, func(msg *nats.Msg) {
		queueCh <- pubsub.Message{
			Data:       msg.Data,
			Arrival:    time.Now(),
			Context:    otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(msg.Header)),
			Deliveries: messageDeliveries(msg),
			Ack:        func() error { return msg.Ack() },
			Nack:       func() error { return msg.Nak() },
		}
	}, subOpts...)
	if err != nil {
		close(appCh)
		return appCh, err
	}

	go func() {
		defer close(appCh)
		defer sub.Unsubscribe()
		for {
			select {
			case data, ok := <-queueCh:
				if !ok {
					break
				}
				select {
				case appCh <- data:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return appCh, nil
}

func (n *NatsPubSub) PublishOnce(ctx context.Context, topic string, data []byte) error {
	return n.Publish(ctx, topic, data)
}

func (n *NatsPubSub) SubscribeOnce(ctx context.Context, topic string, opts pubsub.SubOptions) (chan pubsub.Message, error) {
	opts = normalizeAckWait(opts)
	appCh := make(chan pubsub.Message)
	queueBuf := 1024
	if opts.Backpressure {
		queueBuf = 0
	}
	queueCh := make(chan pubsub.Message, queueBuf)

	workQueue := strings.ReplaceAll(topic, ".", "-")
	if opts.Durable != "" {
		workQueue = opts.Durable
	}

	subOpts := []nats.SubOpt{
		nats.ManualAck(),
		nats.AckWait(opts.AckWait),
		nats.MaxDeliver(opts.MaxDelivers),
		nats.BackOff(opts.Backoff),
		deliverOption(opts.Deliver),
	}
	if opts.Durable != "" {
		subOpts = append(subOpts, nats.Durable(opts.Durable))
	}

	sub, err := n.js.QueueSubscribe(topic, workQueue, func(msg *nats.Msg) {
		queueCh <- pubsub.Message{
			Data:       msg.Data,
			Arrival:    time.Now(),
			Context:    otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(msg.Header)),
			Deliveries: messageDeliveries(msg),
			Ack:        func() error { return msg.Ack() },
			Nack:       func() error { return msg.Nak() },
		}
	}, subOpts...)
	if err != nil {
		close(appCh)
		return appCh, fmt.Errorf("SubscribeOnce error: %v", err)
	}

	go func() {
		defer close(appCh)
		defer sub.Unsubscribe()
		for {
			select {
			case data, ok := <-queueCh:
				if !ok {
					break
				}
				select {
				case appCh <- data:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return appCh, nil
}

func (n *NatsPubSub) NewDedupStore(_ context.Context, group string) (dedup.Store, error) {
	bucket := fmt.Sprintf("gobus_dedup_%s", sanitizeStoreName(group))
	return dedup.NewNatsStore(n.js, bucket, 0)
}

func sanitizeStoreName(value string) string {
	return strings.ReplaceAll(value, "-", "_")
}
