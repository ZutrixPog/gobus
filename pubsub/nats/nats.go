package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zutrixpog/gobus/dedup"
	"github.com/zutrixpog/gobus/pubsub"
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

func (n *NatsPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	if _, err := n.js.Publish(topic, data); err != nil {
		return fmt.Errorf("Publish failed on %s: %s", topic, err)
	}

	return nil
}

func (n *NatsPubSub) Subscribe(ctx context.Context, topic string, opts pubsub.SubOptions) (chan pubsub.Message, error) {
	appCh := make(chan pubsub.Message)
	queueCh := make(chan pubsub.Message, 1024)

	if len(opts.Backoff) > 0 {
		opts.AckWait = opts.Backoff[0]
	}

	sub, err := n.js.Subscribe(topic, func(msg *nats.Msg) {
		queueCh <- pubsub.Message{
			Data:    msg.Data,
			Arrival: time.Now(),
			Ack:     func() error { return msg.Ack() },
			Nack:    func() error { return msg.Nak() },
		}
	},
		nats.ManualAck(),
		nats.AckWait(opts.AckWait),
		nats.MaxDeliver(opts.MaxDelivers),
		nats.BackOff(opts.Backoff),
	)
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
				if time.Since(data.Arrival) > opts.AckWait {
					continue
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
	appCh := make(chan pubsub.Message)
	queueCh := make(chan pubsub.Message, 1024)

	if len(opts.Backoff) > 0 {
		opts.AckWait = opts.Backoff[0]
	}

	workQueue := strings.ReplaceAll(topic, ".", "-")
	sub, err := n.js.QueueSubscribe(topic, workQueue, func(msg *nats.Msg) {
		queueCh <- pubsub.Message{
			Data:    msg.Data,
			Arrival: time.Now(),
			Ack:     func() error { return msg.Ack() },
			Nack:    func() error { return msg.Nak() },
		}
	},
		nats.ManualAck(),
		nats.AckWait(opts.AckWait),
		nats.MaxDeliver(opts.MaxDelivers),
		nats.BackOff(opts.Backoff),
	)
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
				if time.Since(data.Arrival) > opts.AckWait {
					continue
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

func (n *NatsPubSub) NewDedupStore(_ context.Context, instanceID string) (dedup.Store, error) {
	bucket := fmt.Sprintf("gobus_dedup_%s", strings.ReplaceAll(instanceID, "-", "_"))
	return dedup.NewNatsStore(n.js, bucket, 0)
}
