package pubsub

import (
	"context"
	"sync"
	"time"
)

type MockPubSub struct {
	mu        sync.RWMutex
	subs      map[string][]chan Message
	published []Message
	connected bool
}

func NewMockPubSub() *MockPubSub {
	return &MockPubSub{
		subs:      make(map[string][]chan Message),
		connected: true,
	}
}

func (m *MockPubSub) Connected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connected
}

func (m *MockPubSub) SetConnected(connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = connected
}

func (m *MockPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	noopAck := func() error { return nil }
	msg := Message{Data: data, Arrival: time.Now(), Context: ctx, Ack: noopAck, Nack: noopAck}

	m.published = append(m.published, msg)

	for _, ch := range m.subs[topic] {
		select {
		case ch <- msg:
		default:
		}
	}
	return nil
}

func (m *MockPubSub) Subscribe(ctx context.Context, topic string, opts SubOptions) (chan Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan Message, 1024)
	m.subs[topic] = append(m.subs[topic], ch)
	return ch, nil
}

func (m *MockPubSub) PublishOnce(ctx context.Context, topic string, data []byte) error {
	return m.Publish(ctx, topic, data)
}

func (m *MockPubSub) SubscribeOnce(ctx context.Context, topic string, opts SubOptions) (chan Message, error) {
	return m.Subscribe(ctx, topic, opts)
}

func (m *MockPubSub) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = nil
	m.subs = make(map[string][]chan Message)
}

func (m *MockPubSub) PublishedMessages() []Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.published
}

func (m *MockPubSub) SubscriberCount(topic string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subs[topic])
}

func (m *MockPubSub) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for topic, subs := range m.subs {
		for _, ch := range subs {
			close(ch)
		}
		delete(m.subs, topic)
	}
}
