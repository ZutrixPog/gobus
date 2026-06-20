package mqtt

import (
	"context"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/zutrixpog/gobus/pubsub"
)

var (
	_ pubsub.PubSub = (*MqttPubSub)(nil)
)

type MqttPubSub struct {
	client     mqtt.Client
	bufferSize int
	mu         sync.RWMutex
	topics     map[string]*topicHandler
}

type topicHandler struct {
	mu       sync.RWMutex
	channels map[chan pubsub.Message]struct{}
}

type MqttConfig struct {
	BufferSize int
}

func NewMqttPubSub(client mqtt.Client, cfg MqttConfig) (*MqttPubSub, error) {
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 1024
	}

	mp := &MqttPubSub{
		client:     client,
		bufferSize: cfg.BufferSize,
		topics:     make(map[string]*topicHandler),
	}

	return mp, nil
}

func (m *MqttPubSub) Connected() bool {
	return m.client.IsConnected()
}

func (m *MqttPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	if !m.Connected() {
		return fmt.Errorf("not connected to MQTT broker")
	}

	token := m.client.Publish(topic, 0, false, data)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("MQTT publish failed on %s: %w", topic, token.Error())
	}

	return nil
}

func (m *MqttPubSub) Subscribe(ctx context.Context, topic string, opts pubsub.SubOptions) (chan pubsub.Message, error) {
	if !m.Connected() {
		return nil, fmt.Errorf("not connected to MQTT broker")
	}

	ch := make(chan pubsub.Message, m.bufferSize)

	m.mu.Lock()
	th, exists := m.topics[topic]
	if !exists {
		th = &topicHandler{
			channels: make(map[chan pubsub.Message]struct{}),
		}
		m.topics[topic] = th

		handler := m.createHandler(topic)
		token := m.client.Subscribe(topic, 0, handler)
		if token.Wait() && token.Error() != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("MQTT subscribe failed on %s: %w", topic, token.Error())
		}
	}
	th.channels[ch] = struct{}{}
	m.mu.Unlock()

	go func() {
		defer close(ch)
		<-ctx.Done()

		m.mu.Lock()
		if th, ok := m.topics[topic]; ok {
			delete(th.channels, ch)
			if len(th.channels) == 0 {
				delete(m.topics, topic)
				m.client.Unsubscribe(topic)
			}
		}
		m.mu.Unlock()
	}()

	return ch, nil
}

func (m *MqttPubSub) createHandler(topic string) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		pm := pubsub.Message{
			Data:    msg.Payload(),
			Arrival: time.Now(),
			Context: context.Background(),
			Ack: func() error {
				msg.Ack()
				return nil
			},
			Nack: func() error {
				return nil
			},
		}

		m.mu.RLock()
		th, exists := m.topics[topic]
		m.mu.RUnlock()

		if !exists {
			return
		}

		th.mu.RLock()
		for ch := range th.channels {
			select {
			case ch <- pm:
			default:
			}
		}
		th.mu.RUnlock()
	}
}

func (m *MqttPubSub) PublishOnce(ctx context.Context, topic string, data []byte) error {
	if !m.Connected() {
		return fmt.Errorf("not connected to MQTT broker")
	}

	token := m.client.Publish(topic, 1, false, data)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("MQTT publish once failed on %s: %w", topic, token.Error())
	}

	return nil
}

func (m *MqttPubSub) SubscribeOnce(ctx context.Context, topic string, opts pubsub.SubOptions) (chan pubsub.Message, error) {
	if !m.Connected() {
		return nil, fmt.Errorf("not connected to MQTT broker")
	}

	ch := make(chan pubsub.Message, m.bufferSize)
	received := make(map[uint16]bool)
	var mu sync.Mutex

	m.mu.Lock()
	th, exists := m.topics[topic]
	if !exists {
		th = &topicHandler{
			channels: make(map[chan pubsub.Message]struct{}),
		}
		m.topics[topic] = th

		handler := func(client mqtt.Client, msg mqtt.Message) {
			msgID := msg.MessageID()
			mu.Lock()
			if received[msgID] {
				mu.Unlock()
				return
			}
			received[msgID] = true
			mu.Unlock()

			pm := pubsub.Message{
				Data:    msg.Payload(),
				Arrival: time.Now(),
				Context: context.Background(),
				Ack: func() error {
					mu.Lock()
					delete(received, msgID)
					mu.Unlock()
					msg.Ack()
					return nil
				},
				Nack: func() error {
					mu.Lock()
					delete(received, msgID)
					mu.Unlock()
					return nil
				},
			}

			th.mu.RLock()
			for ch := range th.channels {
				select {
				case ch <- pm:
				default:
				}
			}
			th.mu.RUnlock()
		}

		token := m.client.Subscribe(topic, 1, handler)
		if token.Wait() && token.Error() != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("MQTT subscribe once failed on %s: %w", topic, token.Error())
		}
	}
	th.channels[ch] = struct{}{}
	m.mu.Unlock()

	go func() {
		defer close(ch)
		<-ctx.Done()

		m.mu.Lock()
		if th, ok := m.topics[topic]; ok {
			delete(th.channels, ch)
			if len(th.channels) == 0 {
				delete(m.topics, topic)
				m.client.Unsubscribe(topic)
			}
		}
		m.mu.Unlock()
	}()

	return ch, nil
}
