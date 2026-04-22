package mqtt_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/zutrixpog/gobus/pubsub"
	mqttpkg "github.com/zutrixpog/gobus/pubsub/mqtt"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

var (
	testMqttps pubsub.PubSub
	brokerPort string
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		fmt.Printf("failed to create docker pool: %s\n", err.Error())
		os.Exit(1)
	}

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "eclipse-mosquitto",
		Tag:        "latest",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"1883/tcp": {{HostIP: "0.0.0.0", HostPort: "0"}},
		},
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
	})
	if err != nil {
		fmt.Printf("failed to start mosquitto container: %s\n", err.Error())
		os.Exit(1)
	}

	brokerPort = container.GetPort("1883/tcp")

	pool.MaxWait = 10 * time.Second
	if err := pool.Retry(func() error {
		opts := pahomqtt.NewClientOptions().
			AddBroker(fmt.Sprintf("tcp://localhost:%s", brokerPort)).
			SetClientID("test-client").
			SetAutoReconnect(true).
			SetConnectRetry(true)

		client := pahomqtt.NewClient(opts)
		token := client.Connect()
		if token.Wait() && token.Error() != nil {
			return token.Error()
		}
		client.Disconnect(0)
		return nil
	}); err != nil {
		fmt.Printf("failed to connect to mosquitto: %s\n", err.Error())
		container.Close()
		os.Exit(1)
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://localhost:%s", brokerPort)).
		SetClientID("test-main-client").
		SetAutoReconnect(true)

	client := pahomqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		fmt.Printf("failed to connect main client: %s\n", token.Error())
		container.Close()
		os.Exit(1)
	}

	testMqttps, err = mqttpkg.NewMqttPubSub(client, mqttpkg.MqttConfig{BufferSize: 1024})
	if err != nil {
		fmt.Printf("failed to create mqtt pubsub: %s\n", err.Error())
		container.Close()
		os.Exit(1)
	}

	code := m.Run()
	container.Close()
	os.Exit(code)
}

func TestConnected(t *testing.T) {
	require.NotNil(t, testMqttps)
	require.True(t, testMqttps.Connected())
}

func TestPubSub(t *testing.T) {
	ctx := context.Background()
	ps, ok := testMqttps.(pubsub.PubSub)
	require.True(t, ok)

	topic := "gobus/test"

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://localhost:%s", brokerPort)).
		SetClientID("test-subscriber-1").
		SetAutoReconnect(true)
	client1 := pahomqtt.NewClient(opts)
	if token := client1.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("failed to connect client1: %s", token.Error())
	}

	opts2 := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://localhost:%s", brokerPort)).
		SetClientID("test-subscriber-2").
		SetAutoReconnect(true)
	client2 := pahomqtt.NewClient(opts2)
	if token := client2.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("failed to connect client2: %s", token.Error())
	}

	msgChan1, err := ps.Subscribe(ctx, topic, pubsub.SubOptions{AckWait: time.Second * 5})
	require.Nil(t, err)
	msgChan2, err := ps.Subscribe(ctx, topic, pubsub.SubOptions{AckWait: time.Second * 5})
	require.Nil(t, err)

	t.Run("publish to the topic", func(t *testing.T) {
		err := ps.Publish(ctx, topic, []byte("hello"))
		require.Nil(t, err)
	})

	t.Run("all subscribers should receive the publication", func(t *testing.T) {
		time.Sleep(500 * time.Millisecond)
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*3)
		defer cancel()
		received := 0
		var mu sync.Mutex

		for received < 2 {
			select {
			case msg := <-msgChan1:
				msg.Ack()
				mu.Lock()
				received++
				mu.Unlock()
			case msg := <-msgChan2:
				msg.Ack()
				mu.Lock()
				received++
				mu.Unlock()
			case <-ctxTimeout.Done():
				if received == 2 {
					return
				}
				t.Fatalf("expected 2 messages, got %d", received)
				return
			}
		}
	})

	client1.Disconnect(0)
	client2.Disconnect(0)
}

func TestPubSubOnce(t *testing.T) {
	ctx := context.Background()
	ps, ok := testMqttps.(pubsub.PubSub)
	require.True(t, ok)

	topic := "gobus/test/queue"

	msgChan1, err := ps.SubscribeOnce(ctx, topic, pubsub.SubOptions{AckWait: time.Second * 5})
	require.Nil(t, err)

	t.Run("publish once to the topic", func(t *testing.T) {
		err := ps.PublishOnce(ctx, topic, []byte("queue message"))
		require.Nil(t, err)
	})

	t.Run("subscriber should receive the publication", func(t *testing.T) {
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*2)
		defer cancel()

		select {
		case msg := <-msgChan1:
			msg.Ack()
			require.Equal(t, []byte("queue message"), msg.Data)
		case <-ctxTimeout.Done():
			t.Fatal("expected to receive message")
		}
	})
}

func TestPublishErrors(t *testing.T) {
	ctx := context.Background()
	ps, ok := testMqttps.(pubsub.PubSub)
	require.True(t, ok)
	_ = ps

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://localhost:%s", brokerPort)).
		SetClientID("test-publish-client").
		SetAutoReconnect(true)
	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("failed to connect client: %s", token.Error())
	}
	defer client.Disconnect(0)

	ps2, err := mqttpkg.NewMqttPubSub(client, mqttpkg.MqttConfig{BufferSize: 1024})
	require.Nil(t, err)

	t.Run("disconnect and try to publish", func(t *testing.T) {
		client.Disconnect(0)
		time.Sleep(100 * time.Millisecond)
		err := ps2.Publish(ctx, "test/topic", []byte("hello"))
		require.NotNil(t, err)
	})
}
