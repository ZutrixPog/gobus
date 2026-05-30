# gobus

Event bus library for Go wrapping common pub/sub systems.

## Overview

gobus provides a unified interface for event-driven communication across multiple message brokers. It handles publish/subscribe patterns, RPC-style request/response, and worker queue semantics with automatic serialization, retry logic, and middleware support.

## Installation

```bash
go get github.com/zutrixpog/gobus
```

## Quick Start

```go
import (
    "context"
    "github.com/zutrixpog/gobus"
    "github.com/zutrixpog/gobus/pubsub/nats"
)

type UserCreated struct {
    ID    int    `json:"id"`
    Email string `json:"email"`
}

type GetUserRequest struct {
    ID int `json:"id"`
}

type GetUserResponse struct {
    Name  string `json:"name"`
    Email string `json:"email"`
}

func main() {
    conn, _ := nats.Connect("nats://localhost:4222")
    ps, _ := nats.NewNatsPubSub(conn, "gobus", []string{"gobus.*"}, 0)

    gobus.Init(ps, gobus.BusConfig{
        InstanceID: "order-svc-vm1",  // stable across restarts for dedup
        RpcPrefix:  "myapp",
    })

    // Event handler
    gobus.Handle("user.created", func(ctx context.Context, evt UserCreated, meta gobus.Event) error {
        fmt.Printf("user %d created: %s\n", evt.ID, evt.Email)
        return nil
    }, gobus.WithDedup[UserCreated](5*time.Minute))

    // RPC handler
    gobus.Handle("user.get", func(ctx context.Context, req GetUserRequest) (GetUserResponse, error) {
        return GetUserResponse{Name: "taco", Email: "taco@example.com"}, nil
    })

    gobus.Start()

    // Publish event
    gobus.Publish(context.Background(), "user.created", UserCreated{ID: 42, Email: "taco@example.com"})

    // RPC call
    res, _ := gobus.Call[GetUserRequest, GetUserResponse](context.Background(), "user.get", GetUserRequest{ID: 42})
    
    // Graceful shutdown
    gobus.Shutdown(gobus.WithShutdownTimeout(30 * time.Second))
}
```

## Pub/Sub Adapters

| Broker | Package | Notes |
|--------|---------|-------|
| NATS | `pubsub/nats` | JetStream for persistence and replay |
| Redis | `pubsub/redis` | Streams API for work queues |
| MQTT | `pubsub/mqtt` | QoS levels 0-1 |

## API Reference

### Initialization

```go
gobus.Init(ps pubsub.PubSub, config BusConfig)
```

**BusConfig** fields:

| Field | Type | Default | Description |
|-------|------|--------|-------------|
| InstanceID | string | random UUID | Stable identity for dedup scoping; set for restart resilience |
| RpcPrefix | string | "rpc" | Topic prefix for RPC calls |
| Serializer | Serializer | GobSerializer | Payload serialization |
| PublishQueueSize | int | 1024 | Channel buffer for publishing |
| LogChannelSize | int | 512 | Log entries buffer |
| RpcTimeout | time.Duration | 30s | Default RPC timeout |
| HandlerAckWait | time.Duration | 30s | Handler ack wait time |
| MonitorInterval | time.Duration | 5s | Status monitor tick |

### Handlers

```go
// Event handler (one-way)
gobus.Handle(topic string, handler func(ctx context.Context, T, meta Event) error, opts ...HandlerOpt)

// RPC handler (request/response)
gobus.Serve(endpoint string, handler func(ctx context.Context, T) (T, error), opts ...HandlerOpt)
```

**Handler Options:**

| Option | Description |
|--------|-------------|
| `WithConcurrency(n)` | Number of concurrent handlers |
| `WithRetry(n, delay)` | Retry n times with backoff delay |
| `WithBackPressure()` | Apply backpressure on full queue |
| `WithExactlyOnce()` | Broker-level once delivery |
| `WithDedup[T](ttl)` | Application-level dedup by correlation ID; persistent when using Redis/NATS backends |
| `WithMiddleware(...)` | Chain middleware functions |
| `WithRetryQueue(n, delay)` | Queue failed messages for retry |

### Publishing

```go
// Fire-and-forget
gobus.Publish(ctx context.Context, topic string, payload T, opts ...PublishOpt) error

// Request-response
gobus.Call[T, R](ctx context.Context, endpoint string, payload T, opts ...RpcOpt) (R, error)
```

**Publish Options:**

| Option | Description |
|--------|-------------|
| `WithCorrelationID(id)` | Set correlation ID |
| `WithTraceID(id)` | Set trace ID |
| `WithRpcTimeout(d)` | Override RPC timeout |

### Status

```go
gobus.IsRunning() bool
gobus.IsDraining() bool
gobus.HandlerCount() int
gobus.Handlers() []string
gobus.HasHandler(topic string) bool
gobus.RemoveHandler(topic string) bool
gobus.Logs() chan LogEntry
```

### Shutdown

```go
gobus.Shutdown(opts ...ShutdownOpt)
```

**Shutdown Options:**

| Option | Description |
|--------|-------------|
| `WithShutdownTimeout(d)` | Max time to drain |

## Serialization

Two built-in serializers:

- `gobus.DefaultSerializer()` - Gob (binary, faster)
- `gobus.JsonSerializer()` - JSON text

Custom serializer implements:

```go
type Serializer interface {
    Encode(any) ([]byte, error)
    Decode([]byte, any) error
}
```

## Logging

Receive log entries:

```go
go func() {
    for entry := range gobus.Logs() {
        fmt.Printf("[%s] %s\n", entry.Level, entry.Message)
    }
}()
```

Log levels: `debug`, `info`, `warn`, `error`

## Deduplication

`WithDedup[T](ttl)` prevents processing the same message twice within a TTL window by tracking correlation IDs.

### Backend auto-detection

| Backend | Store | Survives restart |
|---------|-------|------------------|
| Redis | `SET NX EX` with `gobus:dedup:<InstanceID>:<correlationID>` | Yes (TTL-managed) |
| NATS | JetStream KV bucket `gobus_dedup_<InstanceID>` | Yes (bucket TTL) |
| MQTT, Mock, others | In-memory map | No |

### Instance scoping

Each bus instance must have a unique `InstanceID`. Two instances with the same ID share dedup state (one instance drops what the other processed).

## Testing

```bash
go test ./... -cover
```

Integration tests require Docker (dockertest).
