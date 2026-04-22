# gobus

A tiny event bus library for Go that wraps pub/sub systems. Because sometimes you just want to send messages around without losing your mind.

## Why

Got tired of wiring up event buses differently for every project. This gives you one interface, plug in whatever broker you like.

## Quick Start

```go
import "github.com/zutrixpog/gobus"

type UserCreated struct {
    ID    int
    Email string
}

type GetUserRequest struct {
    ID int
}

type GetUserResponse struct {
    Name  string
    Email string
}

// Init with your favorite broker
bus.Init(yourPubSubAdapter, bus.BusConfig{
    RpcPrefix: "myapp",
})

// Say hello to handlers
bus.Handle("user.created", func(ctx context.Context, evt UserCreated, meta bus.Event) error {
    fmt.Printf("user %d created with email %s\n", evt.ID, evt.Email)
    return nil
})

// Or go full RPC style with type safety
bus.Handle("user.get", func(ctx context.Context, req GetUserRequest) (GetUserResponse, error) {
    return GetUserResponse{Name: "taco", Email: "taco@example.com"}, nil
})

// Send stuff
bus.Publish(ctx, "user.created", UserCreated{ID: 42, Email: "taco@example.com"})

// Or call and wait for a response
res, err := bus.Call[GetUserRequest, GetUserResponse](ctx, "user.get", GetUserRequest{ID: 42})
```

## Adapters

Pick your poison:

| Broker | Package | Notes |
|--------|---------|-------|
| NATS | `pubsub/nats` | JetStream for persistence |
| Redis | `pubsub/redis` | Streams for the queue stuff |
| MQTT | `pubsub/mqtt` | IoT vibes |

## Features Nobody Asked For But You Get Anyway

- **Middleware** - chain stuff before handlers
- **Retry logic** - with backoff, because networks lie
- **Correlation IDs** - for when things go sideways and you need to trace them
- **Graceful shutdown** - drain mode so you don't drop messages on the floor
- **Pluggable serializers** - JSON, Gob, or roll your own

## Testing

```bash
go test ./...
```

Uses Docker containers for integration tests (dockertest). Make sure Docker is running or those tests will cry.

MIT or whatever, use it, don't be a jerk about it.
