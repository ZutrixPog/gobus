package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zutrixpog/gobus/pubsub"
)

type EventType string

const (
	EVENT_MSG = "msg"
	EVENT_RPC = "rpc"
)

var (
	ErrNotInit    = errors.New("bus is not initialized")
	ErrNotStarted = errors.New("bus is not started")
	ErrSerDe      = errors.New("failed to serialize/deserialize event")
	ErrQueueFull  = errors.New("retry queue is full")
	ErrRpcTimeout = errors.New("rpc timeout")
	ErrDraining   = errors.New("bus is draining, not accepting new events")

	_bus *Bus
)

type (
	ErrorResponse struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}

	Event struct {
		CorrelationID string    `json:"correlation_id"`
		TraceID       string    `json:"trace_id"`
		Type          EventType `json:"event_type"`
		UserID        string    `json:"user_id"`

		Topic   string `json:"topic"`
		Payload []byte `json:"payload"`
		retry   int
		backoff time.Duration

		Time int64 `json:"published"`
	}

	EventStore interface {
		LogEvent(ctx context.Context, evt Event)
	}

	RpcWrapper[T any] struct {
		ResTopic string `json:"res_topic"`
		Payload  T      `json:"payload"`
	}

	RpcFunc[T any, V any] func(ctx context.Context, req T) (V, error)

	HandlerFunc[T any] func(ctx context.Context, evt T, meta Event) error

	Middleware[T any] func(next HandlerFunc[T]) HandlerFunc[T]

	HandlerWrapper struct {
		fn           func(ctx context.Context, msg []byte) error
		retry        int
		middlewares  []any
		backoff      time.Duration
		backpressure bool
		concurrency  int
		topic        string
		once         bool
	}

	BusConfig struct {
		RpcPrefix  string
		Serializer Serializer

		PublishQueueSize int
		LogChannelSize   int

		MonitorInterval time.Duration
		RestartDelay    time.Duration

		HandlerAckWait time.Duration
		RpcAckWait     time.Duration
		RpcTimeout     time.Duration
	}

	LogLevel string

	LogEntry struct {
		Level   LogLevel
		Message string
		Err     error
		Time    time.Time
		Fields  map[string]any
	}

	Bus struct {
		ps         pubsub.PubSub
		cfg        BusConfig
		serializer Serializer

		handlers sync.Map
		pubQueue chan Event
		started  bool
		draining bool

		ctx    context.Context
		cancel context.CancelFunc
		wg     sync.WaitGroup

		logCh     chan LogEntry
		startedAt time.Time
	}
)

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

type RpcOptions struct {
	Timeout time.Duration
}

type RpcOption func(*RpcOptions)

func WithRpcTimeout(d time.Duration) RpcOption {
	return func(o *RpcOptions) {
		o.Timeout = d
	}
}

func Init(ps pubsub.PubSub, cfg BusConfig) *Bus {
	if _bus == nil {
		_, cancel := context.WithCancel(context.Background())

		if cfg.RpcPrefix == "" {
			cfg.RpcPrefix = "gobus"
		}
		if cfg.Serializer == nil {
			cfg.Serializer = DefaultSerializer()
		}
		if cfg.PublishQueueSize == 0 {
			cfg.PublishQueueSize = 100
		}
		if cfg.LogChannelSize == 0 {
			cfg.LogChannelSize = 1024
		}
		if cfg.MonitorInterval == 0 {
			cfg.MonitorInterval = 10 * time.Second
		}
		if cfg.RestartDelay == 0 {
			cfg.RestartDelay = 2 * time.Second
		}
		if cfg.HandlerAckWait == 0 {
			cfg.HandlerAckWait = 30 * time.Second
		}
		if cfg.RpcAckWait == 0 {
			cfg.RpcAckWait = 10 * time.Second
		}
		if cfg.RpcTimeout == 0 {
			cfg.RpcTimeout = 3 * time.Second
		}

		_bus = &Bus{
			ps:         ps,
			cfg:        cfg,
			serializer: cfg.Serializer,

			handlers: sync.Map{},
			pubQueue: make(chan Event, cfg.PublishQueueSize),
			started:  false,

			ctx:    context.Background(),
			cancel: cancel,
			wg:     sync.WaitGroup{},

			logCh: make(chan LogEntry, cfg.LogChannelSize),
		}
	}

	return _bus
}

func Logs() <-chan LogEntry {
	if _bus == nil {
		ch := make(chan LogEntry)
		close(ch)
		return ch
	}
	return _bus.logCh
}

func (b *Bus) log(level LogLevel, msg string, err error, fields map[string]any) {
	entry := LogEntry{
		Level:   level,
		Message: msg,
		Err:     err,
		Time:    time.Now(),
		Fields:  fields,
	}
	select {
	case b.logCh <- entry:
	default:
	}
}

func (b *Bus) monitor(ctx context.Context) {
	b.wg.Go(func() {
		defer b.wg.Done()
		ticker := time.NewTicker(b.cfg.MonitorInterval)
		defer ticker.Stop()

		var consecutiveFailures int
		const maxConsecutiveFailures = 3

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !b.ps.Connected() {
					consecutiveFailures++
					if consecutiveFailures >= maxConsecutiveFailures {
						b.log(LogLevelError, "pubsub connection lost after max retries, manual restart required", nil, nil)
						return
					}
					b.log(LogLevelWarn, fmt.Sprintf("pubsub disconnected, restart attempt %d/%d", consecutiveFailures, maxConsecutiveFailures), nil, nil)
					Restart()
				} else {
					consecutiveFailures = 0
				}
			}
		}
	})
}

func (b *Bus) startPublisher(ctx context.Context) {
	b.wg.Go(func() {
		for {
			select {
			case evt := <-b.pubQueue:
				b.wg.Go(func() {
					eventPayload, err := b.serializer.Encode(evt)
					if err != nil {
						b.log(LogLevelError, "failed to serialize event", err, map[string]any{"topic": evt.Topic})
						return
					}

					err = b.ps.Publish(ctx, evt.Topic, eventPayload)
					if err != nil {
						b.log(LogLevelError, fmt.Sprintf("failed to publish event, %d retries left", evt.retry), err, map[string]any{"topic": evt.Topic})

						for evt.retry > 0 {
							err = b.ps.Publish(ctx, evt.Topic, evt.Payload)
							if err == nil {
								b.log(LogLevelInfo, fmt.Sprintf("published event on %s topic", evt.Topic), nil, nil)
								break
							}

							b.log(LogLevelWarn, "event publish retry failed", err, map[string]any{"topic": evt.Topic})
							time.Sleep(evt.backoff)

							evt.retry -= 1
							evt.backoff *= 2
						}
					}
				})
			case <-ctx.Done():
				return
			}
		}
	})
}

func (b *Bus) startHandler(ctx context.Context) {
	wq, ok := b.ps.(pubsub.WorkQueue)

	b.handlers.Range(func(key, value any) bool {
		topic := key.(string)
		hw := *value.(*HandlerWrapper)

		backoffs := make([]time.Duration, 0, hw.retry)
		for range hw.retry {
			backoffs = append(backoffs, hw.backoff)
			hw.backoff *= 2
		}

		var subch chan pubsub.Message
		var err error
		if hw.once {
			if !ok {
				panic(fmt.Errorf("pub/sub does not support SubscribeOnce, cannot use WithExactlyOnce on topic %s", topic))
			}
			subch, err = wq.SubscribeOnce(ctx, topic, pubsub.SubOptions{
				AckWait:     b.cfg.HandlerAckWait,
				MaxDelivers: hw.retry,
				Backoff:     backoffs,
			})
		} else {
			subch, err = b.ps.Subscribe(ctx, topic, pubsub.SubOptions{
				AckWait:     b.cfg.HandlerAckWait,
				MaxDelivers: hw.retry,
				Backoff:     backoffs,
			})
		}
		if err != nil {
			b.log(LogLevelError, "failed to subscribe to pubsub", err, map[string]any{"topic": topic})
			time.Sleep(b.cfg.RestartDelay)
			Restart()
		}

		for range hw.concurrency {
			b.wg.Go(func() {
				for {
					select {
					case msg := <-subch:
						err := hw.fn(ctx, msg.Data)
						if err != nil {
							msg.Nack()
							b.log(LogLevelError, "failed to handle event", err, map[string]any{"topic": topic})
						} else {
							msg.Ack()
						}
					case <-ctx.Done():
						return
					}
				}
			})
		}

		return true
	})
}

func Handle[T any](topic string, fn HandlerFunc[T], options ...func(*HandlerWrapper)) {
	if _bus == nil {
		panic(ErrNotInit)
	}

	hw := HandlerWrapper{
		retry:        3,
		backoff:      time.Second,
		backpressure: false,
		concurrency:  1,
		topic:        topic,
		once:         false,
	}

	for _, opt := range options {
		opt(&hw)
	}

	for i := len(hw.middlewares) - 1; i >= 0; i-- {
		mid := hw.middlewares[i].(Middleware[T])
		fn = mid(fn)
	}

	hw.fn = func(ctx context.Context, msg []byte) error {
		var evt Event
		if err := _bus.serializer.Decode(msg, &evt); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}

		var req T
		if err := _bus.serializer.Decode(evt.Payload, &req); err != nil {
			return fmt.Errorf("failed to unmarshal event payload: %w", err)
		}
		return fn(ctx, req, evt)
	}

	_bus.handlers.Store(topic, &hw)
}

func WithConcurrency(n int) func(*HandlerWrapper) {
	return func(hw *HandlerWrapper) {
		hw.concurrency = n
	}
}

func WithBackPressure() func(*HandlerWrapper) {
	return func(hw *HandlerWrapper) {
		hw.backpressure = true
	}
}

func WithRetry(n int, backoff time.Duration) func(*HandlerWrapper) {
	return func(hw *HandlerWrapper) {
		hw.retry = n
		hw.backoff = backoff
	}
}

func WithExactlyOnce() func(*HandlerWrapper) {
	return func(hw *HandlerWrapper) {
		hw.once = true
	}
}

func WithMiddleware[T any](middlewares ...Middleware[T]) func(*HandlerWrapper) {
	return func(hw *HandlerWrapper) {
		for _, mid := range middlewares {
			hw.middlewares = append(hw.middlewares, mid)
		}
	}
}

func Publish(ctx context.Context, topic string, payload any, options ...func(*Event)) error {
	if _bus == nil {
		panic(ErrNotInit)
	}
	if !_bus.started {
		panic(ErrNotStarted)
	}
	if _bus.draining {
		return ErrDraining
	}

	payloadBytes, err := _bus.serializer.Encode(payload)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrSerDe, err.Error())
	}

	evt := Event{
		CorrelationID: uuid.New().String(),
		Type:          EVENT_MSG,
		Topic:         topic,
		Payload:       payloadBytes,
		retry:         0,
		backoff:       0,
		Time:          time.Now().Unix(),
	}

	for _, opt := range options {
		opt(&evt)
	}

	select {
	case _bus.pubQueue <- evt:
	default:
		<-_bus.pubQueue
		_bus.pubQueue <- evt
		return ErrQueueFull
	}
	return nil
}

func WithRetryQueue(n int, backoff time.Duration) func(*Event) {
	return func(evt *Event) {
		evt.retry = n
		evt.backoff = backoff
	}
}

func WithCorrelationID(id string) func(*Event) {
	return func(evt *Event) {
		evt.CorrelationID = id
	}
}

func WithTraceID(id string) func(*Event) {
	return func(evt *Event) {
		evt.TraceID = id
	}
}

func ID() string {
	return uuid.New().String()
}

func IsRunning() bool {
	if _bus == nil {
		return false
	}
	return _bus.started && !_bus.draining
}

func IsDraining() bool {
	if _bus == nil {
		return false
	}
	return _bus.draining
}

func StartedAt() time.Time {
	if _bus == nil {
		return time.Time{}
	}
	return _bus.startedAt
}

func HandlerCount() int {
	if _bus == nil {
		return 0
	}
	count := 0
	_bus.handlers.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func Handlers() []string {
	if _bus == nil {
		return nil
	}
	topics := make([]string, 0)
	_bus.handlers.Range(func(key, _ any) bool {
		topics = append(topics, key.(string))
		return true
	})
	return topics
}

func HasHandler(topic string) bool {
	if _bus == nil {
		return false
	}
	_, ok := _bus.handlers.Load(topic)
	return ok
}

func RemoveHandler(topic string) bool {
	if _bus == nil {
		return false
	}
	_, ok := _bus.handlers.LoadAndDelete(topic)
	return ok
}

func genTopicFromFname(fname string) (string, string) {
	uid := ID()

	reqTopic := fmt.Sprintf("%s.func-%s-req", _bus.cfg.RpcPrefix, fname)
	resTopic := fmt.Sprintf("%s.func-%s-res-%s", _bus.cfg.RpcPrefix, fname, uid)
	return reqTopic, resTopic
}

func Call[T any, V any](ctx context.Context, fname string, req T, opts ...RpcOption) (V, error) {
	if _bus == nil {
		panic(ErrNotInit)
	}

	cfg := RpcOptions{Timeout: _bus.cfg.RpcTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}

	var res V
	reqTopic, resTopic := genTopicFromFname(fname)

	payload, err := _bus.serializer.Encode(RpcWrapper[T]{
		ResTopic: resTopic,
		Payload:  req,
	})
	if err != nil {
		return res, err
	}

	resCh, err := _bus.ps.Subscribe(ctx, resTopic, pubsub.SubOptions{
		AckWait:     _bus.cfg.RpcAckWait,
		MaxDelivers: 2,
		Backoff:     []time.Duration{time.Second, time.Second * 2},
	})
	if err != nil {
		return res, err
	}
	defer close(resCh)

	err = _bus.ps.Publish(ctx, reqTopic, payload)
	if err != nil {
		return res, err
	}

	select {
	case msg := <-resCh:
		defer msg.Ack()

		var errRes ErrorResponse
		if err := _bus.serializer.Decode(msg.Data, &errRes); err == nil && errRes.Code != "" {
			return res, fmt.Errorf("%s: %s", errRes.Code, errRes.Message)
		}

		err := _bus.serializer.Decode(msg.Data, &res)
		if err != nil {
			return res, fmt.Errorf("%w: %s", ErrSerDe, err.Error())
		}

	case <-time.After(cfg.Timeout):
		return res, ErrRpcTimeout
	}

	return res, nil
}

func Serve[T any, V any](fname string, fn RpcFunc[T, V]) error {
	if _bus == nil {
		panic(ErrNotInit)
	}

	reqTopic, _ := genTopicFromFname(fname)

	resCh, err := _bus.ps.Subscribe(_bus.ctx, reqTopic, pubsub.SubOptions{
		AckWait:     _bus.cfg.RpcAckWait,
		MaxDelivers: 1,
		Backoff:     []time.Duration{time.Second * 2},
	})
	if err != nil {
		return err
	}

	respond := func(res any, topic, fname string, arrived time.Time) {
		dur := time.Since(arrived)
		if err, ok := res.(ErrorResponse); ok {
			_bus.log(LogLevelError, fmt.Sprintf("RPC - %s - %s", fname, dur), fmt.Errorf("code: %s, error: %s", err.Code, err.Message), nil)
		} else {
			_bus.log(LogLevelInfo, fmt.Sprintf("RPC - %s - %s - SUCCESS", fname, dur), nil, nil)
		}

		payload, err := _bus.serializer.Encode(res)
		if err != nil {
			_bus.log(LogLevelError, "failed to serialize RPC response", err, nil)
			return
		}
		_bus.ps.Publish(_bus.ctx, topic, payload)
	}

	_bus.wg.Go(func() {
		for {
			select {
			case msg := <-resCh:
				arrived := time.Now()
				msg.Ack()

				var req RpcWrapper[T]
				err := _bus.serializer.Decode(msg.Data, &req)
				if err != nil {
					continue
				}

				_bus.wg.Go(func() {
					res, err := fn(_bus.ctx, req.Payload)
					if err != nil {
						respond(ErrorResponse{Code: "ErrRpc", Message: err.Error()}, req.ResTopic, fname, arrived)
						return
					}

					respond(res, req.ResTopic, fname, arrived)
				})
			case <-_bus.ctx.Done():
				return
			}
		}
	})

	return nil
}

func Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	_bus.ctx = ctx
	_bus.cancel = cancel

	_bus.startPublisher(ctx)
	_bus.startHandler(ctx)

	_bus.started = true
	_bus.startedAt = time.Now()
	_bus.draining = false
	_bus.monitor(ctx)
	return nil
}

func Shutdown(opts ...func(*ShutdownOptions)) error {
	cfg := ShutdownOptions{Timeout: 30 * time.Second}
	for _, opt := range opts {
		opt(&cfg)
	}

	_bus.cancel()

	done := make(chan struct{})
	go func() {
		_bus.wg.Wait()
		_bus.started = false
		_bus.draining = false
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(cfg.Timeout):
		return fmt.Errorf("shutdown timed out after %v", cfg.Timeout)
	}
}

type ShutdownOptions struct {
	Timeout time.Duration
}

func WithShutdownTimeout(d time.Duration) func(*ShutdownOptions) {
	return func(o *ShutdownOptions) {
		o.Timeout = d
	}
}

func Restart() {
	_bus.log(LogLevelInfo, "restarting bus", nil, nil)
	Shutdown()
	time.Sleep(_bus.cfg.RestartDelay)
	Start()
}

func Drain(timeout time.Duration) error {
	if _bus == nil {
		return ErrNotInit
	}
	if !_bus.started {
		return ErrNotStarted
	}
	if _bus.draining {
		return nil
	}

	_bus.draining = true
	_bus.log(LogLevelInfo, "bus entering drain mode", nil, nil)

	timeoutCh := time.After(timeout)
	completeCh := DrainComplete()

	select {
	case <-completeCh:
		_bus.log(LogLevelInfo, "drain completed", nil, nil)
		return nil
	case <-timeoutCh:
		_bus.draining = false
		return fmt.Errorf("drain timed out after %v", timeout)
	}
}

func DrainComplete() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for len(_bus.pubQueue) > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		_bus.wg.Wait()
		close(ch)
	}()
	return ch
}
