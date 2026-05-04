package auralog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

type Level string

const (
	Version = "0.1.0-beta.1"

	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
	LevelFatal Level = "fatal"
)

type Metadata map[string]any

type Config struct {
	APIKey                 string
	Environment            string
	Endpoint               string
	FlushInterval          time.Duration
	MaxBatchSize           int
	MaxQueueSize           int
	MaxRetryAttempts       int
	RetryInitialDelay      time.Duration
	RetryMaxDelay          time.Duration
	HTTPTimeout            time.Duration
	ShutdownTimeout        time.Duration
	TraceID                string
	GlobalMetadata         Metadata
	GlobalMetadataSupplier func() Metadata
}

func (c *Config) applyDefaults() {
	if c.Environment == "" {
		c.Environment = "production"
	}
	if c.Endpoint == "" {
		c.Endpoint = "https://ingest.auralog.ai"
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 5 * time.Second
	}
	if c.MaxBatchSize == 0 {
		c.MaxBatchSize = 50
	}
	if c.MaxQueueSize == 0 {
		c.MaxQueueSize = 1000
	}
	if c.MaxRetryAttempts == 0 {
		c.MaxRetryAttempts = 5
	}
	if c.RetryInitialDelay == 0 {
		c.RetryInitialDelay = time.Second
	}
	if c.RetryMaxDelay == 0 {
		c.RetryMaxDelay = 30 * time.Second
	}
	if c.HTTPTimeout == 0 {
		c.HTTPTimeout = 30 * time.Second
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 2 * time.Second
	}
	if c.TraceID == "" {
		c.TraceID = GenerateTraceID()
	}
}

func (c Config) validate() error {
	if c.APIKey == "" {
		return errors.New("auralog: APIKey is required")
	}
	if c.Environment == "" {
		return errors.New("auralog: Environment is required")
	}
	if c.Endpoint == "" {
		return errors.New("auralog: Endpoint is required")
	}
	if c.FlushInterval <= 0 || c.RetryInitialDelay <= 0 || c.RetryMaxDelay <= 0 ||
		c.HTTPTimeout <= 0 || c.ShutdownTimeout <= 0 {
		return errors.New("auralog: durations must be greater than zero")
	}
	if c.MaxBatchSize <= 0 || c.MaxQueueSize <= 0 || c.MaxRetryAttempts <= 0 {
		return errors.New("auralog: queue and retry sizes must be greater than zero")
	}
	if c.RetryMaxDelay < c.RetryInitialDelay {
		return errors.New("auralog: RetryMaxDelay must be >= RetryInitialDelay")
	}
	return nil
}

type LogEntry struct {
	Level       Level    `json:"level"`
	Message     string   `json:"message"`
	Environment string   `json:"environment"`
	Timestamp   string   `json:"timestamp"`
	Metadata    Metadata `json:"metadata,omitempty"`
	StackTrace  string   `json:"stackTrace,omitempty"`
	TraceID     string   `json:"traceId"`
}

type queuedEntry struct {
	entry    LogEntry
	attempts int
}

type Client struct {
	mu        sync.Mutex
	cond      *sync.Cond
	config    Config
	transport Transport

	batchQueue  []queuedEntry
	singleQueue []queuedEntry
	inFlight    int
	warnings    map[string]struct{}

	wake       chan struct{}
	stop       chan struct{}
	workerDone chan struct{}
	stopOnce   sync.Once
}

func New(config Config, transport ...Transport) (*Client, error) {
	config.applyDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}

	t := Transport(nil)
	if len(transport) > 0 {
		t = transport[0]
	}
	if t == nil {
		t = NewHTTPTransport(config)
	}

	client := &Client{
		config:     config,
		transport:  t,
		warnings:   make(map[string]struct{}),
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		workerDone: make(chan struct{}),
	}
	client.cond = sync.NewCond(&client.mu)
	go client.worker()
	return client, nil
}

func (c *Client) Debug(message string, metadata ...any) { c.Log(LevelDebug, message, metadata...) }
func (c *Client) Info(message string, metadata ...any)  { c.Log(LevelInfo, message, metadata...) }
func (c *Client) Warn(message string, metadata ...any)  { c.Log(LevelWarn, message, metadata...) }
func (c *Client) Error(message string, metadata ...any) { c.Log(LevelError, message, metadata...) }
func (c *Client) Fatal(message string, metadata ...any) { c.Log(LevelFatal, message, metadata...) }

func (c *Client) ErrorWithStack(message string, stackTrace string, metadata ...any) {
	c.enqueue(c.buildEntry(LevelError, message, stackTrace, metadataValue(metadata...)))
}

func (c *Client) FatalWithStack(message string, stackTrace string, metadata ...any) {
	c.enqueue(c.buildEntry(LevelFatal, message, stackTrace, metadataValue(metadata...)))
}

func (c *Client) Log(level Level, message string, metadata ...any) {
	c.enqueue(c.buildEntry(level, message, "", metadataValue(metadata...)))
}

func (c *Client) Flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.flushUntilEmpty(ctx)
}

func (c *Client) FlushWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Flush(ctx)
}

func (c *Client) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.stopOnce.Do(func() { close(c.stop) })
	select {
	case <-c.workerDone:
	case <-ctx.Done():
		c.warnOnce("auralog: worker did not stop before shutdown timeout")
	}
	return c.flushUntilEmpty(ctx)
}

func (c *Client) ShutdownWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Shutdown(ctx)
}

func (c *Client) TraceID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.TraceID
}

func (c *Client) SetTraceID(traceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.TraceID = traceID
}

func (c *Client) SetGlobalMetadata(metadata Metadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.GlobalMetadata = cloneMetadata(metadata)
}

func (c *Client) SetGlobalMetadataSupplier(supplier func() Metadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.GlobalMetadataSupplier = supplier
}

func (c *Client) Recover(ctx context.Context) {
	if value := recover(); value != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		c.FatalWithStack("panic recovered", string(debug.Stack()), Metadata{
			"panic": fmt.Sprint(value),
		})
		timeout := c.config.ShutdownTimeout
		if deadline, ok := ctx.Deadline(); ok {
			timeout = time.Until(deadline)
		}
		if timeout <= 0 {
			timeout = c.config.ShutdownTimeout
		}
		_ = c.ShutdownWithTimeout(timeout)
		panic(value)
	}
}

func (c *Client) worker() {
	defer close(c.workerDone)
	timer := time.NewTimer(c.config.FlushInterval)
	defer timer.Stop()
	backoff := c.config.RetryInitialDelay

	for {
		select {
		case <-timer.C:
		case <-c.wake:
		case <-c.stop:
			return
		}

		ok := c.flushOnce(context.Background())
		if ok {
			backoff = c.config.RetryInitialDelay
			resetTimer(timer, c.config.FlushInterval)
			continue
		}
		resetTimer(timer, backoff)
		backoff *= 2
		if backoff > c.config.RetryMaxDelay {
			backoff = c.config.RetryMaxDelay
		}
	}
}

func (c *Client) flushUntilEmpty(ctx context.Context) error {
	retryDelay := c.config.RetryInitialDelay
	for {
		if err := ctx.Err(); err != nil {
			c.warnOnce("auralog: flush timed out with pending logs")
			return err
		}
		if c.empty() {
			return nil
		}
		if c.waitingOnInFlightOnly() {
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				c.warnOnce("auralog: flush timed out with pending logs")
				return ctx.Err()
			}
			continue
		}
		ok := c.flushOnce(ctx)
		if ok {
			retryDelay = c.config.RetryInitialDelay
			continue
		}
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			c.warnOnce("auralog: retry budget exceeded during flush")
			return ctx.Err()
		}
	}
}

func (c *Client) flushOnce(ctx context.Context) bool {
	entries, single := c.popBatch()
	if len(entries) == 0 {
		return true
	}
	defer c.finishInFlight()

	var result SendResult
	if single {
		result = c.transport.SendSingle(ctx, entries[0].entry)
	} else {
		batch := make([]LogEntry, 0, len(entries))
		for _, entry := range entries {
			batch = append(batch, entry.entry)
		}
		result = c.transport.SendBatch(ctx, batch)
	}

	switch result {
	case SendSuccess:
		return true
	case SendPermanentFailure:
		c.warnOnce("auralog: dropping logs after non-retryable delivery failure")
		return true
	default:
		c.requeueOrDrop(entries, single)
		return false
	}
}

func (c *Client) popBatch() ([]queuedEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.singleQueue) > 0 {
		entry := c.singleQueue[0]
		c.singleQueue = c.singleQueue[1:]
		c.inFlight++
		return []queuedEntry{entry}, true
	}
	if len(c.batchQueue) == 0 {
		return nil, false
	}
	count := c.config.MaxBatchSize
	if count > len(c.batchQueue) {
		count = len(c.batchQueue)
	}
	entries := append([]queuedEntry(nil), c.batchQueue[:count]...)
	c.batchQueue = c.batchQueue[count:]
	c.inFlight++
	return entries, false
}

func (c *Client) finishInFlight() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight--
	c.cond.Broadcast()
}

func (c *Client) empty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batchQueue) == 0 && len(c.singleQueue) == 0 && c.inFlight == 0
}

func (c *Client) waitingOnInFlightOnly() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batchQueue) == 0 && len(c.singleQueue) == 0 && c.inFlight > 0
}

func (c *Client) enqueue(entry LogEntry) {
	immediate := entry.Level == LevelError || entry.Level == LevelFatal
	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.batchQueue)+len(c.singleQueue) >= c.config.MaxQueueSize {
		if len(c.batchQueue) > 0 {
			c.batchQueue = c.batchQueue[1:]
		} else {
			c.singleQueue = c.singleQueue[1:]
		}
	}

	queued := queuedEntry{entry: entry}
	if immediate {
		c.singleQueue = append(c.singleQueue, queued)
		c.notifyWorker()
	} else {
		c.batchQueue = append(c.batchQueue, queued)
	}
}

func (c *Client) requeueOrDrop(entries []queuedEntry, single bool) {
	dropped := false
	c.mu.Lock()
	for i := len(entries) - 1; i >= 0; i-- {
		entries[i].attempts++
		if entries[i].attempts >= c.config.MaxRetryAttempts {
			dropped = true
			continue
		}
		if single {
			c.singleQueue = append([]queuedEntry{entries[i]}, c.singleQueue...)
		} else {
			c.batchQueue = append([]queuedEntry{entries[i]}, c.batchQueue...)
		}
	}
	c.mu.Unlock()

	if dropped {
		c.warnOnce("auralog: dropping logs after retry attempts exhausted")
	}
}

func (c *Client) buildEntry(level Level, message string, stackTrace string, metadata any) LogEntry {
	c.mu.Lock()
	environment := c.config.Environment
	traceID := c.config.TraceID
	c.mu.Unlock()

	entry := LogEntry{
		Level:       level,
		Message:     message,
		Environment: environment,
		Timestamp:   UTCTimestampMillis(time.Now()),
		Metadata:    c.mergeMetadata(metadata),
		TraceID:     traceID,
	}
	if stackTrace != "" {
		entry.StackTrace = stackTrace
	}
	return entry
}

func (c *Client) mergeMetadata(metadata any) Metadata {
	c.mu.Lock()
	base := cloneMetadata(c.config.GlobalMetadata)
	supplier := c.config.GlobalMetadataSupplier
	c.mu.Unlock()

	out := Metadata{}
	for key, value := range base {
		out[key] = value
	}
	if supplier != nil {
		for key, value := range c.callSupplier(supplier) {
			out[key] = value
		}
	}

	switch typed := metadata.(type) {
	case nil:
	case Metadata:
		for key, value := range typed {
			out[key] = value
		}
	case map[string]any:
		for key, value := range typed {
			out[key] = value
		}
	default:
		out["value"] = typed
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *Client) callSupplier(supplier func() Metadata) (metadata Metadata) {
	defer func() {
		if value := recover(); value != nil {
			c.warnOnce(fmt.Sprintf("auralog: global metadata supplier failed: %v", value))
			metadata = nil
		}
	}()
	return supplier()
}

func (c *Client) warnOnce(message string) {
	c.mu.Lock()
	_, seen := c.warnings[message]
	if !seen {
		c.warnings[message] = struct{}{}
	}
	c.mu.Unlock()
	if !seen {
		fmt.Fprintln(os.Stderr, message)
	}
}

func (c *Client) notifyWorker() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func metadataValue(metadata ...any) any {
	if len(metadata) == 0 {
		return nil
	}
	return metadata[0]
}

func cloneMetadata(metadata Metadata) Metadata {
	if metadata == nil {
		return nil
	}
	clone := make(Metadata, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func UTCTimestampMillis(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func GenerateTraceID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		now := time.Now().UnixNano()
		for i := range bytes {
			bytes[i] = byte(now >> (i % 8 * 8))
		}
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	out := make([]byte, 36)
	hex.Encode(out[0:8], bytes[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], bytes[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], bytes[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], bytes[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], bytes[10:16])
	return string(out)
}
