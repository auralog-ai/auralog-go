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

// Level is an Auralog wire log level.
type Level string

const (
	// Version is the SDK version reported by the Go module.
	Version = "0.1.0-beta.1"

	// LevelDebug is used for verbose diagnostic logs.
	LevelDebug Level = "debug"
	// LevelInfo is used for normal informational logs.
	LevelInfo Level = "info"
	// LevelWarn is used for warning logs.
	LevelWarn Level = "warn"
	// LevelError is used for error logs, which are sent through the single-log endpoint.
	LevelError Level = "error"
	// LevelFatal is used for fatal logs, which are sent through the single-log endpoint.
	LevelFatal Level = "fatal"
)

// Metadata is the structured metadata bag attached to an Auralog entry.
//
// Object metadata merges directly with global metadata. Scalar metadata passed
// to log methods is wrapped as {"value": ...}.
type Metadata map[string]any

// Config controls client behavior and ingest transport settings.
//
// Zero-valued optional fields are filled with production defaults by New.
// APIKey is required.
type Config struct {
	// APIKey is the Auralog project API key. It is required.
	APIKey string
	// Environment is the environment label attached to each log.
	Environment string
	// Endpoint overrides the Auralog ingest endpoint.
	Endpoint string
	// FlushInterval controls how often the background worker flushes batch logs.
	FlushInterval time.Duration
	// MaxBatchSize is the maximum number of non-error logs sent per batch request.
	MaxBatchSize int
	// MaxQueueSize is the maximum number of pending logs retained in memory.
	MaxQueueSize int
	// MaxRetryAttempts is the number of attempts before a retryable log is dropped.
	MaxRetryAttempts int
	// RetryInitialDelay is the first retry delay after a retryable delivery failure.
	RetryInitialDelay time.Duration
	// RetryMaxDelay caps exponential retry backoff.
	RetryMaxDelay time.Duration
	// HTTPTimeout is the default HTTP client's per-request timeout.
	HTTPTimeout time.Duration
	// ShutdownTimeout is the default shutdown budget used by helpers.
	ShutdownTimeout time.Duration
	// TraceID is attached to each log entry. If empty, New generates one.
	TraceID string
	// GlobalMetadata is static metadata merged into each log entry.
	GlobalMetadata Metadata
	// GlobalMetadataSupplier is called synchronously for each log entry.
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

// LogEntry is the JSON-serializable log payload sent to Auralog ingest.
type LogEntry struct {
	// Level is the Auralog wire log level.
	Level Level `json:"level"`
	// Message is the human-readable log message.
	Message string `json:"message"`
	// Environment is the environment label for the log.
	Environment string `json:"environment"`
	// Timestamp is the UTC RFC3339 millisecond timestamp.
	Timestamp string `json:"timestamp"`
	// Metadata contains structured context for the log.
	Metadata Metadata `json:"metadata,omitempty"`
	// StackTrace contains an optional stack trace for error and fatal logs.
	StackTrace string `json:"stackTrace,omitempty"`
	// TraceID is the distributed trace identifier attached to the log.
	TraceID string `json:"traceId"`
}

type queuedEntry struct {
	entry    LogEntry
	attempts int
}

// Client is the Auralog client.
//
// A Client is safe for concurrent use. Create one per process and share it
// across goroutines. The client owns a background flusher goroutine; call
// Shutdown to stop it deterministically.
type Client struct {
	mu        sync.Mutex
	config    Config
	transport Transport

	batchQueue   []queuedEntry
	singleQueue  []queuedEntry
	inFlight     int
	inFlightDone chan struct{}
	warnings     map[string]struct{}

	wake       chan struct{}
	stop       chan struct{}
	workerDone chan struct{}
	stopOnce   sync.Once
}

// New constructs a client with the supplied transport, or the default HTTP
// transport if none is provided.
//
// The returned client starts a background flusher goroutine. Call Shutdown when
// the host process, CLI command, test, or serverless handler is done logging.
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
		config:       config,
		transport:    t,
		inFlightDone: closedChannel(),
		warnings:     make(map[string]struct{}),
		wake:         make(chan struct{}, 1),
		stop:         make(chan struct{}),
		workerDone:   make(chan struct{}),
	}
	go client.worker()
	return client, nil
}

// Debug queues a debug log.
//
// Metadata arguments may be a single Metadata/map value, a single scalar value
// wrapped as {"value": ...}, or slog-style alternating key/value pairs.
func (c *Client) Debug(message string, args ...any) { c.Log(LevelDebug, message, args...) }

// Info queues an info log.
//
// Metadata arguments may be a single Metadata/map value, a single scalar value
// wrapped as {"value": ...}, or slog-style alternating key/value pairs.
func (c *Client) Info(message string, args ...any) { c.Log(LevelInfo, message, args...) }

// Warn queues a warning log.
//
// Metadata arguments may be a single Metadata/map value, a single scalar value
// wrapped as {"value": ...}, or slog-style alternating key/value pairs.
func (c *Client) Warn(message string, args ...any) { c.Log(LevelWarn, message, args...) }

// Error queues an error log for the single-log endpoint.
//
// Metadata arguments may be a single Metadata/map value, a single scalar value
// wrapped as {"value": ...}, or slog-style alternating key/value pairs.
func (c *Client) Error(message string, args ...any) { c.Log(LevelError, message, args...) }

// Fatal queues a fatal log for the single-log endpoint.
//
// Metadata arguments may be a single Metadata/map value, a single scalar value
// wrapped as {"value": ...}, or slog-style alternating key/value pairs.
func (c *Client) Fatal(message string, args ...any) { c.Log(LevelFatal, message, args...) }

// ErrorWithStack queues an error log with an explicit stack trace.
func (c *Client) ErrorWithStack(message string, stackTrace string, args ...any) {
	c.enqueue(c.buildEntry(LevelError, message, stackTrace, metadataFromArgs(args...)))
}

// FatalWithStack queues a fatal log with an explicit stack trace.
func (c *Client) FatalWithStack(message string, stackTrace string, args ...any) {
	c.enqueue(c.buildEntry(LevelFatal, message, stackTrace, metadataFromArgs(args...)))
}

// Log queues a log at level.
//
// Metadata arguments may be a single Metadata/map value, a single scalar value
// wrapped as {"value": ...}, or slog-style alternating key/value pairs.
func (c *Client) Log(level Level, message string, args ...any) {
	c.enqueue(c.buildEntry(level, message, "", metadataFromArgs(args...)))
}

// Flush synchronously drains queued and in-flight logs until ctx is done.
func (c *Client) Flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.flushUntilEmpty(ctx)
}

// FlushWithTimeout synchronously drains queued and in-flight logs using timeout
// as the flush budget.
func (c *Client) FlushWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Flush(ctx)
}

// Shutdown stops the background worker and drains pending logs until ctx is done.
//
// Shutdown is idempotent. If this client is installed as the package global,
// Shutdown also clears the package global so Init can be called again.
func (c *Client) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.stopOnce.Do(func() { close(c.stop) })
	defer clearGlobal(c)
	select {
	case <-c.workerDone:
	case <-ctx.Done():
		c.warnOnce("auralog: worker did not stop before shutdown timeout")
	}
	return c.flushUntilEmpty(ctx)
}

// ShutdownWithTimeout stops the background worker and drains pending logs using
// timeout as the shutdown budget.
func (c *Client) ShutdownWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Shutdown(ctx)
}

// TraceID returns the trace ID attached to newly built log entries.
func (c *Client) TraceID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.TraceID
}

// SetTraceID changes the trace ID attached to subsequently built log entries.
func (c *Client) SetTraceID(traceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.TraceID = traceID
}

// SetGlobalMetadata replaces static metadata merged into subsequently built log entries.
func (c *Client) SetGlobalMetadata(metadata Metadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.GlobalMetadata = cloneMetadata(metadata)
}

// SetGlobalMetadataSupplier sets a synchronous metadata supplier invoked for
// subsequently built log entries.
//
// Supplier panics are recovered and self-logged once; the original log still
// ships without supplier metadata.
func (c *Client) SetGlobalMetadataSupplier(supplier func() Metadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.GlobalMetadataSupplier = supplier
}

// Recover reports a recovered panic as a fatal Auralog entry, attempts a
// bounded shutdown, then re-panics.
//
// Use as defer client.Recover(ctx) at goroutine boundaries. The stack trace sent
// to Auralog is captured before re-panicking; the Go runtime's re-panic stack is
// the recover site.
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
		if done, ok := c.inFlightOnlyDone(); ok {
			select {
			case <-done:
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
		c.singleQueue = compactQueue(c.singleQueue)
		if c.inFlight == 0 {
			c.inFlightDone = make(chan struct{})
		}
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
	c.batchQueue = compactQueue(c.batchQueue)
	if c.inFlight == 0 {
		c.inFlightDone = make(chan struct{})
	}
	c.inFlight++
	return entries, false
}

func (c *Client) finishInFlight() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight--
	if c.inFlight == 0 {
		close(c.inFlightDone)
	}
}

func (c *Client) empty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batchQueue) == 0 && len(c.singleQueue) == 0 && c.inFlight == 0
}

func (c *Client) inFlightOnlyDone() (<-chan struct{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batchQueue) == 0 && len(c.singleQueue) == 0 && c.inFlight > 0 {
		return c.inFlightDone, true
	}
	return nil, false
}

func (c *Client) enqueue(entry LogEntry) {
	immediate := entry.Level == LevelError || entry.Level == LevelFatal
	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.batchQueue)+len(c.singleQueue) >= c.config.MaxQueueSize {
		if len(c.batchQueue) > 0 {
			c.batchQueue = c.batchQueue[1:]
			c.batchQueue = compactQueue(c.batchQueue)
		} else {
			c.singleQueue = c.singleQueue[1:]
			c.singleQueue = compactQueue(c.singleQueue)
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
	retryable := make([]queuedEntry, 0, len(entries))
	for i := range entries {
		entries[i].attempts++
		if entries[i].attempts >= c.config.MaxRetryAttempts {
			dropped = true
			continue
		}
		retryable = append(retryable, entries[i])
	}

	c.mu.Lock()
	if len(retryable) > 0 {
		if single {
			c.singleQueue = append(retryable, c.singleQueue...)
		} else {
			c.batchQueue = append(retryable, c.batchQueue...)
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
	base := cloneMetadata(c.config.GlobalMetadata)
	supplier := c.config.GlobalMetadataSupplier
	c.mu.Unlock()

	entry := LogEntry{
		Level:       level,
		Message:     message,
		Environment: environment,
		Timestamp:   UTCTimestampMillis(time.Now()),
		Metadata:    c.mergeMetadata(metadata, base, supplier),
		TraceID:     traceID,
	}
	if stackTrace != "" {
		entry.StackTrace = stackTrace
	}
	return entry
}

func (c *Client) mergeMetadata(metadata any, base Metadata, supplier func() Metadata) Metadata {
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

func metadataFromArgs(args ...any) any {
	if len(args) == 0 {
		return nil
	}
	if len(args) == 1 {
		return args[0]
	}
	metadata := Metadata{}
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			return args
		}
		if i+1 >= len(args) {
			metadata[key] = nil
			break
		}
		metadata[key] = args[i+1]
	}
	return metadata
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

func closedChannel() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func compactQueue(queue []queuedEntry) []queuedEntry {
	if len(queue) == 0 {
		return nil
	}
	if cap(queue) > 64 && cap(queue) > len(queue)*2 {
		return append([]queuedEntry(nil), queue...)
	}
	return queue
}

// UTCTimestampMillis formats t as UTC RFC3339 with millisecond precision and a
// trailing Z, matching the Auralog SDK wire format.
func UTCTimestampMillis(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// GenerateTraceID returns a UUIDv4-compatible trace ID.
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
