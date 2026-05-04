package auralog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"time"
)

type recordingTransport struct {
	mu      sync.Mutex
	results []SendResult
	batches [][]LogEntry
	singles []LogEntry
}

func (t *recordingTransport) SendBatch(_ context.Context, entries []LogEntry) SendResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.batches = append(t.batches, append([]LogEntry(nil), entries...))
	return t.next()
}

func (t *recordingTransport) SendSingle(_ context.Context, entry LogEntry) SendResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.singles = append(t.singles, entry)
	return t.next()
}

func (t *recordingTransport) next() SendResult {
	if len(t.results) == 0 {
		return SendSuccess
	}
	result := t.results[0]
	t.results = t.results[1:]
	return result
}

type blockingTransport struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	singles []LogEntry
}

func newBlockingTransport() *blockingTransport {
	return &blockingTransport{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (t *blockingTransport) SendBatch(_ context.Context, _ []LogEntry) SendResult {
	return SendSuccess
}

func (t *blockingTransport) SendSingle(_ context.Context, entry LogEntry) SendResult {
	t.mu.Lock()
	t.singles = append(t.singles, entry)
	t.mu.Unlock()
	t.once.Do(func() { close(t.entered) })
	<-t.release
	return SendSuccess
}

func testConfig() Config {
	return Config{
		APIKey:            "aura_test",
		Environment:       "test",
		FlushInterval:     time.Hour,
		RetryInitialDelay: time.Millisecond,
		RetryMaxDelay:     time.Millisecond,
		ShutdownTimeout:   200 * time.Millisecond,
	}
}

func TestWireFormatAndMetadata(t *testing.T) {
	transport := &recordingTransport{}
	client, err := New(testConfig(), transport)
	if err != nil {
		t.Fatal(err)
	}
	client.SetGlobalMetadata(Metadata{"service": "checkout"})
	client.Info("started", Metadata{"order_id": "ord_1"})
	client.Error("failed", Metadata{"reason": "declined"})
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}

	if len(transport.batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(transport.batches))
	}
	if len(transport.singles) != 1 {
		t.Fatalf("singles = %d, want 1", len(transport.singles))
	}
	entry := transport.batches[0][0]
	if entry.Level != LevelInfo || entry.Environment != "test" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if entry.Metadata["service"] != "checkout" || entry.Metadata["order_id"] != "ord_1" {
		t.Fatalf("metadata mismatch: %+v", entry.Metadata)
	}
	if ok, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`, entry.Timestamp); !ok {
		t.Fatalf("timestamp missing RFC3339 milliseconds: %q", entry.Timestamp)
	}
	_ = client.ShutdownWithTimeout(time.Second)
}

func TestMetadataArguments(t *testing.T) {
	transport := &recordingTransport{}
	client, err := New(testConfig(), transport)
	if err != nil {
		t.Fatal(err)
	}
	client.Info("kv", "user_id", "u_1", "attempt", 2)
	client.Info("scalar", "hello")
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if transport.batches[0][0].Metadata["user_id"] != "u_1" ||
		transport.batches[0][0].Metadata["attempt"] != 2 {
		t.Fatalf("key/value metadata mismatch: %+v", transport.batches[0][0].Metadata)
	}
	if transport.batches[0][1].Metadata["value"] != "hello" {
		t.Fatalf("scalar metadata mismatch: %+v", transport.batches[0][1].Metadata)
	}
	_ = client.ShutdownWithTimeout(time.Second)
}

func TestFlushDrainsAllBatches(t *testing.T) {
	transport := &recordingTransport{}
	cfg := testConfig()
	cfg.MaxBatchSize = 50
	client, err := New(cfg, transport)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 120; i++ {
		client.Info("bulk", Metadata{"index": i})
	}
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, batch := range transport.batches {
		total += len(batch)
	}
	if len(transport.batches) != 3 || total != 120 {
		t.Fatalf("batch split = %d/%d, want 3/120", len(transport.batches), total)
	}
	_ = client.ShutdownWithTimeout(time.Second)
}

func TestFlushWaitsForInFlightSend(t *testing.T) {
	transport := newBlockingTransport()
	client, err := New(testConfig(), transport)
	if err != nil {
		t.Fatal(err)
	}
	client.Error("in flight")
	<-transport.entered

	flushed := make(chan error, 1)
	go func() { flushed <- client.FlushWithTimeout(time.Second) }()
	select {
	case err := <-flushed:
		t.Fatalf("flush returned before in-flight send finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(transport.release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	_ = client.ShutdownWithTimeout(time.Second)
}

func TestRetryExhaustionDropsEntries(t *testing.T) {
	transport := &recordingTransport{
		results: []SendResult{SendRetryableFailure, SendRetryableFailure, SendRetryableFailure},
	}
	cfg := testConfig()
	cfg.MaxRetryAttempts = 2
	client, err := New(cfg, transport)
	if err != nil {
		t.Fatal(err)
	}
	client.Info("drop me")
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if len(transport.batches) != 2 {
		t.Fatalf("attempts = %d, want 2", len(transport.batches))
	}
	if !client.empty() {
		t.Fatal("expected retry-exhausted queue to be empty")
	}
	_ = client.ShutdownWithTimeout(time.Second)
}

func TestRetryPermanentFailureAndScalarMetadata(t *testing.T) {
	retrying := &recordingTransport{results: []SendResult{SendRetryableFailure, SendSuccess}}
	client, err := New(testConfig(), retrying)
	if err != nil {
		t.Fatal(err)
	}
	client.Info("retry")
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if len(retrying.batches) != 2 {
		t.Fatalf("retry batches = %d, want 2", len(retrying.batches))
	}
	_ = client.ShutdownWithTimeout(time.Second)

	permanent := &recordingTransport{results: []SendResult{SendPermanentFailure}}
	client, err = New(testConfig(), permanent)
	if err != nil {
		t.Fatal(err)
	}
	client.Info("scalar", "hello")
	client.Error("bad auth")
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if permanent.batches[0][0].Metadata["value"] != "hello" {
		t.Fatalf("scalar metadata not wrapped: %+v", permanent.batches[0][0].Metadata)
	}
	if len(permanent.singles) != 1 {
		t.Fatalf("permanent singles = %d, want 1", len(permanent.singles))
	}
	_ = client.ShutdownWithTimeout(time.Second)
}

func TestSupplierPanicDoesNotCrashLogging(t *testing.T) {
	transport := &recordingTransport{}
	client, err := New(testConfig(), transport)
	if err != nil {
		t.Fatal(err)
	}
	client.SetGlobalMetadataSupplier(func() Metadata {
		panic("supplier exploded")
	})
	client.Info("still ships", Metadata{"local": true})
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if len(transport.batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(transport.batches))
	}
	if transport.batches[0][0].Metadata["local"] != true {
		t.Fatalf("local metadata missing: %+v", transport.batches[0][0].Metadata)
	}
	_ = client.ShutdownWithTimeout(time.Second)
}

func TestRuntimeUpdatesAndValidation(t *testing.T) {
	transport := &recordingTransport{}
	cfg := testConfig()
	cfg.MaxBatchSize = 1
	cfg.TraceID = "trace-one"
	client, err := New(cfg, transport)
	if err != nil {
		t.Fatal(err)
	}
	client.SetGlobalMetadata(Metadata{"service": "one"})
	client.Info("first")
	client.SetTraceID("trace-two")
	client.SetGlobalMetadata(Metadata{"service": "two"})
	client.Info("second")
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if transport.batches[0][0].TraceID != "trace-one" || transport.batches[0][0].Metadata["service"] != "one" {
		t.Fatalf("first entry mismatch: %+v", transport.batches[0][0])
	}
	if transport.batches[1][0].TraceID != "trace-two" || transport.batches[1][0].Metadata["service"] != "two" {
		t.Fatalf("second entry mismatch: %+v", transport.batches[1][0])
	}
	_ = client.ShutdownWithTimeout(time.Second)

	bad := testConfig()
	bad.FlushInterval = -time.Second
	if _, err := New(bad, &recordingTransport{}); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestHTTPTransportWireAndFailureClassification(t *testing.T) {
	var payloads []map[string]any
	var userAgents []string
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		userAgents = append(userAgents, r.UserAgent())
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, payload)
		w.WriteHeader(status)
	}))
	defer server.Close()

	cfg := testConfig()
	cfg.Endpoint = server.URL
	transport := NewHTTPTransport(cfg)
	if got := transport.SendSingle(context.Background(), LogEntry{Level: LevelError, Message: "x"}); got != SendSuccess {
		t.Fatalf("2xx result = %v", got)
	}
	if payloads[0]["projectApiKey"] != "aura_test" {
		t.Fatalf("missing projectApiKey: %+v", payloads[0])
	}
	if userAgents[0] != "auralog-go/"+Version {
		t.Fatalf("User-Agent = %q", userAgents[0])
	}
	status = http.StatusUnauthorized
	if got := transport.SendSingle(context.Background(), LogEntry{}); got != SendPermanentFailure {
		t.Fatalf("4xx result = %v", got)
	}
	status = http.StatusInternalServerError
	if got := transport.SendSingle(context.Background(), LogEntry{}); got != SendRetryableFailure {
		t.Fatalf("5xx result = %v", got)
	}
}

func TestUTCTimestampMillis(t *testing.T) {
	timestamp := UTCTimestampMillis(time.Date(2026, 5, 4, 1, 2, 3, 456789000, time.FixedZone("JST", 9*60*60)))
	if timestamp != "2026-05-03T16:02:03.456Z" {
		t.Fatalf("timestamp = %q", timestamp)
	}
}

func FuzzGenerateTraceID(f *testing.F) {
	f.Add(1)
	f.Fuzz(func(t *testing.T, _ int) {
		traceID := GenerateTraceID()
		if ok, _ := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, traceID); !ok {
			t.Fatalf("invalid trace ID: %q", traceID)
		}
	})
}

func FuzzMetadataArguments(f *testing.F) {
	f.Add("user_id", "u_1")
	f.Fuzz(func(t *testing.T, key string, value string) {
		client := &Client{warnings: make(map[string]struct{})}
		metadata := client.mergeMetadata(metadataFromArgs(key, value), Metadata{"base": true}, nil)
		if metadata["base"] != true {
			t.Fatalf("base metadata missing: %+v", metadata)
		}
		if key != "" && metadata[key] != value {
			t.Fatalf("argument metadata missing: key=%q value=%q metadata=%+v", key, value, metadata)
		}
	})
}
