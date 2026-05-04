package auralog

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestSlogHandler(t *testing.T) {
	transport := &recordingTransport{}
	client, err := New(testConfig(), transport)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(NewSlogHandler(client, slog.LevelDebug))
	logger.With("service", "checkout").Error("slog failed", "order_id", "ord_1")
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if len(transport.singles) != 1 {
		t.Fatalf("singles = %d, want 1", len(transport.singles))
	}
	metadata := transport.singles[0].Metadata
	if metadata["service"] != "checkout" || metadata["order_id"] != "ord_1" {
		t.Fatalf("metadata mismatch: %+v", metadata)
	}
	grouped := slog.New(NewSlogHandler(client, slog.LevelDebug).WithGroup("request"))
	grouped.Warn("grouped", "id", "req_1")
	if err := client.FlushWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	request, ok := transport.batches[0][0].Metadata["request"].(Metadata)
	if !ok || request["id"] != "req_1" {
		t.Fatalf("group metadata mismatch: %+v", transport.batches[0][0].Metadata)
	}
	if !NewSlogHandler(client, slog.LevelWarn).Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("warn level should be enabled")
	}
	_ = client.ShutdownWithTimeout(time.Second)
}
