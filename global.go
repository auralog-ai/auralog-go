package auralog

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	globalMu     sync.Mutex
	globalClient *Client
)

// Init creates and installs the process-wide Auralog client.
//
// Init returns an error if a global client is already installed. Use New when
// you want multiple independent clients or a client that is not registered as
// the package global.
func Init(config Config) (*Client, error) {
	client, err := New(config)
	if err != nil {
		return nil, err
	}
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalClient != nil {
		_ = client.ShutdownWithTimeout(client.config.ShutdownTimeout)
		return nil, errors.New("auralog: global client is already initialized")
	}
	globalClient = client
	return client, nil
}

// Global returns the process-wide client installed by Init, if any.
func Global() *Client {
	globalMu.Lock()
	defer globalMu.Unlock()
	return globalClient
}

// Shutdown stops and flushes the process-wide client installed by Init.
//
// Shutdown clears the package global before draining so a later Init can
// install a new client.
func Shutdown(ctx context.Context) error {
	globalMu.Lock()
	client := globalClient
	globalClient = nil
	globalMu.Unlock()
	if client == nil {
		return nil
	}
	return client.Shutdown(ctx)
}

// ShutdownWithTimeout stops and flushes the process-wide client using timeout
// as the shutdown budget.
func ShutdownWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return Shutdown(ctx)
}

// Debug emits a debug log through the process-wide client, if one is installed.
func Debug(message string, args ...any) {
	if client := Global(); client != nil {
		client.Debug(message, args...)
	}
}

// Info emits an info log through the process-wide client, if one is installed.
func Info(message string, args ...any) {
	if client := Global(); client != nil {
		client.Info(message, args...)
	}
}

// Warn emits a warning log through the process-wide client, if one is installed.
func Warn(message string, args ...any) {
	if client := Global(); client != nil {
		client.Warn(message, args...)
	}
}

// Error emits an error log through the process-wide client, if one is installed.
func Error(message string, args ...any) {
	if client := Global(); client != nil {
		client.Error(message, args...)
	}
}

// Fatal emits a fatal log through the process-wide client, if one is installed.
func Fatal(message string, args ...any) {
	if client := Global(); client != nil {
		client.Fatal(message, args...)
	}
}

func clearGlobal(client *Client) {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalClient == client {
		globalClient = nil
	}
}
