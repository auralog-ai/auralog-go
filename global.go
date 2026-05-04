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

func Global() *Client {
	globalMu.Lock()
	defer globalMu.Unlock()
	return globalClient
}

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

func ShutdownWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return Shutdown(ctx)
}

func Debug(message string, metadata ...any) {
	if client := Global(); client != nil {
		client.Debug(message, metadata...)
	}
}

func Info(message string, metadata ...any) {
	if client := Global(); client != nil {
		client.Info(message, metadata...)
	}
}

func Warn(message string, metadata ...any) {
	if client := Global(); client != nil {
		client.Warn(message, metadata...)
	}
}

func Error(message string, metadata ...any) {
	if client := Global(); client != nil {
		client.Error(message, metadata...)
	}
}

func Fatal(message string, metadata ...any) {
	if client := Global(); client != nil {
		client.Fatal(message, metadata...)
	}
}
