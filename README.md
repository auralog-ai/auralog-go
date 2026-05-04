# auralog-go (Beta)

Go SDK for [Auralog](https://auralog.ai) — agentic logging and application awareness.

Auralog uses Claude as an on-call engineer: it monitors your logs and errors, alerts you when something's wrong, and opens fix PRs automatically.

[![license](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

## Install

```bash
go get github.com/auralog-ai/auralog-go
```

The beta targets Go 1.22+ and uses only the standard library.

## Quick Start

```go
package main

import (
	"context"
	"os"
	"time"

	auralog "github.com/auralog-ai/auralog-go"
)

func main() {
	client, err := auralog.Init(auralog.Config{
		APIKey:      os.Getenv("AURALOG_API_KEY"),
		Environment: "production",
	})
	if err != nil {
		panic(err)
	}
	defer auralog.ShutdownWithTimeout(2 * time.Second)

	client.Info("user signed in", auralog.Metadata{"user_id": "123"})
	client.Error("payment failed", auralog.Metadata{"order_id": "abc"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Flush(ctx)
}
```

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `APIKey` | `string` | required | Auralog project API key |
| `Environment` | `string` | `production` | Environment label |
| `Endpoint` | `string` | `https://ingest.auralog.ai` | Ingest endpoint override |
| `FlushInterval` | `time.Duration` | `5s` | Time between background flushes |
| `MaxBatchSize` | `int` | `50` | Maximum entries per `/v1/logs` request |
| `MaxQueueSize` | `int` | `1000` | Maximum pending entries before dropping oldest |
| `MaxRetryAttempts` | `int` | `5` | Drop a failed log after this many attempts |
| `RetryInitialDelay` | `time.Duration` | `1s` | First retry delay |
| `RetryMaxDelay` | `time.Duration` | `30s` | Maximum retry delay |
| `HTTPTimeout` | `time.Duration` | `30s` | HTTP client request timeout |
| `ShutdownTimeout` | `time.Duration` | `2s` | Default caller-provided shutdown budget |
| `TraceID` | `string` | generated | Trace ID attached to every log |
| `GlobalMetadata` | `auralog.Metadata` | none | Static metadata merged into every log |
| `GlobalMetadataSupplier` | `func() Metadata` | none | Callable metadata supplier, invoked per log |

## Transport Semantics

- `Debug`, `Info`, and `Warn` logs batch to `/v1/logs`.
- `Error` and `Fatal` logs are prioritized to `/v1/logs/single`.
- `Flush(ctx)` drains queued and in-flight logs until the context is done.
- 4xx ingest responses are permanent failures and are not retried.
- 5xx ingest responses and network failures retry up to `MaxRetryAttempts`.
- Delivery failures are self-logged once to stderr.
- The project API key is sent in the JSON body as `projectApiKey`, matching the other Auralog SDKs and ingest wire format.

## Metadata

Metadata is `auralog.Metadata`, which is an alias for `map[string]any`. Scalar or array metadata is wrapped as `{ "value": ... }` so values are not silently discarded.

```go
client.SetGlobalMetadata(auralog.Metadata{"service": "checkout"})
client.SetGlobalMetadataSupplier(func() auralog.Metadata {
	return auralog.Metadata{"tenant": currentTenant()}
})
```

Supplier panics are recovered and self-logged once; the original log still ships.

Log methods accept either one metadata object, one scalar value, or slog-style
alternating key/value pairs:

```go
client.Info("user signed in", "user_id", "123", "plan", "pro")
client.Info("raw value", "hello")
```

## slog Integration

Use `NewSlogHandler` with Go's standard `log/slog` package:

```go
logger := slog.New(auralog.NewSlogHandler(client, slog.LevelInfo))
logger.Error("payment failed", "order_id", "abc")
```

Slog errors map to Auralog `error`, warnings map to `warn`, debug maps to `debug`, and other records map to `info`.

See `examples/slog` for a runnable example.

## Panic Recovery

Go does not expose a safe process-wide panic hook. Use `defer client.Recover(ctx)` in goroutines where you want panic reporting:

```go
func handleRequest(ctx context.Context, client *auralog.Client) {
	defer client.Recover(ctx)
	// work
}
```

`Recover` logs a fatal entry with the panic value and stack trace, attempts a bounded shutdown, and then re-panics.

See `examples/recover` for a runnable example.

## Development

```bash
go fmt ./...
go test ./...
go test -race -shuffle=on ./...
go vet ./...
```

## Documentation

Full docs at [docs.auralog.ai](https://docs.auralog.ai).

## Security

Found a vulnerability? See [SECURITY.md](./SECURITY.md) for how to report it.

## License

[MIT](./LICENSE) © James Thomas
