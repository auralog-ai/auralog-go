# Changelog

All notable changes to `auralog-go` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-beta.1] - 2026-05-04

### Added

- Initial beta Go SDK.
- Manual logging API with `Debug`, `Info`, `Warn`, `Error`, and `Fatal`.
- Background worker with batching, single-error priority queue, context-bounded `Flush`, and deterministic `Shutdown`.
- Standard library HTTP transport with request timeout and 4xx/5xx failure classification.
- Pluggable `Transport` interface for tests and custom production transports.
- `log/slog` handler integration.
- `Metadata` support with global metadata and supplier support.
- Panic reporting helper for explicit `defer client.Recover(ctx)` usage.

### Changed

- Documented all exported API surfaces for pkg.go.dev.
- Log methods now support slog-style alternating key/value metadata arguments.
- Flush waits on in-flight delivery notifications instead of polling.
- Retry requeue prepends surviving entries in one operation.
- HTTP transport self-logs are deduped and include `User-Agent: auralog-go/<version>`.

### Tests

- Added retry exhaustion, supplier panic, metadata argument, timestamp, slog group, and fuzz coverage.
- CI and release workflows now run tests with `-shuffle=on`.
