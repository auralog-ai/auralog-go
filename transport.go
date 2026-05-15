package auralogs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// SendResult classifies an ingest delivery attempt.
type SendResult int

const (
	// SendSuccess means ingest accepted the payload.
	SendSuccess SendResult = iota
	// SendRetryableFailure means the client should retry the payload later.
	SendRetryableFailure
	// SendPermanentFailure means the payload should be dropped without retrying.
	SendPermanentFailure
)

// Transport sends Auralogs entries to an ingest backend.
//
// Implement this interface to plug in a custom HTTP stack, proxy, recorder, or
// test transport.
type Transport interface {
	// SendBatch sends non-error logs to the batch endpoint.
	SendBatch(ctx context.Context, entries []LogEntry) SendResult
	// SendSingle sends an error or fatal log to the single-log endpoint.
	SendSingle(ctx context.Context, entry LogEntry) SendResult
}

// HTTPTransport is the default standard-library HTTP ingest transport.
type HTTPTransport struct {
	apiKey   string
	endpoint string
	client   *http.Client
	mu       sync.Mutex
	warnings map[string]struct{}
}

// NewHTTPTransport constructs the default HTTP transport for config.
//
// NewHTTPTransport runs Config.validate so callers that bypass New cannot skip
// the https scheme guard or other config invariants. It returns an error when
// the configuration is invalid.
func NewHTTPTransport(config Config) (*HTTPTransport, error) {
	config.applyDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	timeout := config.HTTPTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &HTTPTransport{
		apiKey:   config.APIKey,
		endpoint: strings.TrimRight(config.Endpoint, "/"),
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		warnings: make(map[string]struct{}),
	}, nil
}

// SendBatch sends entries to /v1/logs.
func (t *HTTPTransport) SendBatch(ctx context.Context, entries []LogEntry) SendResult {
	return t.postJSON(ctx, "/v1/logs", map[string]any{
		"projectApiKey": t.apiKey,
		"logs":          entries,
	})
}

// SendSingle sends entry to /v1/logs/single.
func (t *HTTPTransport) SendSingle(ctx context.Context, entry LogEntry) SendResult {
	return t.postJSON(ctx, "/v1/logs/single", map[string]any{
		"projectApiKey": t.apiKey,
		"log":           entry,
	})
}

func (t *HTTPTransport) postJSON(ctx context.Context, path string, body any) SendResult {
	payload, err := json.Marshal(body)
	if err != nil {
		t.warnOnce(fmt.Sprintf("auralogs: failed to encode ingest payload: %v", err))
		return SendPermanentFailure
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.warnOnce(fmt.Sprintf("auralogs: failed to build ingest request: %v", err))
		return SendPermanentFailure
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "auralogs-go/"+Version)

	resp, err := t.client.Do(req)
	if err != nil {
		t.warnOnce(fmt.Sprintf("auralogs: HTTP delivery failure: %v", err))
		return SendRetryableFailure
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return SendSuccess
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		t.warnOnce(fmt.Sprintf("auralogs: refused to follow redirect from ingest (HTTP %d)", resp.StatusCode))
		return SendPermanentFailure
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		t.warnOnce(fmt.Sprintf("auralogs: non-retryable HTTP %d from ingest", resp.StatusCode))
		return SendPermanentFailure
	}
	t.warnOnce(fmt.Sprintf("auralogs: retryable HTTP %d from ingest", resp.StatusCode))
	return SendRetryableFailure
}

func (t *HTTPTransport) warnOnce(message string) {
	t.mu.Lock()
	_, seen := t.warnings[message]
	if !seen {
		t.warnings[message] = struct{}{}
	}
	t.mu.Unlock()
	if !seen {
		fmt.Fprintln(os.Stderr, message)
	}
}
