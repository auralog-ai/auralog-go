package auralog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type SendResult int

const (
	SendSuccess SendResult = iota
	SendRetryableFailure
	SendPermanentFailure
)

type Transport interface {
	SendBatch(ctx context.Context, entries []LogEntry) SendResult
	SendSingle(ctx context.Context, entry LogEntry) SendResult
}

type HTTPTransport struct {
	apiKey   string
	endpoint string
	client   *http.Client
}

func NewHTTPTransport(config Config) *HTTPTransport {
	config.applyDefaults()
	timeout := config.HTTPTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &HTTPTransport{
		apiKey:   config.APIKey,
		endpoint: strings.TrimRight(config.Endpoint, "/"),
		client:   &http.Client{Timeout: timeout},
	}
}

func (t *HTTPTransport) SendBatch(ctx context.Context, entries []LogEntry) SendResult {
	return t.postJSON(ctx, "/v1/logs", map[string]any{
		"projectApiKey": t.apiKey,
		"logs":          entries,
	})
}

func (t *HTTPTransport) SendSingle(ctx context.Context, entry LogEntry) SendResult {
	return t.postJSON(ctx, "/v1/logs/single", map[string]any{
		"projectApiKey": t.apiKey,
		"log":           entry,
	})
}

func (t *HTTPTransport) postJSON(ctx context.Context, path string, body any) SendResult {
	payload, err := json.Marshal(body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "auralog: failed to encode ingest payload: %v\n", err)
		return SendPermanentFailure
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "auralog: failed to build ingest request: %v\n", err)
		return SendPermanentFailure
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "auralog: HTTP delivery failure: %v\n", err)
		return SendRetryableFailure
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return SendSuccess
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		fmt.Fprintf(os.Stderr, "auralog: non-retryable HTTP %d from ingest\n", resp.StatusCode)
		return SendPermanentFailure
	}
	fmt.Fprintf(os.Stderr, "auralog: retryable HTTP %d from ingest\n", resp.StatusCode)
	return SendRetryableFailure
}
