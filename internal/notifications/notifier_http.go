package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"
)

// HTTPNotifierConfig configures the HTTP webhook notifier.
type HTTPNotifierConfig struct {
	// URL is the webhook endpoint to POST to.
	URL string

	// Timeout is the HTTP client timeout. Defaults to 10s.
	Timeout time.Duration

	// AuthHeader is an optional authorization header value (e.g., "Bearer token").
	// If set, it is sent as the "Authorization" header.
	AuthHeader string

	// CustomHeaders are additional HTTP headers to include.
	CustomHeaders map[string]string
}

// HTTPNotifier sends alert notifications via HTTP POST (webhook).
type HTTPNotifier struct {
	url     string
	client  *http.Client
	headers map[string]string
}

// NewHTTPNotifier creates a new HTTPNotifier.
func NewHTTPNotifier(cfg *HTTPNotifierConfig) *HTTPNotifier {
	if cfg == nil {
		cfg = &HTTPNotifierConfig{}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	headers := make(map[string]string, len(cfg.CustomHeaders)+2)
	headers["Content-Type"] = "application/json"
	if cfg.AuthHeader != "" {
		headers["Authorization"] = cfg.AuthHeader
	}
	maps.Copy(headers, cfg.CustomHeaders)

	return &HTTPNotifier{
		url:     cfg.URL,
		headers: headers,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Name returns "http".
func (n *HTTPNotifier) Name() string {
	return "http"
}

// alertPayload is the JSON structure sent to the webhook.
type alertPayload struct {
	ID          string `json:"id"`
	RuleID      string `json:"rule_id"`
	ResourceID  string `json:"resource_id"`
	Status      string `json:"status"`
	Severity    string `json:"severity"`
	OpenedAt    int64  `json:"opened_at"`
	UpdatedAt   int64  `json:"updated_at"`
	LastMatched int64  `json:"last_matched"`
	Count       int    `json:"count"`
}

// Send POSTs the alert as JSON to the configured URL.
func (n *HTTPNotifier) Send(ctx context.Context, alert *alerts.Alert) error {
	p := alertPayload{
		ID:          alert.ID,
		RuleID:      alert.RuleID,
		ResourceID:  alert.ResourceID,
		Status:      alert.Status.String(),
		Severity:    alert.Severity.String(),
		OpenedAt:    alert.OpenedAt,
		UpdatedAt:   alert.UpdatedAt,
		LastMatched: alert.LastMatched,
		Count:       alert.Count,
	}

	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	for k, v := range n.headers {
		req.Header.Set(k, v)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("http post %q: %w", n.url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := readBodyPrefix(resp.Body, 512)
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 500 {
		err := fmt.Errorf("http %d from %q: %s", resp.StatusCode, n.url, respBody)
		return err // retryable (5xx)
	}
	if resp.StatusCode >= 400 {
		err := fmt.Errorf("http %d from %q: %s", resp.StatusCode, n.url, respBody)
		return NewNotRetryableError(err) // non-retryable (4xx)
	}

	return nil
}

// readBodyPrefix reads up to maxBytes from r and returns them as a string.
func readBodyPrefix(r io.Reader, maxBytes int) (string, error) {
	buf := make([]byte, maxBytes)
	n, err := io.ReadFull(r, buf)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return string(buf[:n]), nil
	}
	return string(buf[:n]), err
}

// Close closes idle connections.
func (n *HTTPNotifier) Close() error {
	n.client.CloseIdleConnections()
	return nil
}
