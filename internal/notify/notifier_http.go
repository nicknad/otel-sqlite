package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"time"
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

// HTTPNotifier sends notifications via HTTP POST (webhook).
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

// payload is the JSON structure sent to the webhook.
type payload struct {
	Severity     string            `json:"severity"`
	SeverityText string            `json:"severity_text,omitempty"`
	Body         string            `json:"body"`
	ResourceID   string            `json:"resource_id,omitempty"`
	Timestamp    int64             `json:"timestamp"`
	TraceID      string            `json:"trace_id,omitempty"`
	SpanID       string            `json:"span_id,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	ScopeName    string            `json:"scope_name,omitempty"`
	ScopeVersion string            `json:"scope_version,omitempty"`
}

// Send POSTs the event as JSON to the configured URL.
func (n *HTTPNotifier) Send(ctx context.Context, event *Event) error {
	p := payload{
		Severity:     event.Severity.String(),
		SeverityText: event.SeverityText,
		Body:         event.Body,
		ResourceID:   event.ResourceID,
		Timestamp:    event.Timestamp,
		ScopeName:    event.ScopeName,
		ScopeVersion: event.ScopeVersion,
	}

	// Format trace/span IDs as hex strings.
	if event.TraceID != [16]byte{} {
		p.TraceID = fmt.Sprintf("%032x", event.TraceID)
	}
	if event.SpanID != [8]byte{} {
		p.SpanID = fmt.Sprintf("%016x", event.SpanID)
	}

	// Flatten attributes to a simple map.
	if len(event.Attributes) > 0 {
		p.Attributes = make(map[string]string, len(event.Attributes))
		for _, attr := range event.Attributes {
			p.Attributes[attr.Key] = attrValueString(&attr)
		}
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

	// Discard body to enable connection reuse.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("http %d from %q", resp.StatusCode, n.url)
	}

	return nil
}

// Close closes idle connections.
func (n *HTTPNotifier) Close() error {
	n.client.CloseIdleConnections()
	return nil
}
