package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func TestHTTPNotifier_Send(t *testing.T) {
	var received payload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	notifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:     server.URL + "/webhook",
		Timeout: 5 * time.Second,
	})

	event := &Event{
		Severity:     model.SeverityError,
		SeverityText: "ERROR",
		Body:         "test error message",
		ResourceID:   "res-123",
		Timestamp:    time.Now().UnixNano(),
		TraceID:      [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:       [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Attributes: []model.Attribute{
			{Key: "service.name", Str: "test-svc", Kind: model.ValueString},
		},
	}

	err := notifier.Send(context.Background(), event)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if received.Body != "test error message" {
		t.Errorf("body = %q, want %q", received.Body, "test error message")
	}
	if received.Severity != "ERROR" {
		t.Errorf("severity = %q, want %q", received.Severity, "ERROR")
	}
	if received.ResourceID != "res-123" {
		t.Errorf("resource_id = %q, want %q", received.ResourceID, "res-123")
	}
	if received.TraceID != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace_id = %q", received.TraceID)
	}
	if received.SpanID != "0102030405060708" {
		t.Errorf("span_id = %q", received.SpanID)
	}
	if received.Attributes["service.name"] != "test-svc" {
		t.Errorf("attributes[service.name] = %q, want %q", received.Attributes["service.name"], "test-svc")
	}
}

func TestHTTPNotifier_AuthHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	notifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:        server.URL,
		AuthHeader: "Bearer secret-token",
	})

	event := &Event{Severity: model.SeverityError, Body: "test"}
	err := notifier.Send(context.Background(), event)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func TestHTTPNotifier_ErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	notifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL: server.URL,
	})

	event := &Event{Severity: model.SeverityError, Body: "test"}
	err := notifier.Send(context.Background(), event)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestHTTPNotifier_Name(t *testing.T) {
	n := NewHTTPNotifier(&HTTPNotifierConfig{URL: "http://example.com"})
	if n.Name() != "http" {
		t.Errorf("Name = %q, want %q", n.Name(), "http")
	}
}
