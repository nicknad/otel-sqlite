// Command webhook-receiver is a small HTTP server that accepts JSON
// notification payloads from the notification worker. It records every
// received payload and exposes endpoints to inspect them — useful as a
// test double for webhook destinations during development and E2E testing.
//
// Usage:
//
//	webhook-receiver [-addr :8080]
//
// Endpoints:
//
//	POST /webhook   – receive a notification (JSON body)
//	GET  /received  – list all received notification bodies (JSON array)
//	GET  /health    – health check (returns 200 OK)
//	POST /reset     – clear all received notifications
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	srv := &server{
		received: make([]map[string]any, 0),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", srv.handleWebhook)
	mux.HandleFunc("GET /received", srv.handleList)
	mux.HandleFunc("GET /health", srv.handleHealth)
	mux.HandleFunc("POST /reset", srv.handleReset)

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	}()

	log.Printf("webhook-receiver listening on %s", *addr)
	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	log.Println("stopped")
}

type server struct {
	mu       sync.Mutex
	received []map[string]any
}

func (s *server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.received = append(s.received, payload)
	count := len(s.received)
	s.mu.Unlock()

	log.Printf("received notification #%d: severity=%v body=%v",
		count, payload["severity"], payload["body"])

	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","count":%d}`, count)
}

func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.received)
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (s *server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.received = s.received[:0]
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"reset"}`))
}
