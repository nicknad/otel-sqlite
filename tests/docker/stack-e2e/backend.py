"""Backend service for the otel-sqlite stack E2E.

A deliberately tiny stand-in for a "real" application:

* serves the API that nginx reverse-proxies under ``/api/``;
* exports one OTLP log record per handled request to the collector via
  OTLP/HTTP JSON (stdlib only - no SDK, no pip installs at runtime);
* emits a heartbeat log every few seconds so the sink receives traffic
  even when no HTTP requests flow.

Timestamps: both ``timeUnixNano`` and ``observedTimeUnixNano`` are set
explicitly on every record; the verifier asserts the sink never stores a
zero observed timestamp from this stack.
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

COLLECTOR = os.environ.get("OTEL_ENDPOINT", "http://otelcol:4318").rstrip("/")
PORT = int(os.environ.get("BACKEND_PORT", "8080"))
HEARTBEAT_SECS = float(os.environ.get("BACKEND_HEARTBEAT_SECS", "5"))
SERVICE_NAME = "stack-backend"
SCOPE_NAME = "stack-backend"

_seq_lock = threading.Lock()
_seq = 0


def next_seq() -> int:
    global _seq
    with _seq_lock:
        _seq += 1
        return _seq


def string_kv(key: str, value: str) -> dict:
    return {"key": key, "value": {"stringValue": value}}


def int_kv(key: str, value: int) -> dict:
    return {"key": key, "value": {"intValue": value}}


def export_log(body: str, attributes: list[dict], severity_text: str = "INFO") -> bool:
    """Best-effort OTLP/HTTP JSON export of one INFO log record."""
    now_ns = time.time_ns()
    payload = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        string_kv("service.name", SERVICE_NAME),
                        string_kv("service.namespace", "otel-sqlite-stack-e2e"),
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {"name": SCOPE_NAME},
                        "logRecords": [
                            {
                                "timeUnixNano": str(now_ns),
                                "observedTimeUnixNano": str(now_ns),
                                "severityNumber": 9,
                                "severityText": severity_text,
                                "body": {"stringValue": body},
                                "attributes": attributes,
                            }
                        ],
                    }
                ],
            }
        ]
    }
    request = urllib.request.Request(
        f"{COLLECTOR}/v1/logs",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            response.read()
        return True
    except (urllib.error.URLError, OSError) as error:
        # Never let telemetry failure break request handling.
        print(f"[backend] otlp export failed: {error}", flush=True)
        return False


def wait_for_collector(deadline_secs: int = 60) -> None:
    """Block until the collector accepts an export (or give up; the
    heartbeat thread keeps retrying either way)."""
    print(f"[backend] waiting for collector at {COLLECTOR}", flush=True)
    for attempt in range(1, deadline_secs + 1):
        if export_log(
            "backend startup probe", [string_kv("event", "startup")]
        ):
            print(f"[backend] collector reachable after {attempt}s", flush=True)
            return
        time.sleep(1)
    print("[backend] collector not reachable; continuing anyway", flush=True)


def heartbeat_loop() -> None:
    while True:
        seq = next_seq()
        export_log(
            f"backend heartbeat #{seq}",
            [string_kv("event", "heartbeat"), int_kv("backend.seq", seq)],
        )
        time.sleep(HEARTBEAT_SECS)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        seq = next_seq()
        if self.path.startswith("/api/health"):
            payload = {"status": "ok"}
        elif self.path.startswith("/api/"):
            payload = {"status": "ok", "path": self.path, "seq": seq}
        else:
            self._respond(404, {"error": "not found"})
            return

        export_log(
            f"handled {self.path}",
            [
                string_kv("http.request.method", "GET"),
                string_kv("url.path", self.path),
                int_kv("http.response.status_code", 200),
                int_kv("backend.seq", seq),
                string_kv("event", "request.handled"),
            ],
        )
        self._respond(200, payload)

    def _respond(self, status: int, payload: dict) -> None:
        # Compact separators: deterministic bodies, easy to assert exactly.
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        # Access lines go to stdout; nginx already produces the access log.
        print(f"[backend] {self.address_string()} {format % args}", flush=True)


def main() -> None:
    wait_for_collector()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[backend] listening on :{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
