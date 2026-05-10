package server

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"
)

// SSE tuning. Snapshots are taken at sseTickInterval; heartbeats keep
// proxies from closing idle connections; sseRetryMs tells EventSource
// how long to wait before reconnecting after a drop.
const (
	sseTickInterval      = 500 * time.Millisecond
	sseHeartbeatInterval = 15 * time.Second
	sseRetryMs           = 5000
)

// streamSSEFromHandler turns an existing HTML-rendering handler into an
// SSE stream. The handler is invoked at every tick and its body is
// emitted as one SSE `data:` event when the body changes; identical
// consecutive bodies are suppressed so an idle dashboard makes ~zero
// outbound traffic. Heartbeat comments keep the connection warm.
//
// If completionTrigger is non-empty, an `event: complete` is emitted
// when the wrapped handler returns it via the `HX-Trigger` header
// (e.g., "scanCompleted"); the stream then closes cleanly.
func (s *Server) streamSSEFromHandler(w http.ResponseWriter, r *http.Request, handler http.HandlerFunc, completionTrigger string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported by underlying writer", http.StatusInternalServerError)
		return
	}

	// SSE response headers. X-Accel-Buffering disables nginx-style
	// proxy buffering that would otherwise queue events.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// Tell EventSource clients to wait this many ms before reconnecting.
	if _, err := fmt.Fprintf(w, "retry: %d\n\n", sseRetryMs); err != nil {
		return
	}
	flusher.Flush()

	tick := time.NewTicker(sseTickInterval)
	defer tick.Stop()
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	var lastBody string

	// Emit captures the wrapped handler's output and ships it as one
	// SSE event. Returns true when the handler signalled completion.
	emit := func() bool {
		rec := httptest.NewRecorder()
		handler(rec, r)
		body := rec.Body.String()
		completed := completionTrigger != "" &&
			rec.Header().Get("HX-Trigger") == completionTrigger

		if body != lastBody {
			lastBody = body
			// SSE: each "data:" line is one logical line of payload.
			// HTML can contain newlines, so split and prefix each line.
			for _, line := range strings.Split(body, "\n") {
				if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
					return false
				}
			}
			if _, err := fmt.Fprint(w, "\n"); err != nil {
				return false
			}
			flusher.Flush()
		}
		return completed
	}

	if emit() {
		_, _ = fmt.Fprint(w, "event: complete\ndata: \n\n")
		flusher.Flush()
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return

		case <-tick.C:
			if emit() {
				_, _ = fmt.Fprint(w, "event: complete\ndata: \n\n")
				flusher.Flush()
				return
			}

		case <-heartbeat.C:
			// Comment-style heartbeat — clients ignore it but proxies
			// see traffic and don't close the connection.
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				log.Printf("SSE: heartbeat write failed: %v", err)
				return
			}
			flusher.Flush()
		}
	}
}

// HandleScanEvents streams scan-progress HTML over SSE, replacing the
// dashboard's 2s polling of /api/scan/progress-html. The polling
// endpoint is intentionally retained as a fallback for clients that
// can't speak SSE (or for proxies that buffer it).
func (s *Server) HandleScanEvents(w http.ResponseWriter, r *http.Request) {
	s.streamSSEFromHandler(w, r, s.HandleScanProgressHTML, "scanCompleted")
}

// HandleHashEvents — same idea for hash-scan progress. The hash handler
// doesn't currently set a completion HX-Trigger, so we leave the
// completion field empty; the stream continues to tick (and dedupe
// identical bodies) until the client disconnects.
func (s *Server) HandleHashEvents(w http.ResponseWriter, r *http.Request) {
	s.streamSSEFromHandler(w, r, s.HandleHashProgressHTML, "")
}

// HandleDiskScanEvents — same for the per-disk location scan progress.
func (s *Server) HandleDiskScanEvents(w http.ResponseWriter, r *http.Request) {
	s.streamSSEFromHandler(w, r, s.HandleDiskScanProgressHTML, "diskScanCompleted")
}
