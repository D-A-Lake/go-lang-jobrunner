package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// NewServer exposes the job over HTTP:
//
//	GET  /logs    stream the job's output live (backlog first, then new lines)
//	GET  /status  JSON snapshot: state, attempt, heartbeat, metered time
//	POST /stop    ask the agent to stop the job cleanly
func NewServer(runner *Runner, logs *LogHub, stop func()) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /logs", func(w http.ResponseWriter, req *http.Request) {
		// Plain text keeps it usable from curl and a browser alike. nosniff
		// stops browsers buffering the response while they guess its type.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")

		// Flush pushes buffered bytes to the client right away. Without it,
		// Go's server would hold small writes back and the stream wouldn't be live.
		rc := http.NewResponseController(w)

		backlog, lines, unsubscribe := logs.Subscribe()
		defer unsubscribe()

		for _, line := range backlog {
			fmt.Fprintln(w, line)
		}
		rc.Flush()

		for {
			select {
			case line, ok := <-lines:
				if !ok {
					return // job finished; end the stream
				}
				fmt.Fprintln(w, line)
				rc.Flush()
			case <-req.Context().Done():
				return // client disconnected
			}
		}
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(runner.Status())
	})

	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, req *http.Request) {
		logs.Publish("[agent] stop requested over HTTP")
		stop()
		// 202 Accepted: the stop has begun but may take up to the grace period.
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "stopping")
	})

	return mux
}
