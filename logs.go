package main

import (
	"fmt"
	"io"
	"sync"
)

// LogHub fans every line of job output out to many listeners at once:
// the terminal (written synchronously) and any number of HTTP clients
// (each gets its own channel). It also keeps the most recent lines so a
// client that connects halfway through a job can catch up first.
type LogHub struct {
	mu         sync.Mutex
	out        io.Writer // usually os.Stdout
	history    []string
	maxHistory int
	subs       map[chan string]struct{}
	closed     bool
}

func NewLogHub(out io.Writer, maxHistory int) *LogHub {
	return &LogHub{
		out:        out,
		maxHistory: maxHistory,
		subs:       make(map[chan string]struct{}),
	}
}

// Publish sends one line to the terminal and to every subscriber.
func (h *LogHub) Publish(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}

	fmt.Fprintln(h.out, line)

	h.history = append(h.history, line)
	if len(h.history) > h.maxHistory {
		h.history = h.history[len(h.history)-h.maxHistory:]
	}

	for ch := range h.subs {
		select {
		case ch <- line:
		default:
			// This subscriber isn't keeping up (slow network, say). Drop the
			// line for them rather than block: a stuck HTTP client must never
			// be able to stall the job's output.
		}
	}
}

// Subscribe returns the lines seen so far plus a channel that receives every
// future line. The channel is closed when the hub closes. Call unsubscribe
// when you're done listening.
func (h *LogHub) Subscribe() (backlog []string, lines <-chan string, unsubscribe func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	backlog = append([]string(nil), h.history...) // copy, so the caller can't race with Publish
	ch := make(chan string, 256)
	if h.closed {
		close(ch)
		return backlog, ch, func() {}
	}
	h.subs[ch] = struct{}{}

	unsubscribe = func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		// Close may already have closed and removed it; closing twice would panic.
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
	return backlog, ch, unsubscribe
}

// Close ends every subscription. Streaming HTTP clients see end-of-stream.
func (h *LogHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for ch := range h.subs {
		delete(h.subs, ch)
		close(ch)
	}
}
