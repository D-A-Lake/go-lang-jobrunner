package main

import (
	"math"
	"sync"
	"time"
)

// Attempt is one run of the job, from process start to process exit.
type Attempt struct {
	Number   int
	Duration time.Duration
	ExitCode int // -1 means it was killed by a signal
}

// Meter records how long the job actually ran. Only time the child process
// was alive counts: waiting between retries is not billed.
//
// Durations come from time.Now() and Time.Sub, which use Go's monotonic clock,
// so the figures are correct even if the system clock jumps (NTP sync, DST,
// someone changing the time) while the job runs.
type Meter struct {
	mu           sync.Mutex
	attempts     []Attempt
	runningSince time.Time // zero when no attempt is running
}

// Begin marks the moment an attempt's process started.
func (m *Meter) Begin(start time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runningSince = start
}

// End marks the moment the attempt's process exited.
func (m *Meter) End(number int, end time.Time, exitCode int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts = append(m.attempts, Attempt{
		Number:   number,
		Duration: end.Sub(m.runningSince),
		ExitCode: exitCode,
	})
	m.runningSince = time.Time{}
}

// Total is the metered run time so far, including an attempt still running.
func (m *Meter) Total() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total time.Duration
	for _, a := range m.attempts {
		total += a.Duration
	}
	if !m.runningSince.IsZero() {
		total += time.Since(m.runningSince)
	}
	return total
}

// Attempts returns a copy of the finished attempts.
func (m *Meter) Attempts() []Attempt {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Attempt(nil), m.attempts...)
}

// BillableSeconds converts metered time into whole seconds, rounding any
// partial second up. That rounding is a pricing policy, not a law of nature;
// per-second billing with round-up is what most cloud providers do.
func BillableSeconds(d time.Duration) int64 {
	return int64(math.Ceil(d.Seconds()))
}
