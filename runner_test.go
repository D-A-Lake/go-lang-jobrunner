package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func newTestRunner(command ...string) (*Runner, *Meter) {
	meter := &Meter{}
	cfg := Config{
		Command:    command,
		MaxRetries: 2,
		Backoff:    10 * time.Millisecond,
		Heartbeat:  time.Hour, // keep heartbeats out of the way
		StopGrace:  time.Second,
	}
	return NewRunner(cfg, NewLogHub(io.Discard, 100), meter), meter
}

func TestSucceedsFirstTime(t *testing.T) {
	r, meter := newTestRunner("sh", "-c", "echo hello")
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := len(meter.Attempts()); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
}

func TestRetriesAfterCrash(t *testing.T) {
	// Crash on attempt 1, succeed on attempt 2.
	r, meter := newTestRunner("sh", "-c", `[ "$JOBRUNNER_ATTEMPT" -ge 2 ] || exit 3`)
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	attempts := meter.Attempts()
	if len(attempts) != 2 || attempts[0].ExitCode != 3 || attempts[1].ExitCode != 0 {
		t.Fatalf("attempts = %+v, want exit 3 then exit 0", attempts)
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	r, meter := newTestRunner("sh", "-c", "exit 1")
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded, want failure")
	}
	if n := len(meter.Attempts()); n != 3 { // 1 try + 2 retries
		t.Fatalf("attempts = %d, want 3", n)
	}
}

func TestDoesNotRetryMissingCommand(t *testing.T) {
	r, meter := newTestRunner("no-such-command-xyz")
	err := r.Run(context.Background())
	if !errors.Is(err, errCouldNotStart) {
		t.Fatalf("err = %v, want errCouldNotStart", err)
	}
	if n := len(meter.Attempts()); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
}

func TestStopKillsJobAndMetersTime(t *testing.T) {
	r, meter := newTestRunner("sleep", "30")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)

	start := time.Now()
	err := r.Run(ctx)
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stop took %s; the job wasn't killed promptly", elapsed)
	}
	if got := meter.Total(); got < 250*time.Millisecond || got > time.Second {
		t.Fatalf("metered %s, want about 300ms", got)
	}
	if n := len(meter.Attempts()); n != 1 {
		t.Fatalf("attempts = %d; a stopped job must not be retried", n)
	}
}

func TestStopEscalatesToSIGKILL(t *testing.T) {
	// The job ignores SIGTERM, so only SIGKILL after the grace period ends it.
	r, _ := newTestRunner("sh", "-c", "trap '' TERM; sleep 30")
	r.cfg.StopGrace = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)

	start := time.Now()
	if err := r.Run(ctx); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stop took %s; SIGKILL escalation didn't happen", elapsed)
	}
}

func TestBillableSecondsRoundsUp(t *testing.T) {
	cases := map[time.Duration]int64{
		0:                       0,
		time.Millisecond:        1,
		time.Second:             1,
		1500 * time.Millisecond: 2,
	}
	for d, want := range cases {
		if got := BillableSeconds(d); got != want {
			t.Errorf("BillableSeconds(%s) = %d, want %d", d, got, want)
		}
	}
}
