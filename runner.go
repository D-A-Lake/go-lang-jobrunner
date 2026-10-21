package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Config is everything the runner needs to know about one job.
type Config struct {
	Command    []string      // e.g. ["python", "train.py"]
	MaxRetries int           // how many times to re-run after a crash
	Backoff    time.Duration // wait before the first retry; doubles each time
	Heartbeat  time.Duration // how often to report that the job is alive
	StopGrace  time.Duration // how long to wait after SIGTERM before SIGKILL
}

// ErrStopped means the job was cancelled (Ctrl+C or POST /stop), not that it failed.
var ErrStopped = errors.New("job stopped")

// errCouldNotStart means the command never ran (e.g. typo in the program
// name). Retrying would just fail the same way, so we don't.
var errCouldNotStart = errors.New("could not start job")

// Status is a point-in-time snapshot of the job, served at GET /status.
type Status struct {
	State           string    `json:"state"` // starting, running, backoff, succeeded, failed, stopped
	Attempt         int       `json:"attempt"`
	PID             int       `json:"pid,omitempty"`
	LastHeartbeat   time.Time `json:"last_heartbeat,omitzero"`
	LastOutput      time.Time `json:"last_output,omitzero"`
	MeteredSeconds  float64   `json:"metered_seconds"`
	BillableSeconds int64     `json:"billable_seconds"`
}

// Runner runs a Config's command, retrying on crashes, until it succeeds,
// runs out of retries, or the context is cancelled.
type Runner struct {
	cfg   Config
	logs  *LogHub
	meter *Meter

	mu     sync.Mutex // guards status
	status Status
}

func NewRunner(cfg Config, logs *LogHub, meter *Meter) *Runner {
	return &Runner{cfg: cfg, logs: logs, meter: meter, status: Status{State: "starting"}}
}

// Status returns a copy of the current status with live metering figures.
func (r *Runner) Status() Status {
	r.mu.Lock()
	s := r.status
	r.mu.Unlock()
	total := r.meter.Total()
	s.MeteredSeconds = total.Seconds()
	s.BillableSeconds = BillableSeconds(total)
	return s
}

func (r *Runner) update(fn func(s *Status)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(&r.status)
}

func (r *Runner) setState(state string) {
	r.update(func(s *Status) { s.State = state; s.PID = 0 })
}

// logf publishes a message from the agent itself, marked so it stands out
// from the job's own output.
func (r *Runner) logf(format string, args ...any) {
	r.logs.Publish("[agent] " + fmt.Sprintf(format, args...))
}

// Run is the retry loop. It returns nil on success, ErrStopped if cancelled,
// or the last error if every attempt failed.
func (r *Runner) Run(ctx context.Context) error {
	for attempt := 1; ; attempt++ {
		err := r.runOnce(ctx, attempt)

		switch {
		case ctx.Err() != nil:
			// Cancelled. Check this first: a job we killed exits non-zero,
			// and we must not mistake that for a crash and retry it.
			r.setState("stopped")
			return ErrStopped
		case err == nil:
			r.setState("succeeded")
			return nil
		case errors.Is(err, errCouldNotStart):
			r.setState("failed")
			return err
		case attempt > r.cfg.MaxRetries:
			r.setState("failed")
			return fmt.Errorf("gave up after %d attempts: %w", attempt, err)
		}

		// Exponential backoff: 1s, 2s, 4s, ... so a job that crashes
		// instantly doesn't spin in a tight loop.
		wait := r.cfg.Backoff * time.Duration(1<<(attempt-1))
		r.logf("attempt %d crashed (%v); retrying in %s (%d retries left)",
			attempt, err, wait, r.cfg.MaxRetries-attempt+1)
		r.setState("backoff")

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			r.setState("stopped")
			return ErrStopped
		}
	}
}

// runOnce starts the command, streams its output, sends heartbeats, and
// waits for it to exit (or kills it if ctx is cancelled).
func (r *Runner) runOnce(ctx context.Context, attempt int) error {
	cmd := exec.Command(r.cfg.Command[0], r.cfg.Command[1:]...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("JOBRUNNER_ATTEMPT=%d", attempt), // lets the job know it's a retry (e.g. to resume from a checkpoint)
		"PYTHONUNBUFFERED=1",                         // Python buffers output into a pipe; without this, "live" logs arrive in big lumps
	)

	// Put the job in its own process group. Two reasons:
	//  1. When you press Ctrl+C, the terminal sends SIGINT to the whole
	//     foreground group. In its own group, the job doesn't get that signal
	//     directly, so we stay in charge of how and when it is shut down.
	//  2. We can signal the group (negative PID), which reaches any children
	//     the job spawned too, e.g. PyTorch data-loader workers.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// One pipe for both stdout and stderr, so lines stay in the order the
	// job wrote them.
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("%w: %v", errCouldNotStart, err)
	}
	cmd.Stdout = pw
	cmd.Stderr = pw

	r.logf("attempt %d: starting %q", attempt, r.cfg.Command)
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		r.logf("could not start: %v", err)
		return fmt.Errorf("%w: %v", errCouldNotStart, err)
	}
	// The child has its own copy of the write end now. Close ours, or the
	// reader below would never see end-of-file.
	pw.Close()

	start := time.Now()
	r.meter.Begin(start)
	pid := cmd.Process.Pid
	r.update(func(s *Status) {
		s.State, s.Attempt, s.PID, s.LastOutput = "running", attempt, pid, start
	})

	// Read the job's output line by line and publish each line.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer pr.Close()
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 64*1024), 1024*1024) // allow long lines (up to 1 MB)
		for sc.Scan() {
			r.logs.Publish(sc.Text())
			now := time.Now()
			r.update(func(s *Status) { s.LastOutput = now })
		}
	}()

	// cmd.Wait blocks, so run it in a goroutine and get the result on a
	// channel. That lets the select below also watch for heartbeats and
	// cancellation while we wait.
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	heartbeat := time.NewTicker(r.cfg.Heartbeat)
	defer heartbeat.Stop()

	var waitErr error
wait:
	for {
		select {
		case waitErr = <-waitDone:
			break wait
		case <-heartbeat.C:
			r.sendHeartbeat(attempt, pid, start)
		case <-ctx.Done():
			waitErr = r.terminate(pid, waitDone)
			break wait
		}
	}

	end := time.Now()
	exitCode := exitCodeOf(waitErr)
	r.meter.End(attempt, end, exitCode)

	// Let the reader drain the last lines. A background grandchild could keep
	// the pipe open forever, so don't wait on it indefinitely.
	select {
	case <-readerDone:
	case <-time.After(2 * time.Second):
	}

	r.logf("attempt %d exited with code %d after %s", attempt, exitCode, end.Sub(start).Round(time.Millisecond))
	return waitErr
}

// sendHeartbeat reports that the job is still alive. A real agent would send
// this to a control plane so it can spot dead machines; here it goes to the
// log and to GET /status. It also says how long the job has been silent,
// which is a cheap way to notice a job that is alive but hung.
func (r *Runner) sendHeartbeat(attempt, pid int, start time.Time) {
	// Signal 0 delivers nothing; it only checks the process exists.
	alive := syscall.Kill(pid, 0) == nil
	now := time.Now()

	var quiet time.Duration
	r.update(func(s *Status) {
		s.LastHeartbeat = now
		quiet = now.Sub(s.LastOutput)
	})
	r.logf("heartbeat: attempt %d, pid %d, alive=%t, up %s, last output %s ago",
		attempt, pid, alive, now.Sub(start).Round(time.Second), quiet.Round(time.Second))
}

// terminate shuts the job down politely, then firmly.
// SIGTERM gives it a chance to save a checkpoint and clean up; if it's still
// running after StopGrace, SIGKILL ends it (SIGKILL can't be caught or ignored).
func (r *Runner) terminate(pid int, waitDone <-chan error) error {
	r.logf("stop requested: sending SIGTERM to process group %d (grace period %s)", pid, r.cfg.StopGrace)
	_ = syscall.Kill(-pid, syscall.SIGTERM) // negative pid = the whole group

	select {
	case err := <-waitDone:
		return err
	case <-time.After(r.cfg.StopGrace):
		r.logf("job still running after %s: sending SIGKILL", r.cfg.StopGrace)
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		return <-waitDone
	}
}

// exitCodeOf turns cmd.Wait's error into a process exit code.
func exitCodeOf(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode() // -1 if killed by a signal
	}
	return -1
}
