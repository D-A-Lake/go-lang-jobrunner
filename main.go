// jobrunner runs a command as a child process and looks after it: it streams
// the output live (terminal + HTTP), restarts it if it crashes, stops it
// cleanly on Ctrl+C or POST /stop, and meters how long it ran.
//
//	jobrunner [flags] -- python train.py --epochs 10
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP address for /logs, /status and /stop")
	retries := flag.Int("retries", 3, "times to restart the job if it crashes")
	backoff := flag.Duration("backoff", time.Second, "wait before the first retry (doubles each retry)")
	heartbeat := flag.Duration("heartbeat", 5*time.Second, "how often to report the job is alive")
	grace := flag.Duration("grace", 10*time.Second, "time between SIGTERM and SIGKILL when stopping")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: jobrunner [flags] -- command [args...]\n\nflags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	command := flag.Args()
	if len(command) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	// One context means "keep going". It's cancelled by Ctrl+C (SIGINT),
	// SIGTERM (e.g. `kill` or a container shutting down), or POST /stop.
	// Everything that should stop when the job stops watches this context.
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, stopJob := context.WithCancel(ctx)
	defer stopJob()

	logs := NewLogHub(os.Stdout, 1000)
	meter := &Meter{}
	runner := NewRunner(Config{
		Command:    command,
		MaxRetries: *retries,
		Backoff:    *backoff,
		Heartbeat:  *heartbeat,
		StopGrace:  *grace,
	}, logs, meter)

	// Listen before starting the job, so a port clash fails fast instead of
	// after the job has already started.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jobrunner: %v\n", err)
		os.Exit(1)
	}
	srv := &http.Server{Handler: NewServer(runner, logs, stopJob)}
	go srv.Serve(ln)
	logs.Publish(fmt.Sprintf("[agent] live logs: curl -N http://%s/logs   status: curl http://%s/status", ln.Addr(), ln.Addr()))

	runErr := runner.Run(ctx)

	// Job's done: end any open log streams, then shut the server down.
	logs.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)

	printSummary(command, meter, runErr)

	switch {
	case runErr == nil:
		os.Exit(0)
	case errors.Is(runErr, ErrStopped):
		os.Exit(130) // shell convention for "ended by Ctrl+C"
	default:
		os.Exit(1)
	}
}

func printSummary(command []string, meter *Meter, runErr error) {
	attempts := meter.Attempts()
	total := meter.Total()

	result := "succeeded"
	if errors.Is(runErr, ErrStopped) {
		result = "stopped"
	} else if runErr != nil {
		result = "failed: " + runErr.Error()
	}

	fmt.Println()
	fmt.Println("──────── job summary ────────")
	fmt.Printf("command:          %s\n", strings.Join(command, " "))
	fmt.Printf("result:           %s\n", result)
	for _, a := range attempts {
		fmt.Printf("  attempt %d:      %-10s exit %d\n", a.Number, a.Duration.Round(time.Millisecond), a.ExitCode)
	}
	fmt.Printf("metered time:     %s\n", total.Round(time.Millisecond))
	fmt.Printf("billable seconds: %d\n", BillableSeconds(total))
}
