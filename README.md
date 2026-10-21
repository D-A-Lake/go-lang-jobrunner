# go-lang-jobrunner

A small Go agent that runs a job (like `python train.py`) as a child process and looks after it:

- **Live logs**: output streams to your terminal and over HTTP (`GET /logs`).
- **Clean cancellation**: on Ctrl+C, SIGTERM or `POST /stop`, it sends SIGTERM to the job, waits for a grace period, then sends SIGKILL.
- **Heartbeat and retry**: it reports on a timer that the job is alive and restarts it with exponential backoff if it crashes.
- **Metering**: it records exactly how long the job ran, across all attempts, and prints a billable-seconds figure at the end.

It uses only the Go standard library, with no dependencies. It runs on macOS and Linux (it relies on Unix process groups).

## Try it

```bash
git clone https://github.com/D-A-Lake/go-lang-jobrunner.git && cd go-lang-jobrunner
go build -o jobrunner .
./jobrunner -- python3 examples/train.py --crash-on-first
```

While it runs, in another terminal:

```bash
curl -N http://127.0.0.1:8080/logs      # live log stream (-N turns off curl's buffering)
curl http://127.0.0.1:8080/status       # JSON: state, attempt, heartbeat, metered time
curl -X POST http://127.0.0.1:8080/stop # stop the job cleanly
```

Example output:

```
[agent] attempt 1: starting ["python3" "examples/train.py" "--crash-on-first"]
training started (attempt 1)
epoch 1/5 step 1/4 loss=2.2810
[agent] heartbeat: attempt 1, pid 5685, alive=true, up 5s, last output 0s ago
CUDA error: out of memory (simulated)
[agent] attempt 1 exited with code 1 after 4.012s
[agent] attempt 1 crashed (exit status 1); retrying in 1s (3 retries left)
[agent] attempt 2: starting ...
training finished

──────── job summary ────────
command:          python3 examples/train.py --crash-on-first
result:           succeeded
  attempt 1:      4.012s     exit 1
  attempt 2:      10.041s    exit 0
metered time:     14.053s
billable seconds: 15
```

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `127.0.0.1:8080` | HTTP address |
| `-retries` | `3` | restarts allowed after a crash |
| `-backoff` | `1s` | wait before the first retry; doubles each time |
| `-heartbeat` | `5s` | heartbeat interval |
| `-grace` | `10s` | time between SIGTERM and SIGKILL |

Other things to try: `--ignore-sigterm` shows the escalation to SIGKILL, and `go test -race ./...` runs the tests.

## How the code fits together

Read the files in this order:

1. **`main.go`** parses the flags, builds the pieces and wires them up. The key idea is **one `context.Context`** that means "keep running". Ctrl+C, SIGTERM and `POST /stop` all cancel it, and everything else watches it.
2. **`runner.go`** is the core.
   - `Run` is the retry loop. It checks for cancellation *before* it checks for failure, because a job we killed exits non-zero and must not be retried. It doesn't retry a command that couldn't start at all, such as a typo.
   - `runOnce` starts one attempt. Things worth noticing:
     - `Setpgid: true` puts the job in its **own process group**. The terminal's Ctrl+C then goes only to the agent, which decides how to stop the job. Signalling `-pid` also reaches any child processes the job spawned, such as data-loader workers.
     - stdout and stderr share **one `os.Pipe`**, so lines keep their order. The parent closes its copy of the write end, or the reader would never see end-of-file.
     - `PYTHONUNBUFFERED=1` is set because Python buffers output written to a pipe, which would make the "live" logs arrive in chunks.
     - `cmd.Wait()` runs in a goroutine and reports back on a channel. A `select` can then wait for **exit, a heartbeat tick or cancellation** at the same time. This is the most important Go pattern in the project.
   - `terminate` sends SIGTERM, then SIGKILL after the grace period.
   - `sendHeartbeat` uses `kill(pid, 0)` to check the process still exists. It also reports how long the job has been silent, which is a cheap way to notice a hang.
3. **`logs.go`** contains `LogHub`, a broadcaster. Each line goes to the terminal and to every subscriber's channel. If a subscriber is too slow, its lines are **dropped, never waited on**, so one slow HTTP client can't stall the job.
4. **`server.go`** holds the three HTTP endpoints. `/logs` sends the recent backlog, then flushes each new line as it arrives (`http.ResponseController.Flush`).
5. **`meter.go`** counts only time the process was alive; backoff waits aren't billed. It uses Go's **monotonic clock** (`time.Now()` plus `Sub`), so changes to the system clock don't distort the figures. Partial seconds round up, as with per-second cloud billing.

## What a production version would add

This is deliberately small. A real agent would also need:

- authentication on the HTTP API. Here it only listens on localhost, and `/stop` has no auth.
- heartbeats sent to a control plane, rather than just logged.
- logs shipped to durable storage, so they survive the agent restarting.
- usage records written somewhere durable, so they survive a crash of the agent itself.
- resource limits (cgroups) and a hang detector that acts on "no output for N minutes".
