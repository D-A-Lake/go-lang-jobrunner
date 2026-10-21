"""A fake training job for trying out jobrunner.

    python3 examples/train.py --epochs 5
    python3 examples/train.py --crash-on-first   # crashes on attempt 1, succeeds on the retry
    python3 examples/train.py --ignore-sigterm   # forces jobrunner to escalate to SIGKILL
"""

import argparse
import os
import random
import signal
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("--epochs", type=int, default=5)
parser.add_argument("--step-seconds", type=float, default=0.5)
parser.add_argument("--crash-on-first", action="store_true")
parser.add_argument("--ignore-sigterm", action="store_true")
args = parser.parse_args()

attempt = int(os.environ.get("JOBRUNNER_ATTEMPT", "1"))


def on_sigterm(signum, frame):
    # This is what a well-behaved job does when jobrunner asks it to stop.
    print("got SIGTERM: saving checkpoint and exiting", flush=True)
    time.sleep(0.5)
    sys.exit(143)


if args.ignore_sigterm:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
else:
    signal.signal(signal.SIGTERM, on_sigterm)

print(f"training started (attempt {attempt})")
loss = 2.5
for epoch in range(1, args.epochs + 1):
    for step in range(1, 5):
        time.sleep(args.step_seconds)
        loss *= random.uniform(0.85, 0.97)
        print(f"epoch {epoch}/{args.epochs} step {step}/4 loss={loss:.4f}")

    if args.crash_on_first and attempt == 1 and epoch == 2:
        print("CUDA error: out of memory (simulated)", file=sys.stderr)
        sys.exit(1)

print("training finished")
