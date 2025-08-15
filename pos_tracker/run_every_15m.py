import os, sys, time, subprocess, random, datetime
from pathlib import Path

# === Config ===
SCRIPT = "monitor_parallel_tabs.py"   # the tracker you already have
INTERVAL_MIN = 15                     # run every 15 minutes
JITTER_SEC = (5, 20)                  # add 5-20 sec random delay
LOCKFILE = Path("run_every_15m.lock")
LOGDIR = Path("logs")
PYTHON = sys.executable               # use current venv's python

LOGDIR.mkdir(exist_ok=True)

def acquire_lock():
    # naive cross-platform lock via O_EXCL
    try:
        fd = os.open(LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        # If lock is stale (older than 2 hours), remove it
        try:
            mtime = LOCKFILE.stat().st_mtime
            if (time.time() - mtime) > 2 * 3600:
                LOCKFILE.unlink(missing_ok=True)
                fd = os.open(LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return True
        except FileNotFoundError:
            return acquire_lock()
        return False

def release_lock():
    try:
        LOCKFILE.unlink(missing_ok=True)
    except Exception:
        pass

def log_path():
    ts = datetime.datetime.now().strftime("%Y%m%d")
    return LOGDIR / f"tracker_{ts}.log"

def log_write(msg: str):
    p = log_path()
    with open(p, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")
    print(msg, flush=True)

def sleep_until_next_slot():
    now = datetime.datetime.now()
    # nearest next quarter: 0,15,30,45
    minutes = ((now.minute // 15) + 1) * 15
    if minutes >= 60:
        next_slot = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    else:
        next_slot = now.replace(minute=minutes, second=0, microsecond=0)

    # add small jitter
    jitter = random.randint(*JITTER_SEC)
    next_slot = next_slot + datetime.timedelta(seconds=jitter)

    seconds = (next_slot - datetime.datetime.now()).total_seconds()
    if seconds > 0:
        time.sleep(seconds)

def run_once():
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_write(f"[{ts}] Starting tracker run")
    try:
        cp = subprocess.run([PYTHON, SCRIPT], capture_output=True, text=True)
        if cp.stdout:
            log_write(cp.stdout)
        if cp.stderr:
            log_write("STDERR:\n" + cp.stderr)
        log_write(f"[{ts}] Run finished with exit code {cp.returncode}\n")
    except Exception as e:
        log_write(f"Exception while running: {type(e).__name__}: {e}\n")

def main():
    # immediate first run (optional: comment out if you want to wait until next slot first)
    while True:
        if acquire_lock():
            try:
                run_once()
            finally:
                release_lock()
        else:
            log_write("Another run appears to be active (lock present). Skipping this cycle.\n")

        # wait until the next 15-minute boundary
        sleep_until_next_slot()

if __name__ == "__main__":
    main()
