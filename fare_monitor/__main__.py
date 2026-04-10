import sys

from fare_monitor.browser_worker import run_browser_worker
from fare_monitor.cli import app


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--browser-worker":
        raise SystemExit(run_browser_worker(sys.argv[2:]))
    app()
