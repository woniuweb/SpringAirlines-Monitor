from __future__ import annotations

import sys

from fare_monitor.browser_agent import (
    PLAYWRIGHT_EXTRACT_SCRIPT,
    PLAYWRIGHT_ROUTE_SCAN_SCRIPT,
    PLAYWRIGHT_WINDOW_SCRIPT,
)

SCRIPT_BY_MODE = {
    "extract": PLAYWRIGHT_EXTRACT_SCRIPT,
    "window": PLAYWRIGHT_WINDOW_SCRIPT,
    "route-scan": PLAYWRIGHT_ROUTE_SCAN_SCRIPT,
}


def run_browser_worker(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("Missing browser worker mode.")

    mode = args[0]
    script = SCRIPT_BY_MODE.get(mode)
    if script is None:
        raise SystemExit(f"Unsupported browser worker mode: {mode}")

    original_argv = sys.argv[:]
    try:
        sys.argv = ["browser-worker", *args[1:]]
        exec_globals = {"__name__": "__main__", "__builtins__": __builtins__}
        exec(script, exec_globals, exec_globals)
    finally:
        sys.argv = original_argv
    return 0
