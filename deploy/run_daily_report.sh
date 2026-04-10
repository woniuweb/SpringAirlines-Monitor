#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/fare-monitor/app}"
RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/fare-monitor/runtime}"
VENV_PATH="${VENV_PATH:-$APP_ROOT/.venv}"
CONFIG_PATH="${CONFIG_PATH:-$APP_ROOT/fare-monitor.toml}"
ENV_FILE="${ENV_FILE:-$APP_ROOT/deploy/fare-monitor.env}"
SCAN_DAYS="${SCAN_DAYS:-180}"
LOCK_FILE="${LOCK_FILE:-$RUNTIME_ROOT/fare-monitor.lock}"

mkdir -p "$RUNTIME_ROOT"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ ! -d "$VENV_PATH" ]]; then
  echo "Missing virtualenv: $VENV_PATH" >&2
  exit 1
fi

# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"
cd "$APP_ROOT"

exec flock -n "$LOCK_FILE" \
  python -m fare_monitor run-and-email \
    --base-dir "$RUNTIME_ROOT" \
    --config "$CONFIG_PATH" \
    --days "$SCAN_DAYS"
