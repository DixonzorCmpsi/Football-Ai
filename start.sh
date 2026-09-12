#!/usr/bin/env bash
set -euo pipefail

# start.sh - Local dev runner for Football-Ai.
#
# Modes:
#   ./start.sh                  -> docker (full stack via docker compose)
#   ./start.sh local            -> local (Postgres in docker, backend+frontend on host)
#   ./start.sh frontend         -> frontend dev server; starts/checks backend first
#   ./start.sh backend          -> backend only (Postgres in docker if available, CSV fallback otherwise)
#   ./start.sh db               -> database only (Postgres in docker)
#   ./start.sh stop             -> stop docker stack
#
# Env overrides:
#   BACKEND_PORT (default 8000)
#   FRONTEND_PORT (default 5273)
#   DB_CONNECTION_STRING (default postgresql://admin:password@localhost:5432/football_ai)
#   RUN_ETL_ON_STARTUP (default true for local mode)
#   ALLOW_CSV_FALLBACK (default false). Set to true ONLY to run deliberately on the
#                      last CSV snapshot. Defaulting it to true let a stopped Docker
#                      daemon turn into silently stale data.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-docker}"

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5273}"
DB_CONNECTION_STRING="${DB_CONNECTION_STRING:-postgresql://admin:password@localhost:5432/football_ai}"

color() { printf "\033[1;36m%s\033[0m\n" "$*"; }
warn()  { printf "\033[1;33m%s\033[0m\n" "$*"; }
err()   { printf "\033[1;31m%s\033[0m\n" "$*" >&2; }

compose() {
  (cd "$ROOT" && docker compose "$@")
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    err "Docker is required to start Postgres. Install Docker Desktop and retry."
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    # Docker Desktop does not always come back after a reboot. On macOS, start it
    # rather than fail (or, as this script used to, fall back to stale CSV data).
    if [[ "$(uname -s)" == "Darwin" ]] && open -a Docker >/dev/null 2>&1; then
      color "Docker is not running; starting Docker Desktop..."
      for _ in $(seq 1 90); do
        docker info >/dev/null 2>&1 && return 0
        sleep 2
      done
    fi
    err "Docker is not running. Start Docker Desktop and retry."
    exit 1
  fi
}

wait_for_db() {
  color "Waiting for Postgres to accept connections..."
  for _ in $(seq 1 45); do
    if compose exec -T db pg_isready -U admin -d football_ai >/dev/null 2>&1; then
      color "Postgres is ready on localhost:5432"
      return 0
    fi
    sleep 1
  done
  err "Postgres did not become ready in time. Run 'docker compose logs db' for details."
  exit 1
}

ensure_db() {
  require_docker
  color "Starting Postgres container..."
  compose up -d db
  wait_for_db
}

ensure_db_or_csv_fallback() {
  if [[ "${ALLOW_CSV_FALLBACK:-false}" == "true" ]] && ! { command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; }; then
    warn "=================================================================="
    warn " WARNING: running on the LAST CSV SNAPSHOT, not the live database."
    warn " Data is stale. Unset ALLOW_CSV_FALLBACK to require Postgres."
    warn "=================================================================="
    unset DB_CONNECTION_STRING
    return 0
  fi
  # Default: the live database is required. Failing loudly beats serving stale data.
  ensure_db
  export DB_CONNECTION_STRING
}

backend_ready() {
  "$PYTHON" - <<PY >/dev/null 2>&1
import json, sys, urllib.request
try:
    with urllib.request.urlopen("http://127.0.0.1:${BACKEND_PORT}/health", timeout=5) as r:
        data = json.load(r)
    sys.exit(0 if data.get("ready") else 1)
except Exception:
    sys.exit(1)
PY
}

wait_for_backend() {
  color "Waiting for backend player data..."
  for _ in $(seq 1 90); do
    if backend_ready; then
      color "Backend is ready on http://localhost:$BACKEND_PORT"
      return 0
    fi
    sleep 1
  done
  err "Backend did not become data-ready. Check the backend window/logs."
  exit 1
}

# Pick a Python 3.10+ interpreter (nflreadpy and friends require it).
pick_python() {
  for cand in python3.12 python3.11 python3.10 python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then
      ver=$("$cand" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")' 2>/dev/null || echo "")
      case "$ver" in
        3.1[0-9]|3.[2-9]*) echo "$cand"; return 0 ;;
      esac
    fi
  done
  return 1
}
PYTHON="$(pick_python || true)"

start_backend() {
  color "Starting backend on :$BACKEND_PORT"
  cd "$ROOT/backend"
  if [[ -z "${PYTHON:-}" ]]; then
    err "No Python 3.10+ found. Install Python 3.10 or newer and retry."
    exit 1
  fi
  if [[ ! -d .venv ]]; then
    color "Creating Python venv (.venv) using $PYTHON ($("$PYTHON" --version))..."
    "$PYTHON" -m venv .venv
  fi
  # shellcheck disable=SC1091
  source .venv/bin/activate
  pip install --quiet --upgrade pip
  pip install --quiet -r requirements.txt 2>/dev/null || pip install --quiet \
    fastapi uvicorn polars sqlalchemy psycopg2-binary apscheduler joblib \
    xgboost scikit-learn nflreadpy requests python-dotenv
  export DB_CONNECTION_STRING
  export ALLOW_CSV_FALLBACK="${ALLOW_CSV_FALLBACK:-false}"
  export RUN_ETL_ON_STARTUP="${RUN_ETL_ON_STARTUP:-true}"
  # The venv's interpreter by path, not whatever `uvicorn` PATH resolves to.
  exec .venv/bin/python -m uvicorn applications.server:app --reload --host 0.0.0.0 --port "$BACKEND_PORT"
}

ensure_backend() {
  if backend_ready; then
    color "Backend already ready on :$BACKEND_PORT"
    return 0
  fi
  ensure_db_or_csv_fallback
  color "Starting backend dependency on :$BACKEND_PORT"
  (start_backend) &
  BACKEND_PID=$!
  wait_for_backend
}

start_frontend() {
  color "Starting frontend on :$FRONTEND_PORT"
  cd "$ROOT/Dashboard/predictor-frontend"
  if [[ ! -d node_modules ]]; then
    color "Installing frontend dependencies (npm install)..."
    npm install
  fi
  export VITE_API_BASE_URL="http://localhost:$BACKEND_PORT/api"
  exec npm run dev -- --port "$FRONTEND_PORT" --host
}

case "$MODE" in
  docker)
    require_docker
    color "Starting database first..."
    compose up --build -d db
    wait_for_db
    color "Starting backend + frontend via docker compose..."
    compose up --build -d backend frontend
    color "-> Frontend:  http://localhost"
    color "-> Backend:   http://localhost:8000"
    color "-> Postgres:  localhost:5432 (admin/password)"
    compose ps
    ;;
  local)
    ensure_db_or_csv_fallback
    color "Launching backend + frontend in parallel (Ctrl-C stops both)"
    (start_backend) &
    BACKEND_PID=$!
    wait_for_backend
    (start_frontend) &
    FRONTEND_PID=$!
    trap 'kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true' INT TERM
    wait
    ;;
  backend)
    ensure_db_or_csv_fallback
    start_backend
    ;;
  frontend)
    ensure_backend
    start_frontend
    ;;
  db)
    ensure_db
    compose ps db
    ;;
  stop)
    color "Stopping docker stack"
    compose down
    ;;
  *)
    err "Unknown mode: $MODE"
    echo "Usage: ./start.sh [docker|local|backend|frontend|db|stop]"
    exit 1
    ;;
esac
