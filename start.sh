#!/usr/bin/env bash
set -euo pipefail

# start.sh — Local dev runner for Football-Ai.
#
# Modes:
#   ./start.sh                  → docker (full stack via docker compose)
#   ./start.sh local            → local (Postgres in docker, backend+frontend on host)
#   ./start.sh frontend         → frontend dev server only (Vite, port 5273)
#   ./start.sh backend          → backend only (uvicorn on host, port 8000)
#   ./start.sh stop             → stop docker stack
#
# Env overrides:
#   BACKEND_PORT (default 8000)
#   FRONTEND_PORT (default 5273)
#   DB_CONNECTION_STRING (default postgresql://admin:password@localhost:5432/football_ai)
#   RUN_ETL_ON_STARTUP (default false for local mode)
#   ALLOW_CSV_FALLBACK (default true for local mode)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-docker}"

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5273}"
DB_CONNECTION_STRING="${DB_CONNECTION_STRING:-postgresql://admin:password@localhost:5432/football_ai}"

# Pick a Python 3.10+ interpreter (nflreadpy and friends require it).
pick_python() {
  for cand in python3.12 python3.11 python3.10 python3; do
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

color() { printf "\033[1;36m%s\033[0m\n" "$*"; }
warn()  { printf "\033[1;33m%s\033[0m\n" "$*"; }
err()   { printf "\033[1;31m%s\033[0m\n" "$*" >&2; }

ensure_db() {
  if ! docker ps --format '{{.Names}}' | grep -q '^football-ai[-_]db'; then
    color "Starting Postgres container..."
    (cd "$ROOT" && docker compose up -d db)
    sleep 3
  fi
}

start_backend() {
  color "Starting backend on :$BACKEND_PORT"
  cd "$ROOT/backend"
  if [[ -z "${PYTHON:-}" ]]; then
    err "No Python 3.10+ found. Install one (e.g. 'brew install python@3.11') and retry."
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
  export ALLOW_CSV_FALLBACK="${ALLOW_CSV_FALLBACK:-true}"
  export RUN_ETL_ON_STARTUP="${RUN_ETL_ON_STARTUP:-false}"
  exec uvicorn applications.server:app --reload --host 0.0.0.0 --port "$BACKEND_PORT"
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
    color "Starting full stack via docker compose"
    cd "$ROOT"
    docker compose up --build -d
    color "→ Frontend:  http://localhost"
    color "→ Backend:   http://localhost:8000"
    color "→ Postgres:  localhost:5432 (admin/password)"
    docker compose ps
    ;;
  local)
    ensure_db
    color "Launching backend + frontend in parallel (Ctrl-C stops both)"
    (start_backend) &
    BACKEND_PID=$!
    sleep 2
    (start_frontend) &
    FRONTEND_PID=$!
    trap 'kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true' INT TERM
    wait
    ;;
  backend) ensure_db; start_backend ;;
  frontend) start_frontend ;;
  stop)
    color "Stopping docker stack"
    cd "$ROOT" && docker compose down
    ;;
  *)
    err "Unknown mode: $MODE"
    echo "Usage: ./start.sh [docker|local|backend|frontend|stop]"
    exit 1
    ;;
esac
