# Copilot Instructions — Football-Ai

Quick reference for an AI code assistant working on this repo. Keep edits small and test locally where possible.

## Big picture (high-level)
- Backend: FastAPI app in `backend/applications/server.py`. Holds in-memory Polars frames (`model_data`), ML models (joblib) and implements most API endpoints.
- ETL / Data layer: scripts in `backend/rag_data/` (master orchestrator: `05_etl_to_postgres.py`) populate Postgres tables used by the server. ETL uses Polars + SQLAlchemy and has "smart" skip/evolve behavior.
- Frontend: Vite + React in `Dashboard/predictor-frontend/`. UI reads from backend endpoints and presents schedule, compare, lookup, history and trending lists.
- Data model: canonical table names include `weekly_player_stats_{SEASON}`, `weekly_snap_counts_{SEASON}`, `weekly_injuries_{SEASON}`, `weekly_feature_set_{SEASON}`, `bovada_game_lines`, `bovada_player_props`.

## How to run / dev flows
- Backend (dev):
  - Preferred: use Docker Compose (see `docker-compose.yml`) — `docker-compose up --build db backend` (ensures DB is present).
  - Direct: from repo root run backend with reload: `cd backend && uvicorn applications.server:app --reload --host 0.0.0.0 --port 8000`.
- Frontend (dev):
  - `cd Dashboard/predictor-frontend && npm install && npm run dev` (Vite). Note: root `package.json` uses `npm --prefix frontend` which is out-of-date; use the path above.
- ETL: Prefer running via Docker Compose in the `backend` service (ensures native deps):
  - `docker-compose up --build db backend` then inside container: `python3 rag_data/05_etl_to_postgres.py` or from host: `docker-compose run --rm backend python3 rag_data/05_etl_to_postgres.py`
  - If you prefer local Python, use the included helper `backend/rag_data/run_etl.py`:
    - `python3 backend/rag_data/run_etl.py --check-deps` (verifies required packages like `polars` are installed)
    - `python3 backend/rag_data/run_etl.py --full` (runs full ETL locally; ensure `pip install -r backend/requirements.txt` first)
    - `python3 backend/rag_data/run_etl.py --step 13_generate_production_features.py` (run a single step with automatic upload)
  - Rationale: ETL depends on Polars, Selenium, and platform-specific drivers; containers are the most reliable way to run it.

## Important conventions & patterns (do not assume defaults)
- DB-first: the server tries Postgres first; CSV fallback only if `ALLOW_CSV_FALLBACK=true` in env (dev mode fallback).
- Polars is used everywhere for in-memory frames; convert to pandas only when pushing to SQL.
- Feature / model pairing: models live under `backend/model_training` (or `backend/applications`'s `MODEL_DIR`) as `.joblib` with a companion JSON feature list (e.g. `feature_names_*.json`). Changes to feature names require updating the JSON and retraining.
- ETL smart checks: `05_etl_to_postgres.py` will skip script steps when the target DB table exists and is non-empty (avoid emptying tables unintentionally). For schema changes, ETL may switch a step to `replace` mode.
- Server lifecycle: `lifespan` in `server.py` loads models and data on startup and schedules a daily ETL run (APScheduler @ 06:00). A startup ETL is now triggered asynchronously at boot.
- Health checks: a lightweight `/health` endpoint is available (reports models loaded, DB connectivity probe, ETL script presence, current week).

## API/Frontend specifics to know
- Key backend endpoints:
  - `GET /player/{player_id}` -> full player card (prediction, props, injury status, `is_injury_boosted`)
  - `POST /compare` -> returns two player cards + weekly histories
  - `GET /current_week`, `GET /schedule/{week}`, `GET /player/history/{player_id}`
  - `GET /health` -> health probe (useful for orchestrators)
- Frontend components to check when changing visualizations:
  - `Dashboard/predictor-frontend/src/components/CompareView.tsx` (radar + line chart logic)
  - `ComparisonHistory.tsx` (alternative radar implementation)
  - `PlayerHistory.tsx` (history table & TD counting)
- Trending list (sleeper API): fetched by server and attached to watchlist/trending endpoints; UI shows them in sidebars (desktop) and via a mobile "Trending" view (mobile button + a small footer quick-nav for phones). Also: the app UI is designed to be responsive — sidebars are hidden on small screens and important actions are accessible via the footer mobile nav.

## Debugging & Diagnostics
- Usage boost debugging: new endpoint `GET /debug/usage-boost/{player_id}/{week}` returns whether a usage boost would apply and, if so, which teammate triggered it and why (average points and snaps). Useful for reproducing cases like Brock Purdy.
- ETL runner: use `backend/rag_data/run_etl.py` for cross-platform ETL execution and dependency checks.

## Common pitfalls / gotchas
- Root `package.json` references a non-existent `frontend` folder — prefer `Dashboard/predictor-frontend` commands.
- The ETL script depends on the Python environment (Polars, Selenium for Bovada scraping, etc). Run inside the backend container or ensure `pip install -r backend/requirements.txt`.
- ETL `push_to_postgres` has "smart" modes (`if_missing`, `smart_append`, `replace`). Be cautious when changing CSV schemas — the script can replace tables (intended, but destructive if unexpected).
- Injury / usage logic: the server computes an `is_injury_boosted` flag by scanning teammates and their injury reports; string matching for statuses may be case-insensitive and snap thresholds exist — changes here affect many cards across the UI.

## What to check before PR
- Run the ETL (or a subset) to ensure DB tables used by endpoints are present and non-empty.
- Start backend and verify `/health` responds and that data frames (`df_player_stats`, `df_snap_counts`, `df_injuries`) are loaded into memory.
- Run the frontend locally and exercise: Compare view (radar + line), Player history TD counting, Trending mobile button.
- If changing the ETL or DB schema, verify `05_etl_to_postgres.py` smart-checks don't inadvertently mark a step as skipped.

## Notes for AI contributors
- Prefer small, testable changes and include the file(s) you ran when verifying behavior (e.g. ETL logs, server logs from `/health`).
- When adjusting model code, update the corresponding `feature_names_*.json` if you change feature sets and ensure model artifacts are in `MODEL_DIR`.
- When debugging UI-reporting mismatches (e.g. TD counts), look for: how backend constructs history (`/player/history`), how front-end aggregates it (`PlayerHistory` / `CompareView`), and whether the field is `touchdowns` or derived from passing/rushing/receiving columns.

---
If anything here is outdated or you prefer a different formatting, tell me what to add or remove and I’ll iterate.