# go-api — Go port (proof-of-concept)

A Go reimplementation of the **read-heavy, no-ML slice** of the FastAPI backend,
built to measure whether moving the serving layer to Go improves performance.

## What's ported

| Endpoint | Source (Python) | Notes |
|---|---|---|
| `GET /tier_list/pool/{position}` | `applications/api/routes/tier_list.py` | The headline target: filter players by position, aggregate season stats, sort, serialize. `position` ∈ `QB/RB/WR/TE/ALL`, plus `?include_rookies_only=true`. |
| `GET /current_week` | `routes/general.py` | |
| `GET /health` | `routes/general.py` | Trimmed to the data-readiness fields. |

The aggregation logic (`_aggregate_player_stats`, `calculate_fantasy_points`,
`_calculate_defensive_points`, the rookie flag, and the sort) is ported
faithfully in [`data.go`](data.go).

## Why this slice

The live API loads Postgres tables into memory (Polars) and, per request,
filters/aggregates/serializes that data — no XGBoost inference on this path. That
makes it cleanly portable and a fair test of Go's concurrency (no GIL) and JSON
encoding vs Python/Polars. The offline ML/ETL pipeline (XGBoost training, pandas,
selenium scrapers, `nflreadpy`) is **out of scope** — it's Python-ecosystem-locked.

## Data

Both Go and the Python harness read the **same CSVs** from `../rag_data`:
`player_profiles_2026.csv` + `weekly_player_stats_2025.csv` (+ snaps, which are
skipped — the snap CSV keys on `pfr_id`, not `player_id`, so the Python path's
snap-join guard is false; Go matches that). Postgres isn't required.

## Run

```bash
# Go (port 8002)
go build -o go-api.exe . && ./go-api.exe --addr :8002 --rag-dir ../rag_data --stats-season 2025

# Python reference server (port 8001), from backend/
ALLOW_CSV_FALLBACK=true ./.venv/Scripts/python.exe go-api/harness.py serve 8001
```

## Verify parity + benchmark

```bash
python go-api/harness.py dump go-api/ref   # dump Python reference JSON
python go-api/compare.py                    # assert Go == Python (field-level, all positions)
python go-api/bench.py 600                   # throughput + latency, both servers
```

`compare.py` confirms **byte-for-byte field parity** across all positions
(identical counts, ordering, and every stat value).

## Files

- `main.go` — server, flags, CSV-backed `DataStore` load
- `data.go` — CSV parsing, stat aggregation, rounding (matches CPython `round()`)
- `handlers.go` — the three HTTP handlers + JSON shapes
- `harness.py` — Python parity/benchmark server using the **real** `get_position_pool`
- `compare.py` — parity assertion (Go vs Python)
- `bench.py` — closed-loop load test
