"""Python parity/benchmark harness for the Go port.

Loads the exact same CSVs the Go service reads into the real `model_data`, then
exercises the genuine `routes.tier_list.get_position_pool` so the comparison is
apples-to-apples (same data, same logic the production app runs).

Modes:
  dump  <out_dir>   write reference JSON for each position to <out_dir>
  serve [port]      run a uvicorn server exposing the same endpoints (default 8001)
"""
import asyncio
import json
import os
import sys

# Make `applications` importable (backend/ is two levels up from this file).
BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BACKEND_DIR)

import polars as pl  # noqa: E402

from applications.api.state import model_data  # noqa: E402
from applications.api.services.utils import enforce_types  # noqa: E402
from applications.api.routes import tier_list  # noqa: E402

RAG_DIR = os.path.join(BACKEND_DIR, "rag_data")
STATS_SEASON = int(os.environ.get("STATS_SEASON", "2025"))
POSITIONS = ["QB", "RB", "WR", "TE", "ALL"]


def load_data():
    """Populate model_data identically to how the Go service loads its store."""
    profiles = pl.read_csv(
        os.path.join(RAG_DIR, "player_profiles_2026.csv"), ignore_errors=True
    )
    stats = pl.read_csv(
        os.path.join(RAG_DIR, f"weekly_player_stats_{STATS_SEASON}.csv"),
        ignore_errors=True,
    )
    snaps = pl.read_csv(
        os.path.join(RAG_DIR, f"weekly_snap_counts_{STATS_SEASON}.csv"),
        ignore_errors=True,
    )
    model_data["df_profile"] = enforce_types(profiles)
    model_data["df_player_stats"] = enforce_types(stats)
    model_data["df_snap_counts"] = enforce_types(snaps)
    model_data["injury_map"] = {}
    model_data["gsis_to_sleeper"] = {}
    model_data["sleeper_map"] = {}
    model_data["current_nfl_week"] = 1


def dump(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    load_data()
    for pos in POSITIONS:
        rows = asyncio.run(tier_list.get_position_pool(pos))
        with open(os.path.join(out_dir, f"pool_{pos}.json"), "w") as f:
            json.dump(rows, f)
        print(f"{pos}: {len(rows)} players -> {out_dir}/pool_{pos}.json")


def build_app():
    from fastapi import FastAPI

    load_data()
    app = FastAPI()

    @app.get("/current_week")
    async def current_week():
        return {"week": model_data.get("current_nfl_week", 1)}

    @app.get("/health")
    async def health():
        return {"status": "ok", "ready": True}

    @app.get("/tier_list/pool/{position}")
    async def pool(position: str, include_rookies_only: bool = False):
        return await tier_list.get_position_pool(position, include_rookies_only)

    return app


def serve(port: int):
    import uvicorn

    uvicorn.run(build_app(), host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "dump"
    if mode == "dump":
        dump(sys.argv[2] if len(sys.argv) > 2 else "ref")
    elif mode == "serve":
        serve(int(sys.argv[2]) if len(sys.argv) > 2 else 8001)
    else:
        print(f"unknown mode: {mode}")
        sys.exit(1)
