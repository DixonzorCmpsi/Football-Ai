"""Which NFL week is it? One rule for the pipeline scripts, the same one the app uses.

The provider's "current week" returns 0 between phases and failed outright at times, and the injuries
script then tagged its snapshot as week 18 (the offseason fallback). That left a bogus future week in
the table. The schedule is authoritative: the first week with a game still to be played, moving on to
the next week once every game of that week has a final score.
"""

from __future__ import annotations

import os
from datetime import date, datetime

import polars as pl

HERE = os.path.dirname(os.path.abspath(__file__))


def read_schedule(season: int) -> pl.DataFrame:
    """The season's schedule from its CSV, or from the database when the CSV is not there."""
    path = os.path.join(HERE, f"schedule_{season}.csv")
    if os.path.exists(path):
        return pl.read_csv(path, ignore_errors=True)
    uri = os.getenv("DB_CONNECTION_STRING")
    if not uri:
        return pl.DataFrame()
    try:
        from sqlalchemy import create_engine, text

        with create_engine(uri).connect() as conn:
            result = conn.execute(text(f"SELECT week, gameday, home_score FROM schedule WHERE season = {int(season)}"))
            rows = result.fetchall()
            return pl.DataFrame(rows, schema=list(result.keys()), orient="row") if rows else pl.DataFrame()
    except Exception:
        return pl.DataFrame()


def active_week(schedule: pl.DataFrame, today: date | None = None) -> int | None:
    """The week the app is on, from the schedule alone; None when there is no usable schedule."""
    today = today or datetime.now().date()
    if schedule.is_empty() or not {"week", "gameday"} <= set(schedule.columns):
        return None
    dated = schedule.with_columns(
        pl.col("gameday").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("_day"),
        pl.col("week").cast(pl.Int64, strict=False),
    ).drop_nulls(subset=["_day", "week"])
    if dated.is_empty():
        return None
    upcoming = dated.filter(pl.col("_day") >= today)
    base = int((upcoming if not upcoming.is_empty() else dated).sort("_day")["week"][0])
    last = int(dated["week"].max())
    games = dated.filter(pl.col("week") == base)
    if "home_score" in games.columns and games.height and games["home_score"].null_count() == 0:
        return min(base + 1, last)
    return base
