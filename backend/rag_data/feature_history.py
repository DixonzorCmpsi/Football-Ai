"""Prior-season seeding for the production feature set.

Why this exists
---------------
`13_generate_production_features.py` built its lag features with

    df.groupby('player_id')[col].shift(lag).fillna(0)

over `weekly_player_stats_<CURRENT_SEASON>` alone. Two consequences, both of
which made the models useless in the first weeks of a season:

1. Every `*_lag_1/2/3` was 0 in week 1, because there is no earlier row in the
   current season. Measured on the live 2026 week-1 set: 128 of 129 lag/rolling
   features were zero, the only non-zero one being `draft_ovr`. The QB model
   therefore emitted a near-constant deviation -- 3.120 for Lamar Jackson,
   Mahomes, Herbert and Stroud alike -- so projections did not discriminate
   between players at all.

2. The row universe was "players who already have a stat row this season", which
   in week 1 was 47 players out of 492 active skill players. The other 445 got no
   model contribution whatsoever.

This module supplies the two missing pieces: prior-season rows normalized into
the same derived schema (so lags reach back across the season boundary), and
placeholder rows for an upcoming week (so a player is scored before he has
played). Everything here is pure -- no DB, no network -- so it can be tested.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Raw nflverse weekly dumps use different names than the current-season ETL
# output. Step 13 already applies these to the current season; prior seasons
# need the same treatment before the two can be concatenated.
CANONICAL_RENAMES = {
    "attempts": "pass_attempts",
    "receiving_yards_after_catch": "yards_after_catch",
    "receiving_tds": "receiving_touchdown",
    "passing_tds": "passing_touchdown",
    "rushing_tds": "rush_touchdown",
    "interceptions": "interception",
}

# Columns the model lags. Anything absent is created as 0 so `shift` still works.
LAG_COLUMNS = [
    "offense_snaps", "offense_pct", "targets", "receptions", "receiving_yards",
    "rushing_yards", "rush_attempts", "y_fantasy_points_ppr",
    "pass_attempts", "passing_yards", "passing_touchdown", "interception",
    "receiving_touchdown", "rush_touchdown",
    "team_targets_share", "team_receptions_share", "team_rush_attempts_share",
    "receiving_air_yards", "passing_air_yards", "yards_after_catch",
    "ypr", "ayptarget", "ypc", "adot", "touches", "passer_rating",
    "receptions_redzone", "targets_redzone",
]

IDENTITY_COLUMNS = ["player_id", "player_name", "position", "team", "opponent_team", "season", "week"]


def _as_series(values) -> pd.Series:
    """Numeric Series from a Series, list or array; non-numeric becomes 0."""
    if not isinstance(values, pd.Series):
        values = pd.Series(list(values))
    return pd.to_numeric(values, errors="coerce").fillna(0.0).astype("float64")


def _safe_div(numerator, denominator):
    """Element-wise divide that yields 0 rather than inf/NaN on a zero denominator."""
    num = _as_series(numerator)
    den = _as_series(denominator).reset_index(drop=True)
    num_vals = num.to_numpy()
    den_vals = den.to_numpy()
    out = np.divide(num_vals, den_vals,
                    out=np.zeros(len(num_vals), dtype="float64"), where=den_vals != 0)
    return pd.Series(out, index=num.index)


def passer_rating(completions, attempts, yards, touchdowns, interceptions) -> pd.Series:
    """Standard NFL passer rating, with each component clamped to [0, 2.375]."""
    att = _as_series(attempts)
    a = ((_safe_div(completions, att) - 0.3) * 5).clip(0, 2.375)
    b = ((_safe_div(yards, att) - 3) * 0.25).clip(0, 2.375)
    c = (_safe_div(touchdowns, att) * 20).clip(0, 2.375)
    d = (2.375 - (_safe_div(interceptions, att) * 25)).clip(0, 2.375)
    rating = ((a + b + c + d) / 6) * 100
    # No attempts means no rating; 0 is the honest value, not a clamped 39.6.
    return rating.where(att > 0, 0.0)


def normalize_weekly_stats(df: pd.DataFrame, season: int | None = None) -> pd.DataFrame:
    """Rename a raw weekly dump into the derived schema and guarantee a season."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    renames = {k: v for k, v in CANONICAL_RENAMES.items()
               if k in out.columns and v not in out.columns}
    if renames:
        out = out.rename(columns=renames)
    if "season" not in out.columns:
        out["season"] = season
    if season is not None:
        out["season"] = pd.to_numeric(out["season"], errors="coerce").fillna(season).astype(int)
    if "player_id" in out.columns:
        out["player_id"] = out["player_id"].astype(str)
    return out


def attach_snap_counts(df: pd.DataFrame, snaps: pd.DataFrame,
                       pfr_to_gsis: dict | None = None) -> pd.DataFrame:
    """Merge offense snap share in, keying on gsis id or translating from pfr id."""
    out = df.copy()
    if "offense_snaps" not in out.columns:
        out["offense_snaps"] = 0.0
    if "offense_pct" not in out.columns:
        out["offense_pct"] = 0.0
    if snaps is None or snaps.empty:
        return out

    s = snaps.copy()
    if "player_id" not in s.columns:
        if "pfr_id" in s.columns and pfr_to_gsis:
            s["player_id"] = s["pfr_id"].map(pfr_to_gsis)
        else:
            return out
    s = s.dropna(subset=["player_id"])
    if s.empty:
        return out
    s["player_id"] = s["player_id"].astype(str)

    keys = ["player_id", "week"]
    if "season" in s.columns and "season" in out.columns:
        keys = ["player_id", "season", "week"]
    cols = keys + [c for c in ("offense_snaps", "offense_pct") if c in s.columns]
    s = s[cols].drop_duplicates(subset=keys)

    out = out.drop(columns=["offense_snaps", "offense_pct"], errors="ignore")
    out = out.merge(s, on=keys, how="left")
    for c in ("offense_snaps", "offense_pct"):
        if c not in out.columns:
            out[c] = 0.0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    return out


def derive_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Rebuild the efficiency/usage columns the current-season ETL adds.

    Prior-season dumps carry only raw volume, so without this the seeded rows
    would lag real yardage but zeroed efficiency -- a subtly worse input than no
    history, because the model would read genuine zeros as genuine performance.
    """
    out = df.copy()
    for col in ("targets", "receptions", "rush_attempts", "receiving_yards",
                "rushing_yards", "receiving_air_yards", "pass_attempts",
                "completions", "passing_yards", "passing_touchdown", "interception",
                "y_fantasy_points_ppr", "receptions_redzone", "targets_redzone"):
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    out["ypr"] = _safe_div(out["receiving_yards"], out["receptions"])
    out["ypc"] = _safe_div(out["rushing_yards"], out["rush_attempts"])
    out["adot"] = _safe_div(out["receiving_air_yards"], out["targets"])
    out["ayptarget"] = out["adot"]
    out["touches"] = out["rush_attempts"] + out["receptions"]
    if "passer_rating" not in df.columns:
        out["passer_rating"] = passer_rating(
            out["completions"], out["pass_attempts"], out["passing_yards"],
            out["passing_touchdown"], out["interception"])
    else:
        out["passer_rating"] = pd.to_numeric(df["passer_rating"], errors="coerce").fillna(0.0)
    return out


def add_team_shares(df: pd.DataFrame) -> pd.DataFrame:
    """Per-game share of the team's targets / receptions / carries."""
    out = df.copy()
    if "team" not in out.columns:
        for c in ("team_targets_share", "team_receptions_share", "team_rush_attempts_share"):
            out[c] = out.get(c, 0.0)
        return out

    group_keys = ["team", "week"]
    if "season" in out.columns:
        group_keys = ["team", "season", "week"]

    for share, base in (("team_targets_share", "targets"),
                        ("team_receptions_share", "receptions"),
                        ("team_rush_attempts_share", "rush_attempts")):
        if base not in out.columns:
            out[share] = 0.0
            continue
        totals = out.groupby(group_keys)[base].transform("sum")
        out[share] = _safe_div(out[base], totals)
    return out


def build_upcoming_rows(players: pd.DataFrame, season: int, week: int) -> pd.DataFrame:
    """Zeroed rows for a week that has not been played yet.

    A player has to exist in the frame before `shift` can give him lag features,
    so without this the feature set only ever covers players who already have a
    box score this season -- 47 of 492 in week 1.
    """
    if players is None or players.empty:
        return pd.DataFrame()
    out = players.copy()
    out["season"] = int(season)
    out["week"] = int(week)
    out["player_id"] = out["player_id"].astype(str)
    # The week is unplayed: every production stat is genuinely zero, and the
    # lag columns get filled from history once this is concatenated.
    for col in LAG_COLUMNS + ["completions", "y_fantasy_points_ppr"]:
        if col not in out.columns:
            out[col] = 0.0
    return out


def add_player_lags(df: pd.DataFrame, columns=LAG_COLUMNS, lags=(1, 2, 3)) -> pd.DataFrame:
    """Shift each column back within a player, ordered across season boundaries.

    Sorting by (season, week) is what lets week 1 of a new season inherit the
    tail of the previous one instead of starting at zero.
    """
    out = df.copy()
    if "player_id" not in out.columns:
        return out
    sort_cols = ["player_id"]
    if "season" in out.columns:
        sort_cols.append("season")
    if "week" in out.columns:
        sort_cols.append("week")
    out = out.sort_values(sort_cols).reset_index(drop=True)

    for col in columns:
        if col not in out.columns:
            out[col] = 0.0
        series = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
        out[col] = series
        grouped = out.groupby("player_id")[col]
        for lag in lags:
            out[f"{col}_lag_{lag}"] = grouped.shift(lag).fillna(0.0)
    return out


def add_season_average(df: pd.DataFrame, value_col: str = "y_fantasy_points_ppr") -> pd.DataFrame:
    """Expanding mean of prior games, carried across seasons like the lags."""
    out = df.copy()
    if "player_id" not in out.columns or value_col not in out.columns:
        out["player_season_avg_points"] = 0.0
        return out
    sort_cols = ["player_id"] + [c for c in ("season", "week") if c in out.columns]
    out = out.sort_values(sort_cols).reset_index(drop=True)
    out["player_season_avg_points"] = (
        out.groupby("player_id")[value_col]
        .transform(lambda x: x.expanding().mean().shift(1))
        .fillna(0.0)
    )
    return out
