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


# ---------------------------------------------------------------------------
# Opponent context
#
# The 17 opponent-derived model features (opp_def_*, opp_off_*, rolling_avg_*)
# were read from `weekly_defense_stats_<SEASON>` / `weekly_offense_stats_<SEASON>`,
# which hold only the current season -- 4 rows each in week 1. So every one of
# them was 0 early in a season, for the same reason the player lags were.
#
# Prior seasons have no such tables, but they are fully reconstructable: team
# yardage is the sum of its players' box scores, and points come from the real
# game scores in the schedule. Sacks and interceptions allowed by a defense are
# the sacks suffered and interceptions thrown by the offense it faced.
# ---------------------------------------------------------------------------

OFFENSE_METRICS = ["passing_yards", "rushing_yards", "total_yards"]
DEFENSE_METRICS = ["points_allowed", "passing_yards_allowed", "rushing_yards_allowed",
                   "def_sacks", "def_interceptions"]

# Step 13 renames two defensive rollups to what the models were trained on.
DEFENSE_ROLLUP_RENAMES = {
    "rolling_avg_def_sacks_4_weeks": "rolling_avg_sack_4_weeks",
    "rolling_avg_def_interceptions_4_weeks": "rolling_avg_interception_4_weeks",
}


def team_offense_from_players(player_df: pd.DataFrame) -> pd.DataFrame:
    """Per-team, per-game offensive totals summed from player box scores."""
    if player_df is None or player_df.empty:
        return pd.DataFrame()
    if not {"team", "week"}.issubset(player_df.columns):
        return pd.DataFrame()

    df = player_df.copy()
    for col in ("passing_yards", "rushing_yards", "sacks_suffered", "interception"):
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    keys = ["team", "week"] + (["season"] if "season" in df.columns else [])
    opp = None
    if "opponent_team" in df.columns:
        # Carry the opponent through so defense can be mirrored off this frame.
        opp = df.groupby(keys)["opponent_team"].agg(
            lambda s: s.dropna().iloc[0] if s.notna().any() else None)

    agg = df.groupby(keys).agg(
        passing_yards=("passing_yards", "sum"),
        rushing_yards=("rushing_yards", "sum"),
        sacks_suffered=("sacks_suffered", "sum"),
        interceptions_thrown=("interception", "sum"),
    ).reset_index()
    agg["total_yards"] = agg["passing_yards"] + agg["rushing_yards"]
    if opp is not None:
        agg = agg.merge(opp.rename("opponent_team").reset_index(), on=keys, how="left")
    return agg


def attach_game_points(team_df: pd.DataFrame, schedules: pd.DataFrame) -> pd.DataFrame:
    """Add points scored and allowed from real final scores.

    Points cannot be recovered from player box scores (field goals, extra points
    and defensive scores are not in them), so deriving them from touchdowns would
    be fabricating a feature. Absent a real score the column stays 0.
    """
    out = team_df.copy()
    if out.empty:
        return out
    out["total_off_points"] = 0.0
    out["points_allowed"] = 0.0
    if schedules is None or schedules.empty:
        return out

    s = schedules.copy()
    required = {"home_team", "away_team", "home_score", "away_score", "week"}
    if not required.issubset(s.columns):
        return out
    for c in ("home_score", "away_score"):
        s[c] = pd.to_numeric(s[c], errors="coerce")
    s = s.dropna(subset=["home_score", "away_score"])
    if s.empty:
        return out

    use_season = "season" in out.columns and "season" in s.columns
    keys = ["team", "week"] + (["season"] if use_season else [])

    home = pd.DataFrame({"team": s["home_team"], "week": s["week"],
                         "scored": s["home_score"], "allowed": s["away_score"]})
    away = pd.DataFrame({"team": s["away_team"], "week": s["week"],
                         "scored": s["away_score"], "allowed": s["home_score"]})
    if use_season:
        home["season"] = s["season"].values
        away["season"] = s["season"].values
    points = pd.concat([home, away], ignore_index=True).drop_duplicates(subset=keys)

    out = out.drop(columns=["total_off_points", "points_allowed"], errors="ignore")
    out = out.merge(points, on=keys, how="left")
    out["total_off_points"] = pd.to_numeric(out.pop("scored"), errors="coerce").fillna(0.0)
    out["points_allowed"] = pd.to_numeric(out.pop("allowed"), errors="coerce").fillna(0.0)
    return out


def mirror_to_defense(team_df: pd.DataFrame) -> pd.DataFrame:
    """Turn each team's offensive line into its opponent's defensive line."""
    if team_df is None or team_df.empty or "opponent_team" not in team_df.columns:
        return pd.DataFrame()
    src = team_df.dropna(subset=["opponent_team"]).copy()
    if src.empty:
        return pd.DataFrame()

    out = pd.DataFrame({
        "team": src["opponent_team"].values,
        "week": src["week"].values,
        "passing_yards_allowed": src["passing_yards"].values,
        "rushing_yards_allowed": src["rushing_yards"].values,
        # A defense's sacks are the sacks its opponent suffered.
        "def_sacks": src["sacks_suffered"].values if "sacks_suffered" in src.columns else 0.0,
        "def_interceptions": (src["interceptions_thrown"].values
                              if "interceptions_thrown" in src.columns else 0.0),
        "points_allowed": (src["total_off_points"].values
                           if "total_off_points" in src.columns else 0.0),
    })
    keys = ["team", "week"]
    if "season" in team_df.columns:
        out["season"] = src["season"].values
        keys.append("season")
    return out.drop_duplicates(subset=keys)


def _team_rollups(df: pd.DataFrame, metrics, lag_prefix: str,
                  rolling_prefix: str, window: int = 4) -> pd.DataFrame:
    """Lag and 4-game rolling mean per team, ordered across seasons."""
    if df is None or df.empty or "team" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    sort_cols = ["team"] + [c for c in ("season", "week") if c in out.columns]
    out = out.sort_values(sort_cols).reset_index(drop=True)
    for col in metrics:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
        shifted = out.groupby("team")[col].shift(1)
        out[f"{rolling_prefix}{col}_{window}_weeks"] = (
            shifted.groupby(out["team"]).rolling(window, min_periods=1).mean()
            .reset_index(level=0, drop=True).fillna(0.0)
        )
        for lag in (1, 2, 3):
            out[f"{lag_prefix}{col}_lag_{lag}"] = out.groupby("team")[col].shift(lag).fillna(0.0)
    return out


def add_opponent_features(player_df: pd.DataFrame, offense: pd.DataFrame,
                          defense: pd.DataFrame) -> pd.DataFrame:
    """Merge opponent offensive and defensive rollups onto each player row."""
    out = player_df.copy()
    if "opponent_team" not in out.columns:
        return out

    def _merge(side_df, metrics, lag_prefix, rolling_prefix):
        nonlocal out
        if side_df is None or side_df.empty:
            return
        rolled = _team_rollups(side_df, metrics, lag_prefix, rolling_prefix)
        if rolled.empty:
            return
        rolled = rolled.rename(columns=DEFENSE_ROLLUP_RENAMES)
        keep = [c for c in rolled.columns
                if c.startswith(lag_prefix) or c.startswith(rolling_prefix)
                or c in DEFENSE_ROLLUP_RENAMES.values()]
        id_cols = ["team"] + [c for c in ("season", "week") if c in rolled.columns]
        rolled = rolled[id_cols + keep].rename(columns={"team": "opponent_team"})
        join_keys = ["opponent_team"] + [c for c in ("season", "week")
                                         if c in rolled.columns and c in out.columns]
        out = out.drop(columns=[c for c in keep if c in out.columns], errors="ignore")
        out = out.merge(rolled, on=join_keys, how="left")

    _merge(defense, DEFENSE_METRICS, "opp_def_", "rolling_avg_")
    _merge(offense, OFFENSE_METRICS + ["total_off_points"], "opp_off_", "opp_off_rolling_")

    num_cols = out.select_dtypes(include=["number"]).columns
    out[num_cols] = out[num_cols].fillna(0.0)
    return out


def add_defense_vs_position(player_df: pd.DataFrame, window: int = 4) -> pd.DataFrame:
    """Rolling fantasy points each defense allows to each position.

    Computed over every season present, so week 1 of a new season inherits the
    previous season's tail instead of starting flat like the other rollups did.
    """
    out = player_df.copy()
    needed = {"opponent_team", "week", "position", "y_fantasy_points_ppr"}
    if not needed.issubset(out.columns):
        return out

    has_season = "season" in out.columns
    group = ["opponent_team", "week", "position"] + (["season"] if has_season else [])
    dvp = out.groupby(group)["y_fantasy_points_ppr"].sum().reset_index()
    sort_cols = ["opponent_team", "position"] + [c for c in ("season", "week") if c in dvp.columns]
    dvp = dvp.sort_values(sort_cols).reset_index(drop=True)
    dvp["allowed"] = (
        dvp.groupby(["opponent_team", "position"])["y_fantasy_points_ppr"]
        .transform(lambda x: x.shift(1).rolling(window, min_periods=1).mean())
        .fillna(0.0)
    )
    index_cols = ["opponent_team", "week"] + (["season"] if has_season else [])
    wide = dvp.pivot_table(index=index_cols, columns="position", values="allowed").reset_index()
    wide.columns = [f"rolling_avg_points_allowed_to_{c}" if c in ("QB", "RB", "WR", "TE") else c
                    for c in wide.columns]
    drop = [c for c in wide.columns
            if c.startswith("rolling_avg_points_allowed_to_") and c in out.columns]
    out = out.drop(columns=drop, errors="ignore")
    out = out.merge(wide, on=index_cols, how="left")
    for pos in ("QB", "RB", "WR", "TE"):
        col = f"rolling_avg_points_allowed_to_{pos}"
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out
