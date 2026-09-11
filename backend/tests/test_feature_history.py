"""Prior-season seeding for the production feature set.

The generator built lag features with `groupby('player_id').shift(lag)` over the
current season alone. In week 1 that made every lag 0 -- measured on the live
2026 set, 128 of 129 lag/rolling features were zero -- so the QB model emitted a
near-identical deviation for every quarterback. The row universe was equally
broken: 47 of 492 active skill players had a feature row at all.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rag_data"))

from feature_history import (  # noqa: E402
    add_player_lags,
    add_season_average,
    add_team_shares,
    attach_snap_counts,
    build_upcoming_rows,
    derive_metrics,
    normalize_weekly_stats,
    passer_rating,
)


def _game(pid, season, week, **kw):
    row = {"player_id": pid, "season": season, "week": week, "team": kw.pop("team", "SF")}
    row.update(kw)
    return row


# --------------------------------------------------------------------------
# Normalization
# --------------------------------------------------------------------------

def test_normalize_renames_raw_nflverse_columns():
    df = pd.DataFrame([{"player_id": "a", "attempts": 30, "interceptions": 1,
                        "receiving_yards_after_catch": 40}])
    out = normalize_weekly_stats(df, 2025)
    assert "pass_attempts" in out.columns and out["pass_attempts"][0] == 30
    assert "interception" in out.columns
    assert "yards_after_catch" in out.columns
    assert out["season"][0] == 2025


def test_normalize_does_not_clobber_an_existing_canonical_column():
    df = pd.DataFrame([{"player_id": "a", "attempts": 30, "pass_attempts": 99}])
    out = normalize_weekly_stats(df, 2025)
    assert out["pass_attempts"][0] == 99


def test_normalize_handles_empty():
    assert normalize_weekly_stats(pd.DataFrame(), 2025).empty


# --------------------------------------------------------------------------
# Derived metrics
# --------------------------------------------------------------------------

def test_passer_rating_matches_known_values():
    # A perfect rating is 158.3; a completion-less game is 0, not a clamped 39.6.
    assert passer_rating([20], [20], [500], [10], [0])[0] == pytest.approx(158.3, abs=0.1)
    assert passer_rating([0], [0], [0], [0], [0])[0] == 0.0


def test_derive_metrics_computes_efficiency_without_dividing_by_zero():
    df = pd.DataFrame([
        {"player_id": "a", "receiving_yards": 100, "receptions": 8, "targets": 10,
         "receiving_air_yards": 120, "rushing_yards": 50, "rush_attempts": 10},
        {"player_id": "b", "receiving_yards": 0, "receptions": 0, "targets": 0,
         "receiving_air_yards": 0, "rushing_yards": 0, "rush_attempts": 0},
    ])
    out = derive_metrics(df)
    assert out["ypr"][0] == pytest.approx(12.5)
    assert out["ypc"][0] == pytest.approx(5.0)
    assert out["adot"][0] == pytest.approx(12.0)
    assert out["touches"][0] == 18
    # Zero denominators must produce 0, never inf or NaN.
    assert out.loc[1, ["ypr", "ypc", "adot", "touches"]].tolist() == [0, 0, 0, 0]
    assert np.isfinite(out[["ypr", "ypc", "adot"]].to_numpy()).all()


def test_derive_metrics_preserves_an_existing_passer_rating():
    df = pd.DataFrame([{"player_id": "a", "passer_rating": 101.5, "pass_attempts": 30}])
    assert derive_metrics(df)["passer_rating"][0] == pytest.approx(101.5)


# --------------------------------------------------------------------------
# Snaps and shares
# --------------------------------------------------------------------------

def test_attach_snap_counts_translates_pfr_ids():
    df = pd.DataFrame([_game("gsis1", 2025, 1)])
    snaps = pd.DataFrame([{"pfr_id": "PFR1", "season": 2025, "week": 1,
                           "offense_snaps": 55, "offense_pct": 0.9}])
    out = attach_snap_counts(df, snaps, {"PFR1": "gsis1"})
    assert out["offense_snaps"][0] == 55
    assert out["offense_pct"][0] == pytest.approx(0.9)


def test_attach_snap_counts_is_a_noop_without_a_usable_key():
    df = pd.DataFrame([_game("gsis1", 2025, 1)])
    out = attach_snap_counts(df, pd.DataFrame([{"nope": 1}]), {})
    assert out["offense_snaps"][0] == 0


def test_team_shares_sum_to_one_within_a_team_week():
    df = pd.DataFrame([
        _game("a", 2025, 1, targets=6, receptions=4, rush_attempts=0),
        _game("b", 2025, 1, targets=4, receptions=4, rush_attempts=0),
    ])
    out = add_team_shares(df)
    assert out["team_targets_share"].sum() == pytest.approx(1.0)
    assert out["team_targets_share"][0] == pytest.approx(0.6)
    # A team that ran zero times gets 0, not NaN.
    assert out["team_rush_attempts_share"].tolist() == [0.0, 0.0]


# --------------------------------------------------------------------------
# Lags across the season boundary -- the actual bug
# --------------------------------------------------------------------------

def test_lags_cross_the_season_boundary():
    df = pd.DataFrame([
        _game("a", 2025, 16, y_fantasy_points_ppr=10.0),
        _game("a", 2025, 17, y_fantasy_points_ppr=20.0),
        _game("a", 2026, 1, y_fantasy_points_ppr=0.0),   # the week being projected
    ])
    out = add_player_lags(df, ["y_fantasy_points_ppr"])
    wk1 = out[(out["season"] == 2026) & (out["week"] == 1)].iloc[0]
    # This is the whole fix: week 1 of the new season sees last season's tail.
    assert wk1["y_fantasy_points_ppr_lag_1"] == 20.0
    assert wk1["y_fantasy_points_ppr_lag_2"] == 10.0


def test_lags_do_not_leak_between_players():
    df = pd.DataFrame([
        _game("a", 2025, 17, y_fantasy_points_ppr=30.0),
        _game("b", 2026, 1, y_fantasy_points_ppr=0.0),
    ])
    out = add_player_lags(df, ["y_fantasy_points_ppr"])
    b = out[out["player_id"] == "b"].iloc[0]
    assert b["y_fantasy_points_ppr_lag_1"] == 0.0


def test_lags_create_missing_columns_as_zero():
    df = pd.DataFrame([_game("a", 2026, 1)])
    out = add_player_lags(df, ["targets_redzone"])
    assert out["targets_redzone_lag_1"][0] == 0.0


def test_season_average_carries_across_seasons():
    df = pd.DataFrame([
        _game("a", 2025, 16, y_fantasy_points_ppr=10.0),
        _game("a", 2025, 17, y_fantasy_points_ppr=20.0),
        _game("a", 2026, 1, y_fantasy_points_ppr=0.0),
    ])
    out = add_season_average(df)
    wk1 = out[(out["season"] == 2026) & (out["week"] == 1)].iloc[0]
    # Mean of prior games only; the unplayed week never counts itself.
    assert wk1["player_season_avg_points"] == pytest.approx(15.0)


# --------------------------------------------------------------------------
# Row universe
# --------------------------------------------------------------------------

def test_build_upcoming_rows_zeroes_production_and_stamps_the_week():
    players = pd.DataFrame([{"player_id": "a", "player_name": "A", "position": "QB",
                             "team": "SF", "opponent_team": "LA"}])
    out = build_upcoming_rows(players, 2026, 1)
    assert out["season"][0] == 2026 and out["week"][0] == 1
    assert out["y_fantasy_points_ppr"][0] == 0.0
    assert out["targets"][0] == 0.0


def test_build_upcoming_rows_handles_empty():
    assert build_upcoming_rows(pd.DataFrame(), 2026, 1).empty


def test_unplayed_player_still_gets_real_lags():
    """End-to-end: the placeholder row is what carries history into week 1."""
    history = pd.DataFrame([
        _game("a", 2025, 16, y_fantasy_points_ppr=18.0, targets=9),
        _game("a", 2025, 17, y_fantasy_points_ppr=22.0, targets=11),
    ])
    upcoming = build_upcoming_rows(
        pd.DataFrame([{"player_id": "a", "player_name": "A", "position": "WR",
                       "team": "SF", "opponent_team": "LA"}]), 2026, 1)
    combined = pd.concat([history, upcoming], ignore_index=True, sort=False)
    out = add_player_lags(combined, ["y_fantasy_points_ppr", "targets"])
    wk1 = out[(out["season"] == 2026) & (out["week"] == 1)].iloc[0]
    assert wk1["y_fantasy_points_ppr_lag_1"] == 22.0
    assert wk1["targets_lag_1"] == 11.0
    assert wk1["targets_lag_2"] == 9.0
