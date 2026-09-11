"""Opponent context rebuilt across seasons.

The 17 opponent-derived model features (opp_def_*, opp_off_*, rolling_avg_*)
were read from `weekly_defense_stats_<SEASON>` / `weekly_offense_stats_<SEASON>`.
Those tables hold only the current season -- four rows each in week 1 -- so every
one of those features was 0 early in a season, exactly like the player lags.

Prior seasons have no such tables but are fully reconstructable: team yardage is
the sum of its players' box scores, points come from real final scores, and a
defense's sacks/interceptions are what the offense it faced gave up.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rag_data"))

from feature_history import (  # noqa: E402
    add_defense_vs_position,
    add_opponent_features,
    attach_game_points,
    mirror_to_defense,
    team_offense_from_players,
)


def _p(pid, team, opp, season, week, **kw):
    row = {"player_id": pid, "team": team, "opponent_team": opp,
           "season": season, "week": week, "passing_yards": 0.0, "rushing_yards": 0.0,
           "sacks_suffered": 0.0, "interception": 0.0, "position": "QB",
           "y_fantasy_points_ppr": 0.0}
    row.update(kw)
    return row


# --------------------------------------------------------------------------
# Team aggregates
# --------------------------------------------------------------------------

def test_team_offense_sums_player_box_scores():
    df = pd.DataFrame([
        _p("a", "SF", "LA", 2025, 1, passing_yards=300, sacks_suffered=2, interception=1),
        _p("b", "SF", "LA", 2025, 1, rushing_yards=120),
        _p("c", "LA", "SF", 2025, 1, passing_yards=210, rushing_yards=80),
    ])
    out = team_offense_from_players(df)
    sf = out[out["team"] == "SF"].iloc[0]
    assert sf["passing_yards"] == 300
    assert sf["rushing_yards"] == 120
    assert sf["total_yards"] == 420
    assert sf["sacks_suffered"] == 2
    assert sf["interceptions_thrown"] == 1
    assert sf["opponent_team"] == "LA"


def test_team_offense_handles_empty():
    assert team_offense_from_players(pd.DataFrame()).empty


def test_attach_game_points_uses_real_scores():
    teams = pd.DataFrame([
        {"team": "SF", "season": 2025, "week": 1, "opponent_team": "LA",
         "passing_yards": 300, "rushing_yards": 120, "total_yards": 420,
         "sacks_suffered": 0, "interceptions_thrown": 0},
        {"team": "LA", "season": 2025, "week": 1, "opponent_team": "SF",
         "passing_yards": 210, "rushing_yards": 80, "total_yards": 290,
         "sacks_suffered": 0, "interceptions_thrown": 0},
    ])
    sched = pd.DataFrame([{"season": 2025, "week": 1, "home_team": "SF",
                           "away_team": "LA", "home_score": 27, "away_score": 17}])
    out = attach_game_points(teams, sched)
    sf = out[out["team"] == "SF"].iloc[0]
    la = out[out["team"] == "LA"].iloc[0]
    assert sf["total_off_points"] == 27 and sf["points_allowed"] == 17
    assert la["total_off_points"] == 17 and la["points_allowed"] == 27


def test_attach_game_points_leaves_zero_when_no_score():
    """Points are not derivable from box scores, so an unplayed game stays 0."""
    teams = pd.DataFrame([{"team": "SF", "season": 2026, "week": 1, "opponent_team": "LA",
                           "passing_yards": 0, "rushing_yards": 0, "total_yards": 0,
                           "sacks_suffered": 0, "interceptions_thrown": 0}])
    sched = pd.DataFrame([{"season": 2026, "week": 1, "home_team": "SF",
                           "away_team": "LA", "home_score": None, "away_score": None}])
    out = attach_game_points(teams, sched)
    assert out["total_off_points"].iloc[0] == 0.0


def test_mirror_to_defense_inverts_the_matchup():
    teams = pd.DataFrame([
        {"team": "SF", "season": 2025, "week": 1, "opponent_team": "LA",
         "passing_yards": 300, "rushing_yards": 120, "sacks_suffered": 3,
         "interceptions_thrown": 2, "total_off_points": 27},
    ])
    out = mirror_to_defense(teams)
    la = out[out["team"] == "LA"].iloc[0]
    # What SF gained is what LA allowed; SF's sacks suffered are LA's sacks.
    assert la["passing_yards_allowed"] == 300
    assert la["rushing_yards_allowed"] == 120
    assert la["def_sacks"] == 3
    assert la["def_interceptions"] == 2
    assert la["points_allowed"] == 27


# --------------------------------------------------------------------------
# Rollups across the season boundary
# --------------------------------------------------------------------------

def _two_season_defense(include_upcoming=True):
    """Two played games, plus the upcoming week's (zeroed) row.

    A rollup is attached to the opponent's OWN row for the week being projected,
    so that row has to exist or the merge finds nothing. In the pipeline it does:
    the upcoming-week placeholders feed team_offense_from_players, which feeds
    mirror_to_defense. `include_upcoming=False` pins what happens without it.
    """
    rows = [
        {"team": "LA", "season": 2025, "week": 16, "points_allowed": 17,
         "passing_yards_allowed": 200, "rushing_yards_allowed": 90,
         "def_sacks": 2, "def_interceptions": 1},
        {"team": "LA", "season": 2025, "week": 17, "points_allowed": 31,
         "passing_yards_allowed": 320, "rushing_yards_allowed": 150,
         "def_sacks": 1, "def_interceptions": 0},
    ]
    if include_upcoming:
        rows.append({"team": "LA", "season": 2026, "week": 1, "points_allowed": 0,
                     "passing_yards_allowed": 0, "rushing_yards_allowed": 0,
                     "def_sacks": 0, "def_interceptions": 0})
    return pd.DataFrame(rows)


def test_rollup_needs_the_opponent_row_for_the_projected_week():
    players = pd.DataFrame([_p("a", "SF", "LA", 2026, 1)])
    out = add_opponent_features(players, pd.DataFrame(),
                                _two_season_defense(include_upcoming=False))
    # Without the upcoming-week team row there is nothing to join onto, and the
    # feature degrades to 0 rather than silently borrowing another week.
    assert out["opp_def_points_allowed_lag_1"].iloc[0] == 0.0


def test_opponent_defense_features_reach_into_last_season():
    players = pd.DataFrame([_p("a", "SF", "LA", 2026, 1)])
    out = add_opponent_features(players, pd.DataFrame(), _two_season_defense())
    row = out.iloc[0]
    # Week 1 of the new season sees the defense's most recent games.
    assert row["opp_def_points_allowed_lag_1"] == 31
    assert row["opp_def_points_allowed_lag_2"] == 17
    assert row["rolling_avg_points_allowed_4_weeks"] == pytest.approx(24.0)


def test_defensive_rollups_are_renamed_to_the_trained_names():
    players = pd.DataFrame([_p("a", "SF", "LA", 2026, 1)])
    out = add_opponent_features(players, pd.DataFrame(), _two_season_defense())
    # The models were trained on these names, not the raw column names.
    assert "rolling_avg_sack_4_weeks" in out.columns
    assert "rolling_avg_interception_4_weeks" in out.columns
    assert out["rolling_avg_sack_4_weeks"].iloc[0] == pytest.approx(1.5)


def test_opponent_offense_features_merge_by_opponent():
    offense = pd.DataFrame([
        {"team": "LA", "season": 2025, "week": 16, "passing_yards": 250,
         "rushing_yards": 100, "total_yards": 350, "total_off_points": 20},
        {"team": "LA", "season": 2025, "week": 17, "passing_yards": 310,
         "rushing_yards": 140, "total_yards": 450, "total_off_points": 34},
        # The upcoming week, as the placeholder rows produce it.
        {"team": "LA", "season": 2026, "week": 1, "passing_yards": 0,
         "rushing_yards": 0, "total_yards": 0, "total_off_points": 0},
    ])
    players = pd.DataFrame([_p("a", "SF", "LA", 2026, 1)])
    out = add_opponent_features(players, offense, pd.DataFrame())
    row = out.iloc[0]
    assert row["opp_off_total_yards_lag_1"] == 450
    assert row["opp_off_total_yards_lag_2"] == 350
    assert row["opp_off_rolling_total_yards_4_weeks"] == pytest.approx(400.0)


def test_opponent_features_are_a_noop_without_an_opponent_column():
    players = pd.DataFrame([{"player_id": "a", "week": 1}])
    out = add_opponent_features(players, pd.DataFrame(), _two_season_defense())
    assert "opp_def_points_allowed_lag_1" not in out.columns


def test_no_leakage_from_the_current_game():
    """A rollup must never include the game being projected."""
    defense = pd.DataFrame([
        {"team": "LA", "season": 2026, "week": 1, "points_allowed": 99,
         "passing_yards_allowed": 999, "rushing_yards_allowed": 999,
         "def_sacks": 9, "def_interceptions": 9},
    ])
    players = pd.DataFrame([_p("a", "SF", "LA", 2026, 1)])
    out = add_opponent_features(players, pd.DataFrame(), defense)
    assert out["opp_def_points_allowed_lag_1"].iloc[0] == 0.0
    assert out["rolling_avg_points_allowed_4_weeks"].iloc[0] == 0.0


# --------------------------------------------------------------------------
# Defense vs position
# --------------------------------------------------------------------------

def test_dvp_carries_across_seasons():
    df = pd.DataFrame([
        _p("a", "SF", "LA", 2025, 16, position="WR", y_fantasy_points_ppr=20.0),
        _p("b", "SF", "LA", 2025, 17, position="WR", y_fantasy_points_ppr=30.0),
        _p("c", "SF", "LA", 2026, 1, position="WR", y_fantasy_points_ppr=0.0),
    ])
    out = add_defense_vs_position(df)
    wk1 = out[(out["season"] == 2026) & (out["week"] == 1)].iloc[0]
    assert wk1["rolling_avg_points_allowed_to_WR"] == pytest.approx(25.0)


def test_dvp_is_a_noop_when_columns_are_missing():
    df = pd.DataFrame([{"player_id": "a", "week": 1}])
    out = add_defense_vs_position(df)
    assert "rolling_avg_points_allowed_to_WR" not in out.columns
