"""A finished game's player card carries the full-PPR points the player scored."""

import polars as pl
import pytest

from applications.api.services import prediction
from applications.api.state import model_data


@pytest.fixture
def week_one(monkeypatch):
    saved = {k: model_data.get(k) for k in ("df_schedule", "df_player_stats")}
    model_data["df_schedule"] = pl.DataFrame({
        "season": [2026, 2026, 2026], "week": [1, 1, 2],
        "home_team": ["CIN", "SEA", "CIN"], "away_team": ["TB", "NE", "BAL"],
        "home_score": [33, None, None], "away_score": [27, None, None],
    }, schema_overrides={"home_score": pl.Int64, "away_score": pl.Int64})
    model_data["df_player_stats"] = pl.DataFrame({
        "player_id": ["chase", "irving"], "week": [1, 1],
        "y_fantasy_points_ppr": [24.6, None],
        "receptions": [0, 3], "receiving_yards": [0, 20], "rushing_yards": [0, 71], "rush_touchdown": [0, 1],
    })
    yield
    model_data.update(saved)


def test_final_game_with_a_stat_line_uses_its_ppr_points(week_one):
    assert prediction.actual_result("chase", "CIN", 1) == (24.6, True)


def test_points_are_computed_as_full_ppr_when_the_column_is_missing(week_one):
    # 3 receptions + 2.0 receiving + 7.1 rushing + 6 for the touchdown
    assert prediction.actual_result("irving", "TB", 1) == (18.1, True)


def test_final_game_without_a_stat_line_is_zero_not_blank(week_one):
    assert prediction.actual_result("inactive", "TB", 1) == (0.0, True)


def test_prop_actuals_line_up_with_the_card_labels(week_one):
    stats = prediction.actual_stats("irving", 1, True)
    assert stats["Rush Yds"] == 71 and stats["Receptions"] == 3 and stats["Rec Yds"] == 20 and stats["TDs"] == 1
    assert stats["Pass Yds"] is None, "a column the stat line doesn't carry stays unknown"
    assert prediction.actual_stats("inactive", 1, True)["Rush Yds"] == 0
    assert prediction.actual_stats("chase", 2, False) is None


def test_unplayed_game_has_no_actual(week_one):
    assert prediction.actual_result("chase", "CIN", 2) == (None, False)
    assert prediction.actual_result("somebody", "SEA", 1) == (None, False)
