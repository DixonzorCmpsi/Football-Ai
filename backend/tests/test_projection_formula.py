"""How a projection is assembled: pinned to what the backtest chose.

model_training/backtest_projection_formula.py is the evidence. These tests keep
the pieces from drifting back toward the formula it replaced, which ran almost
four points high: a baseline that skipped zero-point games, plus a 5*ln(1+x)
multiplier on a deviation that already leaned positive.
"""

import polars as pl
import pytest

from applications.api.services import prediction as pr
from applications.api.state import model_data


@pytest.fixture
def history():
    saved = model_data.get("df_player_stats_history")
    saved_index = model_data.pop("_prior_form_index", None)

    def set_rows(rows):
        model_data.pop("_prior_form_index", None)
        model_data["df_player_stats_history"] = pl.DataFrame(rows)

    yield set_rows
    model_data.pop("_prior_form_index", None)
    if saved_index is not None:
        model_data["_prior_form_index"] = saved_index
    if saved is None:
        model_data.pop("df_player_stats_history", None)
    else:
        model_data["df_player_stats_history"] = saved


def _game(pid, season, week, pts):
    return {"player_id": pid, "season": season, "week": week, "y_fantasy_points_ppr": float(pts)}


def test_the_model_adjustment_is_not_amplified():
    """The old 5*ln(1+x) turned a +1.4 deviation into +4.4 points."""
    baseline, adjustment = pr.combine_projection(10.0, 10.0, 1.4, "WR", has_history=True)
    assert adjustment == pytest.approx(1.4 - pr.DEVIATION_CENTER["WR"])
    assert baseline + adjustment < 11.0


def test_the_baseline_leans_on_the_full_season_average():
    baseline, _ = pr.combine_projection(season_avg=9.0, recent_form=17.0, deviation=0.0,
                                        pos="WR", has_history=True)
    assert baseline == pytest.approx(0.75 * 9.0 + 0.25 * 17.0)


def test_a_player_with_no_history_keeps_the_model_signal():
    """Nothing was measured for rookies; centering would leave them at ~0."""
    baseline, adjustment = pr.combine_projection(0.0, 0.0, 1.4, "WR", has_history=False)
    assert baseline == 0.0
    assert adjustment > 1.4


def test_zero_point_games_count_in_the_prior_season_average(history):
    history([_game("p", 2025, w, pts) for w, pts in [(1, 10), (2, 0), (3, 0), (4, 10)]])
    avg, games = pr.prior_season_average("p")
    assert (avg, games) == (pytest.approx(5.0), 4)
    form, _ = pr.prior_season_form("p")
    assert form == pytest.approx(10.0), "recent form still skips zeros; only the average counts them"


def test_the_prior_season_average_uses_only_the_latest_season(history):
    history([_game("p", 2024, 1, 30), _game("p", 2025, 1, 6), _game("p", 2025, 2, 8)])
    assert pr.prior_season_average("p") == (pytest.approx(7.0), 2)


def test_season_average_crossfades_from_last_season(history):
    history([_game("p", 2025, w, 12) for w in range(1, 9)])
    assert pr.season_average_with_prior([], "p") == pytest.approx(12.0)
    assert pr.season_average_with_prior([4.0, 4.0], "p") == pytest.approx(0.5 * 4.0 + 0.5 * 12.0)
    assert pr.season_average_with_prior([4.0] * 4, "p") == pytest.approx(4.0)


def test_a_hot_finish_no_longer_sets_week_one(history):
    """Jalen Coker, 2026 week 1: a 28-point playoff game and a 17 had him at 18.5.

    Market lines implied about 9. His 2025 games, most recent first.
    """
    games = [28.4, 16.7, 3.6, 7.7, 14.0, 17.4, 6.2, 11.2, 5.1, 1.9, 6.6, 0.0]
    history([_game("coker", 2025, 19 - i, pts) for i, pts in enumerate(games)])
    form, _ = pr.prior_season_form("coker")
    season_avg = pr.season_average_with_prior([], "coker")
    old = form + 5.0 * __import__("math").log1p(1.43)
    baseline, adjustment = pr.combine_projection(season_avg, form, 1.43, "WR", has_history=True)
    assert old > 18
    assert baseline + adjustment < 12.5
