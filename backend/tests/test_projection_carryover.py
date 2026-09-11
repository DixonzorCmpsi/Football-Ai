"""Early-season projections must carry over prior-season form.

`df_player_stats` only ever holds the current season, and the projection
baseline read from it alone -- so in week 1 every player started from 0.0 and the
projection collapsed to the model's deviation term. Brock Purdy, coming off a
16-18 pt/game season, projected 7.1.
"""

import polars as pl
import pytest

from applications.api.services import prediction as pr
from applications.api.state import model_data


@pytest.fixture
def history():
    saved = model_data.get("df_player_stats_history")
    yield lambda df: model_data.__setitem__("df_player_stats_history", df)
    if saved is None:
        model_data.pop("df_player_stats_history", None)
    else:
        model_data["df_player_stats_history"] = saved


def _week(player_id, season, week, passing_yards=0.0, passing_touchdown=0.0):
    return {
        "player_id": player_id, "season": season, "week": week,
        "passing_yards": passing_yards, "passing_touchdown": passing_touchdown,
        "rushing_yards": 0.0, "rushing_touchdown": 0.0,
        "receiving_yards": 0.0, "receiving_touchdown": 0.0, "receptions": 0.0,
        "interceptions": 0.0, "fumbles_lost": 0.0,
    }


def test_prior_season_form_is_zero_without_history(history):
    history(pl.DataFrame())
    assert pr.prior_season_form("00-0000001") == (0.0, 0)


def test_prior_season_form_is_zero_for_unknown_player(history):
    history(pl.DataFrame([_week("someone-else", 2025, 1, 300, 3)]))
    # A rookie with no prior production gets 0 -- the honest answer, not a guess.
    assert pr.prior_season_form("00-0000001") == (0.0, 0)


def test_prior_season_form_uses_most_recent_games(history):
    pid = "00-0000001"
    rows = [_week(pid, 2024, w, 100, 0) for w in range(1, 6)]          # older, low
    rows += [_week(pid, 2025, w, 300, 3) for w in range(1, 6)]          # recent, high
    history(pl.DataFrame(rows))
    avg, games = pr.prior_season_form(pid, max_games=4)
    assert games == 4
    # 300 yds + 3 TD scores far above a 100-yard game; the recent season wins.
    assert avg == pytest.approx(pr.calculate_fantasy_points(_week(pid, 2025, 1, 300, 3)))


def test_prior_season_form_falls_through_a_missed_season(history):
    pid = "00-0000001"
    # Played 2024, missed 2025 entirely: fall back further rather than to zero.
    history(pl.DataFrame([_week(pid, 2024, w, 250, 2) for w in range(1, 5)]))
    avg, games = pr.prior_season_form(pid)
    assert games == 4 and avg > 0


def test_prior_season_form_ignores_zero_games(history):
    pid = "00-0000001"
    rows = [_week(pid, 2025, 1, 0, 0), _week(pid, 2025, 2, 0, 0),
            _week(pid, 2025, 3, 300, 3)]
    history(pl.DataFrame(rows))
    avg, games = pr.prior_season_form(pid)
    assert games == 1
    assert avg == pytest.approx(pr.calculate_fantasy_points(_week(pid, 2025, 3, 300, 3)))


def test_blend_is_all_prior_at_week_one(history):
    pid = "00-0000001"
    history(pl.DataFrame([_week(pid, 2025, w, 300, 3) for w in range(1, 5)]))
    prior, _ = pr.prior_season_form(pid)
    # No current-season games yet: the baseline is entirely last season.
    assert pr.blend_with_prior(0.0, 0, pid) == pytest.approx(prior)


def test_blend_ignores_prior_once_the_season_is_established(history):
    pid = "00-0000001"
    history(pl.DataFrame([_week(pid, 2025, w, 300, 3) for w in range(1, 5)]))
    # Four current-season games: prior season no longer contributes at all.
    assert pr.blend_with_prior(9.0, 4, pid) == pytest.approx(9.0)
    assert pr.blend_with_prior(9.0, 9, pid) == pytest.approx(9.0)


def test_blend_crossfades_linearly(history):
    pid = "00-0000001"
    history(pl.DataFrame([_week(pid, 2025, w, 300, 3) for w in range(1, 5)]))
    prior, _ = pr.prior_season_form(pid)
    # Two of four games in: an even split, so a projection never jumps.
    assert pr.blend_with_prior(10.0, 2, pid) == pytest.approx(0.5 * 10.0 + 0.5 * prior)
    assert pr.blend_with_prior(10.0, 1, pid) == pytest.approx(0.25 * 10.0 + 0.75 * prior)


def test_blend_without_history_returns_current(history):
    history(pl.DataFrame())
    assert pr.blend_with_prior(6.5, 0, "00-0000001") == pytest.approx(6.5)


def test_purdy_projects_like_a_starting_qb(client):
    """Regression for the reported symptom, against real loaded data."""
    hist = model_data.get("df_player_stats_history")
    if hist is None or hist.is_empty():
        pytest.skip("historical stats not loaded in this environment")
    avg, games = pr.prior_season_form("00-0037834")
    if games == 0:
        pytest.skip("no prior-season rows for Purdy in this environment")
    # He averaged 16-18 pts/game last season; the baseline must reflect that
    # rather than the 0.0 that produced a 7.1 projection.
    assert avg > 10.0
    assert pr.blend_with_prior(0.0, 0, "00-0037834") > 10.0
