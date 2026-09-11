"""A profile roster status must not outrank the live depth chart.

Aaron Donald carries `status = EXE` while the depth chart -- pulled the same
morning -- lists him at pos_rank 1 for LA twice (NT on 09-01, RDE on 09-11). He
is playing; the status field is what lags. Filtering on status alone would have
deleted a legitimate starter from the injury report and from the starter set.

Same shape as the roster-override bug: an undated field outranking a dated feed.
"""

import polars as pl
import pytest

from applications.api.services import data_loader as dl
from applications.api.state import model_data


@pytest.fixture(autouse=True)
def _restore():
    saved = {k: model_data.get(k) for k in ("df_profile", "df_depth_charts")}
    yield
    for k, v in saved.items():
        if v is None:
            model_data.pop(k, None)
        else:
            model_data[k] = v


def _profile(rows):
    model_data["df_profile"] = pl.DataFrame(rows)


def _depth(ids):
    model_data["df_depth_charts"] = pl.DataFrame(
        [{"gsis_id": i, "team": "LA", "pos_rank": 1, "dt": "2026-09-11T12:21:50Z"} for i in ids]
    )


def test_stale_status_does_not_remove_a_player_the_feed_still_lists():
    _profile([{"player_id": "ad", "player_name": "Aaron Donald", "status": "EXE"}])
    _depth(["ad"])
    # The exact regression: EXE plus an active depth-chart entry means playing.
    assert dl.departed_player_ids() == set()


def test_player_absent_from_the_feed_is_treated_as_gone():
    _profile([{"player_id": "gone", "player_name": "Cut Guy", "status": "CUT"}])
    _depth(["someone_else"])
    assert dl.departed_player_ids() == {"gone"}


def test_active_players_are_never_flagged():
    _profile([
        {"player_id": "a", "player_name": "Starter", "status": "ACT"},
        {"player_id": "b", "player_name": "Practice", "status": "DEV"},
    ])
    _depth([])
    assert dl.departed_player_ids() == set()


def test_injured_reserve_is_kept():
    # RES and INA are on the roster and hurt -- exactly what a report is for.
    _profile([
        {"player_id": "ir", "player_name": "IR Guy", "status": "RES"},
        {"player_id": "ina", "player_name": "Inactive Guy", "status": "INA"},
    ])
    _depth([])
    assert dl.departed_player_ids() == set()


def test_no_feed_means_nobody_is_hidden():
    """With nothing to corroborate, trust no one over a field known to lag."""
    _profile([{"player_id": "gone", "player_name": "Cut Guy", "status": "CUT"}])
    model_data["df_depth_charts"] = pl.DataFrame()
    assert dl.departed_player_ids() == set()


def test_missing_columns_degrade_safely():
    model_data["df_profile"] = pl.DataFrame([{"player_id": "a"}])
    _depth([])
    assert dl.departed_player_ids() == set()
    model_data["df_profile"] = pl.DataFrame()
    assert dl.departed_player_ids() == set()


def test_starters_are_not_filtered_by_status(client):
    """All five pos_rank-1 players with an off-roster status are on the depth
    chart, so none of them should be dropped from the starter set."""
    starters = model_data.get("starter_gsis_ids") or set()
    if not starters:
        pytest.skip("depth charts not loaded in this environment")
    departed = dl.departed_player_ids()
    assert starters.isdisjoint(departed), (
        "a starter came from the live depth chart; it must not be filtered by a stale status"
    )
