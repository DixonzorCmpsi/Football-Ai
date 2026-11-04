"""The game log's over/under must agree with the Vegas tab.

The Vegas tab reads df_lines. The game log once fell back to a separate nflverse lookup,
so it showed 52.5 for a game the Vegas tab called "no line posted". For the current
season the log now reads df_lines and nothing else; a game with no line is None.
"""

import polars as pl

from applications.api.config import CURRENT_SEASON
from applications.api.routes import players
from applications.api.services.data_loader import enrich_schedule_with_lines
from applications.api.state import model_data


def _lines(rows):
    return pl.DataFrame(rows, schema={"week": pl.Int64, "home_team": pl.Utf8, "away_team": pl.Utf8, "total_over": pl.Float64})


def test_current_season_total_comes_from_the_lines(monkeypatch):
    monkeypatch.setitem(model_data, "df_lines", _lines([(9, "ATL", "CIN", 52.5)]))
    season = int(CURRENT_SEASON)
    assert players._game_total(season, 9, "ATL") == 52.5
    assert players._game_total(season, 9, "CIN") == 52.5


def test_current_season_without_a_line_is_none_not_a_guess(monkeypatch):
    monkeypatch.setitem(model_data, "df_lines", _lines([(9, "ATL", "CIN", 52.5)]))
    # Another game, another week: no line in df_lines means None, never a schedule lookup.
    assert players._game_total(int(CURRENT_SEASON), 10, "ATL") is None
    monkeypatch.setitem(model_data, "df_lines", pl.DataFrame())
    assert players._game_total(int(CURRENT_SEASON), 9, "ATL") is None


def test_prior_season_total_uses_the_schedule(monkeypatch):
    monkeypatch.setitem(players._SCHEDULE_DATES, 1999, {(3, "ATL"): "1999-09-20"})
    monkeypatch.setitem(players._SCHEDULE_TOTALS, 1999, {(3, "ATL"): 43.5})
    assert players._game_total(1999, 3, "ATL") == 43.5
    assert players._game_total(1999, 4, "ATL") is None


def test_schedule_without_lines_is_enriched_from_nflverse(monkeypatch):
    import nflreadpy

    sched = pl.DataFrame({"game_id": ["g1", "g2"], "week": [9, 9]})
    nflverse = pl.DataFrame({"game_id": ["g1"], "total_line": [52.5], "spread_line": [-3.0]})
    monkeypatch.setattr(nflreadpy, "load_schedules", lambda seasons: nflverse)
    out = enrich_schedule_with_lines(sched)
    assert out.filter(pl.col("game_id") == "g1")["total_line"].to_list() == [52.5]
    assert out.filter(pl.col("game_id") == "g2")["total_line"].to_list() == [None]


def test_schedule_that_already_has_lines_is_left_alone(monkeypatch):
    import nflreadpy

    def boom(seasons):
        raise AssertionError("nflverse must not be called when the columns exist")

    monkeypatch.setattr(nflreadpy, "load_schedules", boom)
    sched = pl.DataFrame({"game_id": ["g1"], "total_line": [40.0], "spread_line": [1.0]})
    assert enrich_schedule_with_lines(sched).equals(sched)


def _history_for(client, monkeypatch, stats, snaps):
    monkeypatch.setitem(model_data, "df_player_stats", stats)
    monkeypatch.setitem(model_data, "df_player_stats_history", pl.DataFrame())
    monkeypatch.setitem(model_data, "df_snap_counts", snaps)
    monkeypatch.setitem(model_data, "df_snap_counts_history", pl.DataFrame())
    monkeypatch.setitem(
        model_data, "df_profile",
        pl.DataFrame({"player_id": ["P1"], "player_name": ["Test WR"], "team_abbr": ["NYJ"], "pfr_id": ["TestWr00"]}),
    )
    monkeypatch.setattr(players, "_game_opponent", lambda season, week, team=None: "LV")
    res = client.get("/player/history/P1")
    assert res.status_code == 200
    return res.json()


def test_game_with_snaps_but_no_stat_row_still_shows_in_the_log(client, monkeypatch):
    """A WR who plays and is never targeted has snaps but no row in the stats table."""
    season = int(CURRENT_SEASON)
    stats = pl.DataFrame({"player_id": ["P1"], "season": [season], "week": [1], "team": ["NYJ"],
                          "opponent_team": ["TEN"], "receptions": [1], "targets": [1], "receiving_yards": [30]})
    snaps = pl.DataFrame({"pfr_id": ["TestWr00"] * 3, "season": [season] * 3, "week": [1, 2, 3],
                          "offense_snaps": [5, 50, 0], "offense_pct": [0.07, 0.78, 0.0]})
    rows = _history_for(client, monkeypatch, stats, snaps)
    by_week = {r["week"]: r for r in rows}
    assert set(by_week) == {1, 2}  # week 3 had zero snaps: he did not play
    assert by_week[2]["points"] == 0.0 and by_week[2]["snap_count"] == 50
    assert by_week[2]["team"] == "NYJ" and by_week[2]["opponent"] == "LV"
    assert by_week[1]["receptions"] == 1  # the real stat row is untouched


def test_player_with_snaps_and_no_stat_rows_at_all_gets_a_log(client, monkeypatch):
    season = int(CURRENT_SEASON)
    stats = pl.DataFrame(schema={"player_id": pl.Utf8, "season": pl.Int64, "week": pl.Int64})
    snaps = pl.DataFrame({"pfr_id": ["TestWr00"], "season": [season], "week": [4],
                          "offense_snaps": [20], "offense_pct": [0.3]})
    rows = _history_for(client, monkeypatch, stats, snaps)
    assert [r["week"] for r in rows] == [4]


def test_a_failed_schedule_fetch_is_not_cached(monkeypatch):
    """One nflverse hiccup used to blank every game until the server restarted."""
    import nflreadpy

    calls = {"n": 0}

    def flaky(seasons):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("network down")
        return pl.DataFrame({"week": [3], "gameday": ["1998-09-20"], "home_team": ["ATL"], "away_team": ["CAR"],
                             "total_line": [41.0]})

    for cache in (players._SCHEDULE_DATES, players._SCHEDULE_TOTALS, players._SCHEDULE_GAMES, players._SCHEDULE_FAILED):
        monkeypatch.setitem(cache, 1998, {}) if cache is not players._SCHEDULE_FAILED else cache.pop(1998, None)
    monkeypatch.setattr(nflreadpy, "load_schedules", flaky)
    monkeypatch.setattr(players, "_SCHEDULE_RETRY_SECONDS", 0)
    assert players._game_total(1998, 3, "ATL") is None  # first fetch failed
    assert players._game_total(1998, 3, "ATL") == 41.0  # the failure was not remembered


def test_current_season_schedule_comes_from_the_app_table(monkeypatch):
    sched = pl.DataFrame({"week": [9], "home_team": ["KC"], "away_team": ["NYJ"], "gameday": ["2026-11-08"]})
    monkeypatch.setitem(model_data, "df_schedule", sched)
    monkeypatch.setitem(model_data, "data_loaded_at", "t-bye-test")
    season = int(CURRENT_SEASON)
    assert players._game_opponent(season, 9, "NYJ") == "KC"
    assert players._game_date(season, 9, "NYJ") == "2026-11-08"


def _next_game(monkeypatch, schedule_rows, current_week, last_stat_week):
    import asyncio

    monkeypatch.setitem(model_data, "df_schedule", pl.DataFrame(
        schedule_rows, schema={"week": pl.Int64, "home_team": pl.Utf8, "away_team": pl.Utf8, "gameday": pl.Utf8}))
    monkeypatch.setitem(model_data, "data_loaded_at", f"t-{current_week}-{len(schedule_rows)}")
    monkeypatch.setitem(model_data, "current_nfl_week", current_week)
    monkeypatch.setitem(model_data, "df_lines", pl.DataFrame())
    monkeypatch.setitem(model_data, "df_player_stats", pl.DataFrame(
        {"player_id": ["P2"], "season": [int(CURRENT_SEASON)], "week": [last_stat_week], "team": ["ATL"]}))
    monkeypatch.setitem(model_data, "df_profile", pl.DataFrame({"player_id": ["P2"], "team_abbr": ["ATL"]}))
    return asyncio.run(players.get_player_next_game("P2"))


def test_next_game_is_a_bye_when_the_team_does_not_play_the_active_week(monkeypatch):
    sched = [(8, "ATL", "TB", "2026-11-01"), (10, "ATL", "CAR", "2026-11-15")]
    out = _next_game(monkeypatch, sched, current_week=9, last_stat_week=8)
    assert out["bye"] is True and out["week"] == 9 and out["next_week"] == 10
    assert out["opponent"] is None and out["over_under"] is None


def test_next_game_is_the_active_weeks_game_even_when_stats_lag(monkeypatch):
    sched = [(6, "ATL", "CHI", "2026-10-18"), (9, "ATL", "CIN", "2026-11-08")]
    out = _next_game(monkeypatch, sched, current_week=9, last_stat_week=5)
    assert out["bye"] is False and out["week"] == 9 and out["opponent"] == "CIN"
