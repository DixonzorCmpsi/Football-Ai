"""FanDuel board parsing: the same rows the Bovada parser produces, from a datacenter-friendly source.

Bovada answers an empty board to cloud servers, so FanDuel is the primary source. These pin the
shapes the parser reads (taken from the real board) so a format change fails here, not silently
in the app.
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rag_data"))

import fanduel_api_client as fd  # noqa: E402


def _runner(name, odds, handicap=0, result=None, player=False, status="ACTIVE"):
    r = {
        "runnerName": name, "handicap": handicap, "runnerStatus": status, "isPlayerSelection": player,
        "winRunnerOdds": {"americanDisplayOdds": {"americanOdds": odds}},
    }
    if result:
        r["result"] = {"type": result}
    return r


def _market(mtype, runners, status="OPEN", in_play=False):
    return {"marketType": mtype, "marketStatus": status, "inPlay": in_play, "runners": runners}


GAME = {
    "1": _market("MONEY_LINE", [_runner("Seattle Seahawks", -158, result="AWAY"), _runner("Denver Broncos", 134, result="HOME")]),
    "2": _market("MATCH_HANDICAP_(2-WAY)", [_runner("Seattle Seahawks", -105, -3, "AWAY"), _runner("Denver Broncos", -115, 3, "HOME")]),
    "3": _market("TOTAL_POINTS_(OVER/UNDER)", [_runner("Over", -102, 41.5, "OVER"), _runner("Under", -120, 41.5, "UNDER")]),
}


def test_game_lines_use_the_home_and_away_flags_not_the_order():
    row = fd.parse_game_lines(GAME, "Seattle Seahawks @ Denver Broncos", 2026, 6, "2026_06_SEA_DEN")
    assert (row["home_team"], row["away_team"]) == ("DEN", "SEA")
    assert (row["home_ml"], row["away_ml"]) == ("+134", "-158")
    # A favourite carries a minus sign: SEA (away) -3, DEN (home) +3.
    assert (row["away_spread"], row["home_spread"]) == ("-3.0", "+3.0")
    assert (row["total_over"], row["total_under"]) == (41.5, 41.5)
    assert row["total_over_prob"] == 50.5  # -102 -> 102/202


def test_a_game_with_no_lines_is_none():
    assert fd.parse_game_lines({}, "Seattle Seahawks @ Denver Broncos", 2026, 6, "x") is None


def test_started_markets_are_ignored():
    live = {"1": _market("MONEY_LINE", [_runner("Seattle Seahawks", -158, result="AWAY")], in_play=True)}
    assert fd.parse_game_lines(live, "Seattle Seahawks @ Denver Broncos", 2026, 6, "x") is None


def test_player_over_under_and_scorer_markets():
    markets = {
        "a": _market("PLAYER_X_RECEIVING_YARDS_MEDIUM", [
            _runner("Troy Franklin Over", -114, 16.5, "OVER", True), _runner("Troy Franklin Under", -114, 16.5, "UNDER", True)]),
        "b": _market("PLAYER_X_RUSHING_+_RECEIVING_YARDS", [
            _runner("RJ Harvey Over", -114, 38.5, "OVER", True), _runner("RJ Harvey Under", -114, 38.5, "UNDER", True)]),
        "c": _market("ANY_TIME_TOUCHDOWN_SCORER", [_runner("Jaxon Smith-Njigba", 105, player=True), _runner("Denver Broncos D/ST", 900, player=True)]),
        # Quarter and drive markets are other wagers.
        "d": _market("PLAYER_X_1ST_QUARTER_RECEIVING_YARDS", [_runner("Troy Franklin Over", -110, 5.5, "OVER", True)]),
        "e": _market("HOME_DRIVE_X_RECEIVING_YDS_-_PLAYER_Y", [_runner("Troy Franklin Over", -110, 5.5, "OVER", True)]),
    }
    rows = fd.parse_player_props(markets, 2026, 6, "2026_06_SEA_DEN")
    got = {(r["player_name"], r["prop_type"], r["side"], r["line"], r["odds"]) for r in rows}
    assert got == {
        ("Troy Franklin", "Receiving Yards", "over", 16.5, "-114"),
        ("Troy Franklin", "Receiving Yards", "under", 16.5, "-114"),
        ("RJ Harvey", "Rushing & Receiving Yards", "over", 38.5, "-114"),
        ("RJ Harvey", "Rushing & Receiving Yards", "under", 38.5, "-114"),
        ("Jaxon Smith-Njigba", "Anytime TD", "Yes", 1.0, "+105"),
    }
    assert all(r["implied_prob"] is not None for r in rows)


def test_alternate_ladders_become_overs_on_the_half_point():
    markets = {"a": _market("PLAYER_X_ALT_RECEIVING_YARDS_MEDIUM", [
        _runner("RJ Harvey 20+ Yards", -128, player=True), _runner("RJ Harvey 25+ Yards", 110, player=True)])}
    rows = fd.parse_player_props(markets, 2026, 6, "g")
    assert [(r["prop_type"], r["line"], r["side"]) for r in rows] == [
        ("Alternate Receiving Yards", 19.5, "over"), ("Alternate Receiving Yards", 24.5, "over")]


def test_suspended_runners_and_markets_are_dropped():
    markets = {
        "a": _market("PLAYER_X_RECEPTIONS_MEDIUM", [_runner("Evan Engram Over", -158, 2.5, "OVER", True, status="SUSPENDED")]),
        "b": _market("PLAYER_X_RECEPTIONS_MEDIUM", [_runner("Evan Engram Over", -158, 2.5, "OVER", True)], status="SUSPENDED"),
    }
    assert fd.parse_player_props(markets, 2026, 6, "g") == []


def test_events_keep_only_upcoming_games():
    page = {"attachments": {"events": {
        "1": {"eventId": 1, "name": "Seattle Seahawks @ Denver Broncos", "openDate": "2026-10-16T00:16:00.000Z"},
        "2": {"eventId": 2, "name": "NFL Futures", "openDate": "2026-03-06T18:48:05.000Z"},
        "3": {"eventId": 3, "name": "Houston Texans @ Jacksonville Jaguars", "openDate": "2026-10-10T17:00:00.000Z"},
    }}}
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    assert [e["name"] for e in fd.parse_events(page, now)] == ["Seattle Seahawks @ Denver Broncos"]
    assert fd.split_event_name("Seattle Seahawks @ Denver Broncos") == ("DEN", "SEA")
