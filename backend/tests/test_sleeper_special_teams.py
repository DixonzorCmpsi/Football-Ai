"""Kickers and team defenses carry Sleeper's projection and get no start/sit call.

We don't model them. Before this, a kicker became a 0.0 card and, if he was in
the saved lineup, was listed under "Sit" because the model's lineup never picks
kickers.
"""

import asyncio

import polars as pl
import pytest

from applications.api.services import sleeper_league as sl
from applications.api.state import model_data


def _proj(sleeper_id, position, team, pts, first="", last=""):
    return {
        "player_id": sleeper_id, "team": team, "opponent": "SEA",
        "stats": {"pts_ppr": pts, "pts_half_ppr": pts, "pts_std": pts},
        "player": {"position": position, "first_name": first, "last_name": last, "team": team},
    }


@pytest.fixture
def league(monkeypatch):
    rosters = {"players": [], "starters": []}

    def fake_get(path, ttl=None):
        if path.endswith("/rosters"):
            return [{"roster_id": 1, **rosters}]
        if path.startswith("/league/"):
            return {"league_id": "L1", "season": "2026", "scoring_settings": {"rec": 1},
                    "roster_positions": ["QB", "K", "DEF", "BN"]}
        return None

    projections = {
        "12713": _proj("12713", "K", "NE", 6.1, "Andy", "Borregales"),
        "SEA": _proj("SEA", "DEF", "SEA", 8.81, "Seattle", "Seahawks"),
    }
    monkeypatch.setattr(sl, "_get", fake_get)
    monkeypatch.setattr(sl, "external_projections", lambda season, week: projections)
    for key in ("sleeper_map", "df_profile"):
        monkeypatch.setitem(model_data, key, model_data.get(key))
    model_data["sleeper_map"] = {"777": "00-kicker"}
    model_data["df_profile"] = pl.DataFrame([{"player_id": "00-kicker", "player_name": "Bye Week Kicker", "position": "K"}])
    return rosters


def test_kicker_and_defense_carry_sleepers_projection(league):
    league["players"] = ["12713", "SEA"]
    league["starters"] = ["12713", "SEA"]
    result = asyncio.run(sl.analyze_roster("L1", 1, 1))
    special = {c["position"]: c for c in result["special_teams"]}
    assert special["K"]["prediction"] == 6.1 and special["K"]["player_name"] == "Andy Borregales"
    assert special["DEF"]["prediction"] == 8.81 and special["DEF"]["team"] == "SEA"
    assert all(c["projection_source"] == "sleeper" for c in result["special_teams"])
    assert result["special_teams_projected_total"] == pytest.approx(14.91)


def test_they_are_never_recommended_or_told_to_sit(league):
    league["players"] = ["12713", "SEA", "777"]
    league["starters"] = ["12713", "SEA", "777"]
    result = asyncio.run(sl.analyze_roster("L1", 1, 1))
    assert result["players"] == [] and result["recommended_starters"] == []
    assert result["start_but_should_sit"] == []
    assert result["unmatched_sleeper_ids"] == [], "a kicker on bye is still a kicker, not an unknown player"
    bye = next(c for c in result["special_teams"] if c["sleeper_id"] == "777")
    assert bye["prediction"] is None and bye["position"] == "K"


def test_projections_follow_league_scoring(monkeypatch):
    row = {"player_id": "1", "stats": {"pts_ppr": 9.0, "pts_std": 7.0}, "player": {"position": "DEF"}}
    assert sl._external_card("1", row, "Standard", False, None)["prediction"] == 7.0
    assert sl._external_card("1", row, "PPR", False, None)["prediction"] == 9.0
