"""Player search: skill players by default, everyone on request, best matches first."""

import asyncio

import polars as pl
import pytest

from applications.api.routes import general
from applications.api.state import model_data


@pytest.fixture
def profiles(monkeypatch):
    monkeypatch.setitem(model_data, "df_profile", pl.DataFrame([
        {"player_id": "1", "player_name": "Josh Allen", "position": "QB", "team_abbr": "BUF", "headshot": "", "status": "ACT"},
        {"player_id": "2", "player_name": "Josh Allen", "position": "LB", "team_abbr": "JAX", "headshot": "", "status": "ACT"},
        {"player_id": "3", "player_name": "Braelon Allen", "position": "RB", "team_abbr": "NYJ", "headshot": "", "status": "ACT"},
        {"player_id": "4", "player_name": "Josh Conerly Jr.", "position": "OL", "team_abbr": "WAS", "headshot": "", "status": "ACT"},
        {"player_id": "5", "player_name": "Allen Lazard", "position": "WR", "team_abbr": "NYJ", "headshot": "", "status": "RET"},
        {"player_id": "6", "player_name": "Keenan Allen", "position": "WR", "team_abbr": "LAC", "headshot": "", "status": "ACT"},
    ]))


def search(q, **kw):
    return asyncio.run(general.search_players(q, **kw))


def test_default_scope_is_players_we_project(profiles):
    assert search("conerly") == []
    assert {p["position"] for p in search("allen")} <= {"QB", "RB", "WR", "TE"}


def test_scope_all_reaches_linemen_and_defenders(profiles):
    assert [p["player_name"] for p in search("conerly", scope="all")] == ["Josh Conerly Jr."]
    assert {p["position"] for p in search("josh allen", scope="all")} == {"QB", "LB"}


def test_exact_and_prefix_matches_rank_first_and_active_before_retired(profiles):
    names = [p["player_name"] for p in search("allen")]
    # "Allen Lazard" starts with the query but is retired; surname matches that are active come first.
    assert names.index("Braelon Allen") < names.index("Allen Lazard")
    assert search("josh allen")[0]["player_id"] == "1"


def test_limit_is_honoured_and_bounded(profiles):
    assert len(search("allen", scope="all", limit=2)) == 2
    assert len(search("a", scope="all", limit=10_000)) <= 100


def test_initials_and_a_dropped_letter_still_find_the_player(monkeypatch):
    """"AD Mitchell" is how he's known and how a model will ask; the profile says Adonai."""
    monkeypatch.setitem(model_data, "df_profile", pl.DataFrame([
        {"player_id": "1", "player_name": "Adonai Mitchell", "position": "WR", "team_abbr": "NYJ", "headshot": "", "status": "ACT"},
        {"player_id": "2", "player_name": "Keaton Mitchell", "position": "RB", "team_abbr": "LAC", "headshot": "", "status": "ACT"},
        {"player_id": "3", "player_name": "C.J. Stroud", "position": "QB", "team_abbr": "HOU", "headshot": "", "status": "ACT"},
    ]))
    for q in ("AD Mitchell", "A.D. Mitchell", "ad mitchel"):
        assert [p["player_name"] for p in search(q)] == ["Adonai Mitchell"], q
    assert [p["player_name"] for p in search("CJ Stroud")] == ["C.J. Stroud"]
    # A plain substring hit never falls through to the loose rule.
    assert {p["player_name"] for p in search("mitchell")} == {"Adonai Mitchell", "Keaton Mitchell"}
    assert search("zz mitchell") == []
