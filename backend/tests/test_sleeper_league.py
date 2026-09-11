"""Sleeper league import, lineup recommendation and waiver ranking.

The live end-to-end path needs a real Sleeper account with leagues, so the
network layer is stubbed here and the analysis runs against the app's actual
model data and player id maps.
"""

import asyncio

import pytest

from applications.api.services import sleeper_league as sl
from applications.api.state import model_data


@pytest.fixture(autouse=True)
def _clear_cache():
    sl._CACHE.clear()
    yield
    sl._CACHE.clear()


def _card(sleeper_id, pos, proj, in_lineup=False):
    return {"sleeper_id": str(sleeper_id), "position": pos,
            "prediction": proj, "in_saved_lineup": in_lineup}


# --------------------------------------------------------------------------
# Pure logic
# --------------------------------------------------------------------------

def test_scoring_label():
    assert sl._scoring_label({"rec": 1}) == "PPR"
    assert sl._scoring_label({"rec": 0.5}) == "Half PPR"
    assert sl._scoring_label({"rec": 0}) == "Standard"
    assert sl._scoring_label({}) == "Standard"


def test_recommend_lineup_fills_fixed_slots_then_flex():
    ranked = [
        _card(1, "QB", 22.0), _card(2, "RB", 18.0), _card(3, "RB", 15.0),
        _card(4, "WR", 14.0), _card(5, "WR", 13.0), _card(6, "TE", 9.0),
        _card(7, "RB", 12.0),  # best remaining flex
        _card(8, "WR", 4.0),
    ]
    out = sl._recommend_lineup(ranked, ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "BN", "BN"])
    assert [c["sleeper_id"] for c in out] == ["1", "2", "3", "4", "5", "6", "7"]


def test_recommend_lineup_never_starts_a_player_twice():
    ranked = [_card(1, "RB", 20.0), _card(2, "WR", 10.0)]
    out = sl._recommend_lineup(ranked, ["RB", "FLEX"])
    assert [c["sleeper_id"] for c in out] == ["1", "2"]


def test_recommend_lineup_skips_kickers_and_defense():
    # The model has no projection for K/DEF, so recommending one would be
    # inventing a number.
    ranked = [_card(1, "QB", 20.0), _card(2, "K", 99.0), _card(3, "DEF", 99.0)]
    out = sl._recommend_lineup(ranked, ["QB", "K", "DEF"])
    assert [c["sleeper_id"] for c in out] == ["1"]


def test_recommend_lineup_falls_back_when_league_has_no_positions():
    ranked = [_card(i, p, 20 - i) for i, p in enumerate(
        ["QB", "RB", "RB", "WR", "WR", "TE", "WR"], start=1)]
    assert len(sl._recommend_lineup(ranked, [])) == 7


def test_position_counts():
    assert sl._position_counts([_card(1, "RB", 1), _card(2, "RB", 2), _card(3, "WR", 3)]) == {
        "RB": 2, "WR": 1}


# --------------------------------------------------------------------------
# Analysis against stubbed Sleeper responses + real model data
# --------------------------------------------------------------------------

def _two_real_sleeper_ids():
    mapping = model_data.get("gsis_to_sleeper") or {}
    profile = model_data.get("df_profile")
    if profile is None or profile.is_empty() or not mapping:
        return []
    picked = []
    for row in profile.iter_rows(named=True):
        if (row.get("position") or "").upper() not in ("QB", "RB", "WR", "TE"):
            continue
        sid = mapping.get(row.get("player_id"))
        if sid:
            picked.append(str(sid))
        if len(picked) >= 4:
            break
    return picked


def test_analyze_roster_projects_real_players(client, monkeypatch):
    """`client` boots the app lifespan, which is what populates model_data."""
    ids = _two_real_sleeper_ids()
    if len(ids) < 4:
        pytest.skip("player id maps unavailable in this environment")

    def fake_get(path, ttl=None):
        if path.endswith("/rosters"):
            return [{"roster_id": 1, "owner_id": "u1",
                     "players": ids, "starters": ids[:1]}]
        if path.startswith("/league/"):
            return {"league_id": "L1", "name": "Test", "season": "2026",
                    "total_rosters": 1, "roster_positions": ["QB", "RB", "WR", "TE", "BN"],
                    "scoring_settings": {"rec": 1}}
        return None

    monkeypatch.setattr(sl, "_get", fake_get)
    result = asyncio.run(sl.analyze_roster("L1", 1, 1))

    assert result["roster_id"] == 1
    assert result["league"]["scoring_type"] == "PPR"
    assert len(result["players"]) > 0
    # Ranked descending by projection.
    projections = [float(p.get("prediction") or 0) for p in result["players"]]
    assert projections == sorted(projections, reverse=True)
    # Recommended starters are a subset of the roster.
    roster_ids = {p["sleeper_id"] for p in result["players"]}
    assert {c["sleeper_id"] for c in result["recommended_starters"]} <= roster_ids
    assert isinstance(result["projected_total"], float)


def test_analyze_roster_reports_unknown_players(client, monkeypatch):
    def fake_get(path, ttl=None):
        if path.endswith("/rosters"):
            return [{"roster_id": 2, "players": ["999999999"], "starters": []}]
        if path.startswith("/league/"):
            return {"league_id": "L1", "roster_positions": ["QB"], "scoring_settings": {}}
        return None

    monkeypatch.setattr(sl, "_get", fake_get)
    result = asyncio.run(sl.analyze_roster("L1", 2, 1))
    # An id we cannot map is reported, never silently dropped.
    assert result["unmatched_sleeper_ids"] == ["999999999"]
    assert result["players"] == []


def test_analyze_roster_rejects_unknown_roster(monkeypatch):
    monkeypatch.setattr(sl, "_get", lambda path, ttl=None: [] if path.endswith("/rosters") else {})
    with pytest.raises(sl.SleeperError):
        asyncio.run(sl.analyze_roster("L1", 42, 1))


def test_rostered_ids_union_across_teams(monkeypatch):
    monkeypatch.setattr(sl, "_get", lambda path, ttl=None: [
        {"roster_id": 1, "players": ["a", "b"]},
        {"roster_id": 2, "players": ["b", "c"]},
        {"roster_id": 3, "players": None},
    ])
    assert sl.rostered_sleeper_ids("L1") == {"a", "b", "c"}


def test_waivers_exclude_rostered_players(client, monkeypatch):
    ids = _two_real_sleeper_ids()
    if len(ids) < 4:
        pytest.skip("player id maps unavailable in this environment")

    def fake_get(path, ttl=None):
        if path.endswith("/rosters"):
            return [{"roster_id": 1, "players": ids}]
        if "trending" in path:
            return []
        return None

    monkeypatch.setattr(sl, "_get", fake_get)
    result = asyncio.run(sl.waiver_targets("L1", 1, limit=10))
    returned = {p["sleeper_id"] for p in result["players"]}
    assert returned.isdisjoint(set(ids)), "a rostered player must never appear on waivers"
    assert result["rostered"] == len(ids)


def test_sleeper_error_on_unreachable_api(monkeypatch):
    import requests

    def boom(*a, **k):
        raise requests.RequestException("offline")

    monkeypatch.setattr(sl.requests, "get", boom)
    with pytest.raises(sl.SleeperError):
        sl.resolve_user("anybody")


def test_resolve_user_handles_blank_input():
    assert sl.resolve_user("") is None
    assert sl.resolve_user("   ") is None
