"""League insights: standings, all-play luck, power, position ranks, matchups."""

import asyncio

import pytest

from applications.api.services import league_insights as li
from applications.api.services import sleeper_league as sl
from applications.api.state import model_data


def test_all_play_counts_every_opponent_every_week():
    weekly = {1: {1: 120.0, 2: 100.0, 3: 90.0, 4: 100.0}, 2: {1: 80.0, 2: 130.0, 3: 95.0, 4: 70.0}}
    ap = li.all_play(weekly)
    assert ap[1] == {"wins": 4, "losses": 2, "ties": 0, "pct": 0.667}
    assert ap[2] == {"wins": 4, "losses": 1, "ties": 1, "pct": 0.75}
    assert ap[4] == {"wins": 1, "losses": 4, "ties": 1, "pct": 0.25}


def test_streak_and_ranks():
    assert li.streak(["W", "L", "W", "W"]) == "W2"
    assert li.streak([]) is None
    assert li.ranks({"a": 10, "b": 20, "c": 20, "d": 5}) == {"b": 1, "c": 1, "a": 3, "d": 4}


def test_power_shifts_from_projection_toward_results_but_never_drops_either():
    assert li.power_weights(0) == {"projection": 1.0, "all_play": 0.0, "points_for": 0.0}
    mid = li.power_weights(5)
    assert mid["projection"] == pytest.approx(0.5) and mid["all_play"] == pytest.approx(0.3)
    assert li.power_weights(17)["projection"] == pytest.approx(0.35)


def test_win_probability_is_symmetric_and_modest():
    assert li.win_probability(110, 110) == pytest.approx(0.5)
    p = li.win_probability(120, 100)
    assert 0.7 < p < 0.8, "a 20-point projected edge is a lean, not a lock"
    assert li.win_probability(100, 120) == pytest.approx(1 - p)


# --- a whole league ---------------------------------------------------------------------

CARDS = {
    # gsis -> (name, position, projection)
    "q1": ("QB One", "QB", 20.0), "q2": ("QB Two", "QB", 14.0),
    "r1": ("RB One", "RB", 15.0), "r2": ("RB Two", "RB", 9.0), "r3": ("RB Three", "RB", 7.0),
    "w1": ("WR One", "WR", 16.0), "w2": ("WR Two", "WR", 8.0), "w3": ("WR Three", "WR", 11.0),
    "t1": ("TE One", "TE", 9.0), "t2": ("TE Two", "TE", 5.0),
}


@pytest.fixture
def league(monkeypatch):
    rosters = [
        {"roster_id": 1, "players": ["101", "201", "301", "401", "203", "12713"],
         "settings": {"wins": 1, "losses": 0, "fpts": 130, "fpts_decimal": 50, "fpts_against": 100, "ppts": 150}},
        {"roster_id": 2, "players": ["102", "202", "302", "402", "303", "SEA"],
         "settings": {"wins": 0, "losses": 1, "fpts": 100, "fpts_against": 130, "fpts_against_decimal": 50, "ppts": 125}},
    ]
    matchups = {
        1: [{"roster_id": 1, "matchup_id": 1, "points": 130.5}, {"roster_id": 2, "matchup_id": 1, "points": 100.0}],
        2: [{"roster_id": 1, "matchup_id": 1, "points": 12.0}, {"roster_id": 2, "matchup_id": 1, "points": 3.0}],
    }

    def fake_get(path, ttl=None):
        if path.endswith("/rosters"):
            return rosters
        if path.endswith("/users"):
            return [{"user_id": "u1", "display_name": "Me", "metadata": {"team_name": "Mine"}}]
        if "/matchups/" in path:
            return matchups.get(int(path.rsplit("/", 1)[1]), [])
        if path.startswith("/league/"):
            return {"league_id": "L", "name": "Test", "season": "2026", "total_rosters": 2,
                    "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "K", "DEF", "BN"], "scoring_settings": {"rec": 1}}
        return None

    for r in rosters:
        r["owner_id"] = "u1" if r["roster_id"] == 1 else "u2"
    monkeypatch.setattr(sl, "_get", fake_get)
    monkeypatch.setattr(sl, "external_projections", lambda s, w: {
        "12713": {"player": {"position": "K", "first_name": "Andy", "last_name": "Borregales"}, "stats": {"pts_ppr": 7.0}},
        "SEA": {"player": {"position": "DEF", "first_name": "Seattle", "last_name": "Seahawks"}, "stats": {"pts_ppr": 9.0}},
    })
    monkeypatch.setitem(model_data, "sleeper_map", {'101': 'q1', '102': 'q2', '201': 'r1', '202': 'r2', '203': 'r3', '301': 'w1', '302': 'w2', '303': 'w3', '401': 't1', '402': 't2'})  # real Sleeper player ids are numeric

    async def fake_card(gsis, week):
        name, pos, proj = CARDS[gsis]
        return {"player_id": gsis, "player_name": name, "position": pos, "team": "CAR", "prediction": proj}

    monkeypatch.setattr(li, "_card", fake_card)


def test_a_league_comes_back_whole(league):
    out = asyncio.run(li.league_insights("L", week=2, roster_id=2))
    teams = {t["roster_id"]: t for t in out["teams"]}
    assert out["league"]["weeks_played"] == 1, "week 2 is in progress; only week 1 counts"

    one, two = teams[1], teams[2]
    assert (one["wins"], one["points_for"], one["max_points_for"]) == (1, 130.5, 150.0)
    assert one["efficiency"] == pytest.approx(0.87)
    assert one["all_play"]["pct"] == 1.0
    assert two["luck"] is None, "one week of all-play is too little to call luck"

    # Model lineup: QB 20 + RB 15 + WR 16 + TE 9 + FLEX (RB3 7) = 67, + K 7.
    assert one["by_group"] == {"QB": 20.0, "RB": 15.0, "WR": 16.0, "TE": 9.0, "FLEX": 7.0, "K": 7.0}
    assert one["projected_total"] == 74.0
    # Team two: QB 14 + RB 9 + WR 11 + TE 5 + FLEX (WR 8) = 47, + DEF 9.
    assert two["projected_total"] == 56.0
    assert one["position_ranks"]["QB"] == 1 and two["position_ranks"]["QB"] == 2
    assert two["position_ranks"]["DEF"] == 1

    assert one["power_rank"] == 1 and one["standing_rank"] == 1

    m = out["matchups"][0]["teams"]
    assert {s["roster_id"] for s in m} == {1, 2}
    assert sum(s["win_probability"] for s in m) == pytest.approx(1.0)

    you = out["you"]
    assert you["standing_rank"] == 2 and you["power_rank"] == 2 and you["teams"] == 2
    assert you["opponent"]["roster_id"] == 1
    assert you["weakest"] in ("QB", "RB", "WR", "TE", "FLEX")
