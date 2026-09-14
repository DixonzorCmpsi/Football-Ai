"""Head-to-head fantasy matchup: the calls it makes, and the opponent it finds."""

from __future__ import annotations

import asyncio

import polars as pl
import pytest

from applications.api.services import fantasy_matchup as fm
from applications.api.services import sleeper_league as sl


class TestPieces:
    def test_range_widens_with_volume_and_never_goes_negative(self):
        low_floor, low_ceiling = fm.projection_range(2.0)
        high_floor, high_ceiling = fm.projection_range(20.0)
        assert low_floor == 0.0
        assert (high_ceiling - high_floor) > (low_ceiling - low_floor)

    def test_win_probability_is_even_at_zero_margin_and_monotone(self):
        assert fm.normal_win_probability(0, 30) == pytest.approx(0.5)
        assert fm.normal_win_probability(10, 30) > fm.normal_win_probability(5, 30) > 0.5

    @pytest.mark.parametrize("spread, total, label", [
        (-8.0, 50.0, "Positive"),
        (9.0, 38.0, "Negative"),
        (-1.0, 44.0, "Neutral"),
        (None, None, "Neutral"),
    ])
    def test_game_script(self, spread, total, label):
        assert fm.game_script(spread, total, None)["label"] == label

    def test_td_chance_prefers_the_book_and_labels_the_source(self):
        assert fm.td_chance(38.5, 0.9, 30.0) == {"probability": 0.385, "source": "bovada"}
        model = fm.td_chance(None, 0.5, 22.5)
        assert model["source"] == "model" and model["probability"] == pytest.approx(0.393, abs=0.001)
        assert fm.td_chance(None, None, 25.0) is None

    def test_a_hot_streak_is_pulled_toward_the_position_norm(self):
        stats = pl.DataFrame({"player_id": ["p"] * 4, "season": [2026] * 4, "week": [1, 2, 3, 4],
                              "rush_touchdown": [2, 2, 2, 2], "receiving_touchdown": [0, 0, 0, 0],
                              "passing_touchdown": [3, 3, 3, 3]})
        raw = fm.touchdowns_per_game("p", [(stats, True)])
        shrunk = fm.touchdowns_per_game("p", [(stats, True)], position="RB")
        assert raw == 2.0, "passing touchdowns aren't a player's own touchdowns"
        assert fm.TD_PRIOR["RB"] < shrunk < raw
        assert fm.touchdowns_per_game("nobody", [(stats, True)], position="WR") == fm.TD_PRIOR["WR"]

    def test_main_props_pick_the_even_money_line_per_market(self):
        props = [
            {"prop_type": "Receiving Yards", "line": 48.5, "odds": "-110", "side": "over", "implied_prob": 52.38},
            {"prop_type": "Receiving Yards", "line": 69.5, "odds": "+200", "side": "over", "implied_prob": 33.3},
            {"prop_type": "Receiving Yards", "line": 48.5, "odds": "-110", "side": "under", "implied_prob": 52.38},
            {"prop_type": "Receptions", "line": 3.5, "odds": "-130", "side": "over", "implied_prob": 56.5},
            {"prop_type": "Passing Yards", "line": 250.5, "odds": "-110", "side": "over", "implied_prob": 52.38},
        ]
        assert fm.main_props("WR", props) == [
            {"market": "Receiving Yards", "line": 48.5, "odds": "-110"},
            {"market": "Receptions", "line": 3.5, "odds": "-130"},
        ]

    def test_defense_rank_one_is_the_softest(self):
        stats = pl.DataFrame({
            "week": [1, 1, 1, 2], "position": ["WR", "WR", "WR", "WR"],
            "opponent_team": ["NYG", "NYG", "BUF", "BUF"], "y_fantasy_points_ppr": [20.0, 15.0, 8.0, 6.0],
        })
        ranks = fm.defense_vs_position([(stats, True)])["WR"]
        assert ranks["NYG"]["rank"] == 1 and ranks["NYG"]["allowed"] == 35.0
        assert ranks["BUF"]["rank"] == 2 and ranks["BUF"]["allowed"] == 7.0

    def test_injury_flags(self):
        assert fm.injury_flag("Questionable") == "Q"
        assert fm.injury_flag("Out") == "OUT"
        assert fm.injury_flag("Active") is None


def _card(pid, name, pos, team, opp, proj, status="Active"):
    return {"player_id": pid, "player_name": name, "position": pos, "team": team, "opponent": opp,
            "prediction": proj, "injury_status": status, "spread": -3.5, "overunder": 47.5,
            "implied_total": 25.5, "moneyline": "-170", "lines_source": "bovada", "props": [],
            "anytime_td_prob": 40.0 if pos != "QB" else None}


@pytest.fixture
def league(monkeypatch):
    cards = {
        "g1": _card("g1", "Me QB", "QB", "DAL", "WAS", 18.0),
        "g2": _card("g2", "Me WR", "WR", "LA", "NYG", 15.0, status="Out"),
        "g3": _card("g3", "Bench WR", "WR", "CIN", "HOU", 11.0),
        "g4": _card("g4", "Them QB", "QB", "WAS", "DAL", 16.0),
        "g5": _card("g5", "Them WR", "WR", "KC", "IND", 9.0),
    }
    monkeypatch.setattr(sl, "get_league", lambda lid: {"league_id": lid, "name": "Test", "scoring_type": "PPR",
                                                        "season": "2026", "roster_positions": ["QB", "WR", "BN"]})
    monkeypatch.setattr(sl, "get_rosters", lambda lid: [
        {"roster_id": 1, "players": ["1", "2", "3"], "starters": ["1", "2"], "settings": {"wins": 1, "losses": 0}},
        {"roster_id": 2, "players": ["4", "5"], "starters": ["4", "5"], "settings": {"wins": 0, "losses": 1}},
        {"roster_id": 3, "players": [], "starters": [], "settings": {}},
    ])
    monkeypatch.setattr(sl, "roster_owners", lambda lid: [
        {"roster_id": 1, "team_name": "Mine", "display_name": "me"},
        {"roster_id": 2, "team_name": "Theirs", "display_name": "them"},
    ])
    monkeypatch.setattr(sl, "_get", lambda path, ttl=0: [
        {"roster_id": 1, "matchup_id": 7, "starters": ["1", "2"], "points": 0, "players_points": {}},
        {"roster_id": 2, "matchup_id": 7, "starters": ["4", "5"], "points": 0, "players_points": {}},
        {"roster_id": 3, "matchup_id": None, "starters": []},
    ])
    monkeypatch.setattr(sl, "external_projections", lambda season, week: {})
    monkeypatch.setattr(sl, "_sleeper_to_gsis", lambda sid: f"g{sid}")

    async def fake_card(gsis, week):
        return cards.get(gsis)

    monkeypatch.setattr(fm, "_card", fake_card)
    monkeypatch.setattr(fm, "_stats_frames", lambda: [])
    monkeypatch.setattr(fm, "defense_vs_position", lambda frames=None: {"WR": {"NYG": {"rank": 3, "allowed": 30.0, "teams": 32}}})
    monkeypatch.setattr(fm, "_kickoffs", lambda week: {"DAL": {"kickoff": "2026-09-20 16:25", "home": True, "opponent": "WAS", "final": False},
                                                       "WAS": {"kickoff": "2026-09-20 16:25", "home": False, "opponent": "DAL", "final": False}})


class TestHeadToHead:
    def test_finds_the_opponent_and_compares_every_slot(self, league):
        out = asyncio.run(fm.head_to_head("L1", 1, 2))
        me, them = out["you"], out["opponent"]
        assert them["team_name"] == "Theirs"
        assert [p["player_name"] for p in me["starters"]] == ["Me QB", "Me WR"]
        assert me["projected_total"] == 33.0 and them["projected_total"] == 25.0
        assert 0.5 < out["win_probability"] < 1
        assert {e["group"]: e["edge"] for e in out["group_edges"]} == {"QB": 2.0, "WR": 6.0}
        wr = me["starters"][1]
        assert wr["injury_flag"] == "OUT" and wr["defense_rank"]["rank"] == 3
        assert wr["td"] == {"probability": 0.4, "source": "bovada"}
        assert wr["script"]["label"] == "Positive"
        # Both managers start a player in DAL vs WAS.
        assert out["shared_games"][0]["game"] == "DAL vs WAS"

    def test_suggests_a_bench_player_for_an_injured_starter(self, league):
        out = asyncio.run(fm.head_to_head("L1", 1, 2))
        assert out["bench_fixes"] == [{"out": "Me WR", "slot": "WR", "reason": "injury: Out",
                                       "replace_with": {"player_id": "g3", "player_name": "Bench WR", "position": "WR", "projection": 11.0}}]

    def test_a_team_without_an_opponent_gets_a_message(self, league):
        out = asyncio.run(fm.head_to_head("L1", 3, 2))
        assert out["opponent"] is None and "No opponent" in out["message"]

    def test_over_http(self, league, client):
        response = client.get("/sleeper/league/L1/roster/1/matchup?week=2")
        assert response.status_code == 200, response.text
        assert response.json()["opponent"]["team_name"] == "Theirs"
