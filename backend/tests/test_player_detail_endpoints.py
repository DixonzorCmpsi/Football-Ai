"""The player page's bio, ranks and depth chart.

These call the route functions directly with `model_data` patched, so they need
neither a database nor the CSVs.
"""
import polars as pl
import pytest

from applications.api.routes import players
from applications.api.state import model_data


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    """Every test sets exactly the frames it reads and starts from empty caches."""
    for key in ("df_depth_charts", "df_player_stats", "df_profile"):
        monkeypatch.setitem(model_data, key, pl.DataFrame())
    monkeypatch.setitem(model_data, "data_loaded_at", None)
    players._RANK_CACHE.clear()
    monkeypatch.setitem(players._PLAYERS_TABLE, "df", pl.DataFrame())


def _depth(rows):
    return pl.DataFrame(
        rows,
        schema=["dt", "team", "player_name", "gsis_id", "pos_grp", "pos_abb", "pos_slot", "pos_rank"],
        orient="row",
    )


class TestDepthChart:
    def test_slots_are_ordered_ranked_and_deduplicated(self, monkeypatch):
        off = "3WR 1TE"
        monkeypatch.setitem(model_data, "df_depth_charts", _depth([
            ("2026-10-12", "LAC", "B Mitchell", "00-2", off, "RB", 11, 2),
            ("2026-10-12", "LAC", "A Hampton", "00-1", off, "RB", 11, 1),
            # the feed repeats a player and leaves rank ties
            ("2026-10-12", "LAC", "A Hampton", "00-1", off, "RB", 11, 1),
            ("2026-10-12", "LAC", "C Vidal", "00-3", off, "RB", 11, 2),
            ("2026-10-12", "LAC", "J Herbert", "00-9", off, "QB", 9, 1),
            ("2026-10-12", "LAC", "L McConkey", "00-5", off, "WR", 1, 1),
            ("2026-10-12", "LAC", "Q Johnston", "00-6", off, "WR", 2, 1),
            ("2026-10-12", "LAC", "T Harris", "00-7", off, "WR", 8, 1),
            ("2026-10-12", "LAC", "D James", "00-8", "Base 3-4 D", "SS", 9, 1),
            ("2026-10-12", "DEN", "Other Team", "00-4", off, "QB", 9, 1),
        ]))
        out = players.get_team_depth_chart("lac")
        assert out["team"] == "LAC"
        assert out["as_of"].startswith("2026-10-12")
        # Offense reads QB, RB, then the receivers by their slot labels.
        assert [s["pos"] for s in out["offense"]] == ["QB", "RB", "LWR", "RWR", "SWR"]
        rb = next(s for s in out["offense"] if s["pos"] == "RB")
        assert [(p["name"], p["rank"]) for p in rb["players"]] == [
            ("A Hampton", 1), ("B Mitchell", 2), ("C Vidal", 3),
        ]
        assert [s["pos"] for s in out["defense"]] == ["SS"]

    def test_unknown_team_is_empty_not_an_error(self):
        assert players.get_team_depth_chart("ZZZ") == {
            "team": "ZZZ", "as_of": None, "offense": [], "defense": [],
        }


def _stats(rows):
    return pl.DataFrame(
        rows,
        schema=["player_id", "week", "position", "team", "rushing_yards"],
        orient="row",
    )


class TestRanks:
    def test_nfl_and_team_rank_by_season_points(self, monkeypatch):
        monkeypatch.setitem(model_data, "df_player_stats", _stats([
            # rushing yards score 0.1 each: totals 30, 20, 10
            ("00-1", 1, "RB", "LAC", 100), ("00-1", 2, "RB", "LAC", 200),
            ("00-2", 1, "RB", "LAC", 100), ("00-2", 2, "RB", "LAC", 100),
            ("00-3", 1, "RB", "DEN", 50), ("00-3", 2, "RB", "DEN", 50),
            ("00-4", 1, "WR", "LAC", 900),  # a different position never counts
        ]))
        second = players.get_player_ranks("00-2")
        assert second["ranked"] is True
        assert (second["nfl_rank"], second["nfl_of"]) == (2, 3)
        assert (second["team_rank"], second["team_of"]) == (2, 2)
        assert second["total"] == 20.0 and second["games"] == 2

        third = players.get_player_ranks("00-3")
        assert (third["nfl_rank"], third["team_rank"], third["team_of"]) == (3, 1, 1)

    def test_a_week_listed_twice_counts_once(self, monkeypatch):
        monkeypatch.setitem(model_data, "df_player_stats", _stats([
            ("00-1", 1, "RB", "LAC", 100), ("00-1", 1, "RB", "LAC", 100),
        ]))
        out = players.get_player_ranks("00-1")
        assert out["games"] == 1 and out["total"] == 10.0

    def test_a_player_without_stats_is_not_ranked(self):
        assert players.get_player_ranks("00-404") == {"player_id": "00-404", "ranked": False}


class TestBio:
    def test_fields_come_from_the_players_table(self, monkeypatch):
        monkeypatch.setitem(players._PLAYERS_TABLE, "df", pl.DataFrame([{
            "gsis_id": "00-1", "display_name": "Kimani Vidal", "position": "RB", "latest_team": "LAC",
            "jersey_number": "28", "height": 68, "weight": 215, "birth_date": "2001-08-28",
            "years_of_experience": 3, "college_name": "Troy", "draft_year": 2024,
            "draft_round": 6, "draft_pick": 181, "draft_team": "LAC", "ngs_status_short_description": "Active",
        }]))
        bio = players.get_player_bio("00-1")
        assert bio["name"] == "Kimani Vidal"
        assert (bio["height_in"], bio["weight_lb"]) == (68, 215)
        assert bio["birth_date"] == "2001-08-28" and bio["age"] >= 25
        assert (bio["college"], bio["draft_round"], bio["draft_pick"]) == ("Troy", 6, 181)

    def test_missing_player_gives_nulls_not_guesses(self):
        bio = players.get_player_bio("00-404")
        assert bio["college"] is None and bio["birth_date"] is None and bio["height_in"] is None


class TestUsage:
    def _stats(self):
        base = dict(season=2026, season_type="REG", team="LAC", opponent_team="DEN")
        rows = []
        for week in (1, 2):
            rows += [
                dict(player_id="rb1", player_display_name="A Back", position="RB", week=week, rush_attempts=15, targets=3, receptions=2, rushing_yards=70, **base),
                dict(player_id="rb2", player_display_name="B Back", position="RB", week=week, rush_attempts=5, targets=1, receptions=1, rushing_yards=20, **base),
                dict(player_id="qb1", player_display_name="C Quarter", position="QB", week=week, attempts=30, rush_attempts=0, **base),
                dict(player_id="wr1", player_display_name="D Wide", position="WR", week=week, targets=6, receptions=4, receiving_yards=60, **base),
            ]
        return pl.DataFrame(rows)

    def test_shares_are_of_the_team_total_and_rank_teammates(self, monkeypatch):
        monkeypatch.setitem(model_data, "df_player_stats", self._stats())
        out = players.get_player_usage("rb1")
        assert out["team"] == "LAC" and out["season"] == 2026 and out["seasons"] == [2026]
        week1 = out["games"][0]
        # 15 of the team's 20 carries; 3 of its 10 targets (3 + 1 + 6)
        assert (week1["carries"], week1["team_carries"], week1["carry_share"]) == (15, 20, 75.0)
        assert (week1["targets"], week1["team_targets"], week1["target_share"]) == (3, 10, 30.0)
        assert out["summary"]["carry_share"] == 75.0
        assert out["summary"]["carries_per_game"] == 15.0
        assert [m["name"] for m in out["teammates"]] == ["A Back", "B Back"]
        assert out["teammates"][0]["rank"] == 1

    def test_quarterback_pass_attempt_share_and_unknown_player(self, monkeypatch):
        monkeypatch.setitem(model_data, "df_player_stats", self._stats())
        assert players.get_player_usage("qb1")["summary"]["pass_att_share"] == 100.0
        assert players.get_player_usage("nobody")["games"] == []


class TestStorylines:
    def test_a_failed_lookup_is_an_empty_list_not_a_500(self, monkeypatch):
        from applications.api.services import storylines

        def boom(*_a, **_k):
            raise RuntimeError("store not loaded")

        monkeypatch.setattr(storylines, "get_player_storylines", boom)
        out = players.get_storylines("00-1")
        assert out["storylines"] == [] and out["count"] == 0 and out["degraded"] is True
