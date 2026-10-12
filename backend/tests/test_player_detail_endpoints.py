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
