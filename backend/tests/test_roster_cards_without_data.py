"""A matchup must still load when player data has not.

`test_matchup_includes_spread_and_over_under` failed on every machine without a
database: the roster builder filtered an empty, column-less frame and the whole
matchup endpoint returned 500, taking the game's odds down with it.
"""

import asyncio

import polars as pl

from applications.api.services import prediction as pr
from applications.api.state import model_data


def test_roster_is_empty_not_an_error_when_nothing_is_loaded(monkeypatch):
    monkeypatch.setattr(pr, "read_db", lambda q: pl.DataFrame())
    monkeypatch.setitem(model_data, "df_profile", pl.DataFrame())
    assert asyncio.run(pr.get_team_roster_cards("CAR", 1)) == []


def test_roster_is_empty_when_profiles_are_missing_entirely(monkeypatch):
    def down(q):
        raise ConnectionError("database down")
    monkeypatch.setattr(pr, "read_db", down)
    monkeypatch.delitem(model_data, "df_profile", raising=False)
    assert asyncio.run(pr.get_team_roster_cards("CAR", 1)) == []
