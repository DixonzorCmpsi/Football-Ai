"""Game lines: spread, moneyline and over/under, from Bovada's JSON or the schedule.

Every game's over/under went blank because Bovada split "O 41 (-110)" into
separate lines and the text scrape stopped matching. Worse, it kept "finding"
moneylines, which were really the spread prices. These pin the JSON parse, the
CSV merge that keeps finished games' lines, and the schedule fallback.
"""

import importlib.util
import os
import sys

import polars as pl
import pytest

RAG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rag_data")
sys.path.insert(0, RAG)

from bovada_api_client import GAME_LINE_COLUMNS, parse_event_game_lines  # noqa: E402

from applications.api.services.data_loader import fill_lines_from_schedule  # noqa: E402


def _load_10b():
    spec = importlib.util.spec_from_file_location("bovada_api_step", os.path.join(RAG, "10b_bovada_api_props.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _market(description, outcomes, period="Game"):
    return {"description": description, "period": {"description": period}, "outcomes": outcomes}


def _outcome(side, american, handicap=None, name=""):
    price = {"american": american}
    if handicap is not None:
        price["handicap"] = handicap
    return {"description": name, "type": side, "price": price}


# Shape copied from the live board for DEN @ KC, 2026-09-14.
EVENT = {
    "competitors": [{"name": "Denver Broncos", "home": False}, {"name": "Kansas City Chiefs", "home": True}],
    "displayGroups": [{
        "description": "Game Lines",
        "markets": [
            _market("Point Spread", [_outcome("A", "-105", "2.0", "Denver Broncos"), _outcome("H", "-115", "-2.0", "Kansas City Chiefs")]),
            _market("Moneyline", [_outcome("A", "+115", name="Denver Broncos"), _outcome("H", "-135", name="Kansas City Chiefs")]),
            _market("Total", [_outcome("O", "-105", "43.5", "Over"), _outcome("U", "-115", "43.5", "Under")]),
            # A first-half total must not overwrite the game total.
            _market("Total", [_outcome("O", "-110", "21.5", "Over"), _outcome("U", "-110", "21.5", "Under")], period="First Half"),
        ],
    }],
}


def test_game_lines_come_from_named_sides_not_render_order():
    row = parse_event_game_lines(EVENT, 2026, 1, "2026_01_DEN_KC", "t")
    assert row["home_team"] == "KC" and row["away_team"] == "DEN"
    assert row["total_over"] == 43.5 and row["total_under"] == 43.5
    assert row["home_ml"] == "-135" and row["away_ml"] == "+115", "moneylines, not spread prices"
    assert row["home_spread"] == "-2.0" and row["away_spread"] == "+2.0"
    assert row["total_over_prob"] == pytest.approx(51.22, abs=0.01)
    assert list(row) == GAME_LINE_COLUMNS, "COPY maps by position, so the order is the table's"


def test_an_event_without_game_markets_has_no_lines():
    assert parse_event_game_lines({"competitors": EVENT["competitors"], "displayGroups": []}, 2026, 1, "g") is None


def test_merge_replaces_refreshed_games_and_keeps_finished_ones(tmp_path):
    step = _load_10b()
    path = tmp_path / "lines.csv"
    old = pl.DataFrame([
        {**{c: None for c in GAME_LINE_COLUMNS}, "game_id": "2026_01_ATL_PIT", "week": "1", "season": "2026",
         "home_ml": "-105", "away_ml": "-115"},  # the broken scrape: ML, no total, no spread
        {**{c: None for c in GAME_LINE_COLUMNS}, "game_id": "2026_01_NE_SEA", "week": "1", "season": "2026",
         "total_over": "44.5", "home_spread": "-3.0", "home_ml": "-166"},  # a good finished game
        {**{c: None for c in GAME_LINE_COLUMNS}, "game_id": "2026_01_DEN_KC", "week": "1", "season": "2026",
         "total_over": "40.0"},  # about to be refreshed
    ], schema={c: pl.Utf8 for c in GAME_LINE_COLUMNS})
    old.write_csv(path)

    fresh = pl.DataFrame([parse_event_game_lines(EVENT, 2026, 1, "2026_01_DEN_KC", "t")], infer_schema_length=None)
    merged = step.merge_into_csv(str(path), fresh, GAME_LINE_COLUMNS, step._text_scrape_damage)

    by_game = {r["game_id"]: r for r in merged.iter_rows(named=True)}
    assert set(by_game) == {"2026_01_NE_SEA", "2026_01_DEN_KC"}
    assert by_game["2026_01_DEN_KC"]["total_over"] == "43.5"
    assert by_game["2026_01_NE_SEA"]["total_over"] == "44.5"
    assert merged.columns == GAME_LINE_COLUMNS


def _schedule():
    return pl.DataFrame({
        "game_id": ["2026_01_NE_SEA", "2026_01_DEN_KC", "2026_02_MIA_SF"],
        "week": [1, 1, 2], "season": [2026, 2026, 2026],
        "home_team": ["SEA", "KC", "SF"], "away_team": ["NE", "DEN", "MIA"],
        # nflverse: positive spread_line = home favored.
        "spread_line": [3.0, 2.5, 12.5], "total_line": [44.5, 44.0, 46.5],
        "moneyline_home": [-166, -140, -900], "moneyline_away": [140, 120, 600],
    })


def test_schedule_fills_games_bovada_lacks_and_leaves_bovada_alone():
    lines = pl.DataFrame({
        "game_id": ["2026_01_DEN_KC", "2026_01_NE_SEA"], "week": [1, 1], "season": [2026, 2026],
        "home_team": ["KC", "SEA"], "away_team": ["DEN", "NE"],
        "total_over": [43.5, None], "home_spread": [-2.0, None], "home_ml": ["-135", "-105"],
    })
    out = fill_lines_from_schedule(lines, _schedule())
    by_game = {r["game_id"]: r for r in out.iter_rows(named=True)}
    assert len(out) == 3
    assert by_game["2026_01_DEN_KC"]["total_over"] == 43.5, "Bovada's own line wins"
    assert by_game["2026_01_DEN_KC"]["home_ml"] == "-135"
    sea = by_game["2026_01_NE_SEA"]
    assert sea["total_over"] == 44.5 and sea["home_spread"] == -3.0, "home favored -> negative handicap"
    assert sea["home_ml"] == "-166" and sea["processed_at"] == "schedule", "the broken row is replaced"
    assert by_game["2026_02_MIA_SF"]["away_ml"] == "+600"


def test_no_bovada_at_all_still_gives_every_game_a_total():
    out = fill_lines_from_schedule(pl.DataFrame(), _schedule())
    assert out.height == 3 and out["total_over"].null_count() == 0


def test_a_schedule_without_lines_changes_nothing():
    lines = pl.DataFrame({"game_id": ["g"], "total_over": [40.0]})
    assert fill_lines_from_schedule(lines, _schedule().drop("total_line")).equals(lines)
