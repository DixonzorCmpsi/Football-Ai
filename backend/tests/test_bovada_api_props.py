"""Bovada JSON board parsing and prop/player name matching.

The old pipeline scraped Bovada's *rendered page text*, which only ever showed
the default market tab -- so the app had Anytime TD for everyone and real
statistical lines for a couple of dozen stars. These tests pin the structured
parse that replaced it, and the name-matching guard that stops one player's
lines from being displayed under another player's name.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rag_data"))

from bovada_api_client import (  # noqa: E402
    american_to_implied_prob,
    classify_market,
    event_teams,
    parse_event_player_props,
    split_player_and_team,
)


def test_american_to_implied_prob():
    assert american_to_implied_prob("+100") == 50.0
    assert american_to_implied_prob("EVEN") == 50.0
    assert american_to_implied_prob("-110") == pytest.approx(52.38, abs=0.01)
    assert american_to_implied_prob("+825") == pytest.approx(10.81, abs=0.01)
    assert american_to_implied_prob(None) is None
    assert american_to_implied_prob("n/a") is None


def test_split_player_and_team():
    assert split_player_and_team("Caleb Williams (CHI)") == ("Caleb Williams", "CHI")
    # Bovada's own codes differ from nflverse in a few spots.
    assert split_player_and_team("Puka Nacua (LAR)") == ("Puka Nacua", "LA")
    # Half/quarter suffixes must not leak into the name.
    assert split_player_and_team("Jalen Coker (CAR) - 1H") == ("Jalen Coker", "CAR")
    assert split_player_and_team("") == (None, None)


def test_classify_market():
    assert classify_market("Total Passing Yards - Bryce Young (CAR)") == (
        "ou", "Passing Yards", "Bryce Young (CAR)")
    assert classify_market("Anytime Touchdown Scorer")[:2] == ("list", "Anytime TD")
    assert classify_market("Player to Record a 20+ Yard Reception")[:2] == (
        "list", "20+ Yard Reception")
    # Game-level markets are not player props.
    assert classify_market("Total Points")[0] is None
    assert classify_market("Winning Margin")[0] is None


def _event(markets, group="Passing Yards"):
    return {"displayGroups": [{"description": group, "markets": markets}]}


def test_parse_over_under_market_yields_both_sides():
    ev = _event([{
        "description": "Total Passing Yards - Caleb Williams (CHI)",
        "period": {"description": "Game"},
        "outcomes": [
            {"description": "Over", "price": {"handicap": "228.5", "american": "-115"}},
            {"description": "Under", "price": {"handicap": "228.5", "american": "-115"}},
        ],
    }])
    rows = parse_event_player_props(ev, 2026, 1, "2026_01_CHI_CAR")
    assert len(rows) == 2
    assert {r["side"] for r in rows} == {"over", "under"}
    assert all(r["line"] == 228.5 for r in rows)
    assert all(r["player_name"] == "Caleb Williams" for r in rows)
    assert all(r["prop_type"] == "Passing Yards" for r in rows)


def test_parse_skips_non_game_periods():
    # A 1H line for the same player would otherwise collide with his game line.
    ev = _event([{
        "description": "Total Passing Yards - Caleb Williams (CHI)",
        "period": {"description": "First Half"},
        "outcomes": [
            {"description": "Over", "price": {"handicap": "120.5", "american": "-115"}},
        ],
    }])
    assert parse_event_player_props(ev, 2026, 1, "g") == []


def test_parse_player_list_market():
    ev = _event([{
        "description": "Anytime Touchdown Scorer",
        "period": {"description": "Game"},
        "outcomes": [
            {"description": "Chuba Hubbard (CAR)", "price": {"american": "+125"}},
            {"description": "Sam Roush (CHI)", "price": {"american": "+2500"}},
        ],
    }], group="TD Scorer Props")
    rows = parse_event_player_props(ev, 2026, 1, "g")
    assert len(rows) == 2
    assert {r["player_name"] for r in rows} == {"Chuba Hubbard", "Sam Roush"}
    assert all(r["side"] == "Yes" and r["line"] == 1.0 for r in rows)
    assert rows[0]["implied_prob"] == pytest.approx(44.44, abs=0.01)


def test_parse_ignores_outcomes_without_a_line():
    ev = _event([{
        "description": "Total Receiving Yards - Some Guy (CHI)",
        "period": {"description": "Game"},
        "outcomes": [{"description": "Over", "price": {"american": "-115"}}],
    }])
    assert parse_event_player_props(ev, 2026, 1, "g") == []


def test_event_teams_uses_competitor_flags():
    ev = {"competitors": [
        {"name": "Carolina Panthers", "home": True},
        {"name": "Chicago Bears", "home": False},
    ]}
    assert event_teams(ev) == ("CAR", "CHI")


# --------------------------------------------------------------------------
# Prop <-> player name matching
# --------------------------------------------------------------------------

from applications.api.services.prediction import _safe_prop_name_match, _surname  # noqa: E402


def test_surname_ignores_generational_suffixes():
    assert _surname("Michael Pittman Jr.") == "pittman"
    assert _surname("Marvin Harrison Jr") == "harrison"
    assert _surname("Odell Beckham III") == "beckham"
    assert _surname("") == ""


def test_fuzzy_match_never_crosses_players():
    # The exact regression: at difflib's old 0.6 cutoff "Kenny Pickett" matched
    # "Kyle Pitts", so a backup QB displayed a tight end's receiving lines.
    assert _safe_prop_name_match("Kenny Pickett", ["Kyle Pitts", "Puka Nacua"]) is None


def test_fuzzy_match_still_handles_name_variants():
    assert _safe_prop_name_match(
        "Michael Pittman Jr.", ["Michael Pittman", "Kyle Pitts"]) == "Michael Pittman"


def test_fuzzy_match_handles_empty_inputs():
    assert _safe_prop_name_match("", ["Kyle Pitts"]) is None
    assert _safe_prop_name_match("Kyle Pitts", []) is None
