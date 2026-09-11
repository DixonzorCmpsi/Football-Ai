"""Response shaping for the MCP server.

The REST API is built for a UI -- a single matchup is ~31 player cards with a
dozen prop rows each -- so these turn that into something an agent can reason
over without burning its context. The bugs pinned here were all found by running
the tools against the live backend rather than by reading the code.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_server.formatting import (  # noqa: E402
    player_line,
    props_summary,
    summarize_history,
    summarize_matchup,
    summarize_roster_analysis,
    summarize_schedule,
    summarize_search,
    summarize_storylines,
    summarize_waivers,
)


def _card(**kw):
    card = {"player_name": "Brock Purdy", "position": "QB", "team": "SF",
            "opponent": "LA", "prediction": 22.5, "floor_prediction": 18.0}
    card.update(kw)
    return card


# --------------------------------------------------------------------------
# Player line
# --------------------------------------------------------------------------

def test_player_line_core_fields():
    out = player_line(_card())
    assert "Brock Purdy (QB SF vs LA)" in out
    assert "proj 22.5" in out
    assert "floor 18.0" in out


def test_player_line_does_not_rescale_snap_percentage():
    # The API returns 0-100 already; multiplying by 100 here produced "snaps 8400%".
    assert "snaps 84%" in player_line(_card(snap_percentage=84.0))
    assert "snaps 100%" in player_line(_card(snap_percentage=100.0))


def test_player_line_flags_only_real_injuries():
    assert "[Out]" in player_line(_card(injury_status="Out"))
    assert "[" not in player_line(_card(injury_status="Active"))
    assert "[" not in player_line(_card(injury_status=""))


def test_player_line_handles_missing_opponent_and_empty_card():
    assert "(QB SF)" in player_line(_card(opponent=None))
    assert player_line({}) == ""


# --------------------------------------------------------------------------
# Props
# --------------------------------------------------------------------------

def test_props_summary_drops_the_under_side():
    props = [
        {"prop_type": "Passing Yards", "line": 228.5, "side": "over",
         "odds": "-115", "implied_prob": 53.5},
        {"prop_type": "Passing Yards", "line": 228.5, "side": "under",
         "odds": "-115", "implied_prob": 53.5},
    ]
    out = props_summary(props)
    # Both sides carry the same line, so printing both doubles text for nothing.
    assert out.count("Passing Yards") == 1
    assert "228.5" in out and "-115" in out


def test_props_summary_respects_limit_and_empty():
    props = [{"prop_type": f"M{i}", "line": i, "side": "over"} for i in range(20)]
    assert len(props_summary(props, limit=3).split(";")) == 3
    assert props_summary([]) == ""


# --------------------------------------------------------------------------
# Matchup
# --------------------------------------------------------------------------

def test_summarize_matchup_ranks_and_trims():
    data = {
        "overunder": 48.5, "spread": -3.0,
        "weather": {"is_dome": False, "temp_f": 72, "wind_mph": 5, "condition": "Clear"},
        "home_roster": [_card(player_name=f"P{i}", prediction=i) for i in range(12)],
        "away_roster": [],
        "home_injuries": [
            {"name": "Hurt Guy", "position": "WR", "status": "Out", "is_starter": True},
            {"name": "Fine Guy", "position": "WR", "status": "Active"},
        ],
    }
    out = summarize_matchup(data, 1, "SF", "LA", top_n=3)
    assert "Week 1: LA @ SF" in out
    assert "O/U 48.5" in out and "spread -3.0" in out
    assert "72F" in out and "wind 5mph" in out
    # Highest projection first, trimmed to top_n.
    assert out.index("P11") < out.index("P10")
    assert "P8" not in out
    # Healthy players are not an injury report.
    assert "Hurt Guy" in out and "Fine Guy" not in out
    assert "starter" in out


def test_summarize_matchup_reports_dome():
    data = {"weather": {"is_dome": True}, "home_roster": [], "away_roster": []}
    assert "dome" in summarize_matchup(data, 1, "MIN", "GB")


def test_summarize_matchup_handles_no_data():
    assert "No matchup data" in summarize_matchup({}, 1, "SF", "LA")


# --------------------------------------------------------------------------
# History -- the "- pts" bug
# --------------------------------------------------------------------------

def test_summarize_history_reads_the_endpoint_column_names():
    # /player/history returns points/passing_yds, not fantasy_points_ppr/passing_yards.
    rows = [{"season": 2026, "week": 1, "opponent": "LA", "points": 21.1,
             "passing_yds": 205, "rushing_yds": 29, "touchdowns": 3}]
    out = summarize_history(rows)
    assert "21.1 pts" in out
    assert "205 pass yd" in out
    assert "2026 wk1 vs LA" in out


def test_summarize_history_accepts_the_weekly_stats_spelling_too():
    rows = [{"season": 2025, "week": 4, "fantasy_points_ppr": 18.2, "passing_yards": 310}]
    out = summarize_history(rows)
    assert "18.2 pts" in out and "310 pass yd" in out


def test_summarize_history_handles_wrapped_and_empty():
    assert "21.1 pts" in summarize_history({"history": [{"week": 1, "points": 21.1}]})
    assert summarize_history([]) == "No history available."


# --------------------------------------------------------------------------
# Sleeper
# --------------------------------------------------------------------------

def test_roster_analysis_leads_with_the_decision():
    data = {
        "league": {"name": "Test", "scoring_type": "PPR"},
        "roster_id": 1, "week": 1, "projected_total": 112.4,
        "bench_but_should_start": [_card(player_name="Bench Guy")],
        "start_but_should_sit": [_card(player_name="Sit Guy")],
        "recommended_starters": [_card(player_name="Bench Guy")],
        "unmatched_sleeper_ids": ["999"],
    }
    out = summarize_roster_analysis(data)
    assert "112.4 pts" in out
    assert "START Bench Guy" in out
    assert "SIT   Sit Guy" in out
    assert "1 rostered player(s) could not be matched" in out


def test_roster_analysis_says_so_when_nothing_should_change():
    data = {"league": {}, "roster_id": 1, "week": 1, "projected_total": 100.0,
            "recommended_starters": []}
    assert "already matches" in summarize_roster_analysis(data)


def test_waivers_include_trending_adds():
    data = {"week": 1, "rostered": 180,
            "players": [_card(player_name="FA Guy", trending_adds=4200)]}
    out = summarize_waivers(data)
    assert "FA Guy" in out and "+4200 adds/24h" in out
    assert "180 players rostered" in out


def test_waivers_handles_empty():
    assert "No available free agents" in summarize_waivers({"week": 1, "players": []})


# --------------------------------------------------------------------------
# Misc
# --------------------------------------------------------------------------

def test_search_lists_ids_for_follow_up_calls():
    out = summarize_search([{"player_name": "Brock Purdy", "position": "QB",
                             "team_abbr": "SF", "player_id": "00-0037834",
                             "status": "ACT"}])
    assert "00-0037834" in out


def test_search_truncates_long_lists():
    rows = [{"player_name": f"P{i}", "position": "WR", "team_abbr": "SF",
             "player_id": str(i)} for i in range(40)]
    out = summarize_search(rows, limit=5)
    assert "and 35 more" in out


def test_schedule_shows_finals_and_kickoffs():
    games = [
        {"away_team": "NE", "home_team": "SEA", "away_score": 10, "home_score": 13},
        {"away_team": "CHI", "home_team": "CAR", "gameday": "2026-09-13"},
    ]
    out = summarize_schedule(games, 1)
    assert "FINAL NE 10 - SEA 13" in out
    assert "CHI @ CAR" in out and "2026-09-13" in out


def test_storylines_and_empty_states():
    out = summarize_storylines([{"headline": "Purdy shines", "published": "2026-09-11",
                                 "description": "Threw for 205."}])
    assert "Purdy shines" in out and "Threw for 205." in out
    assert "No recent storylines" in summarize_storylines([])
    assert "No games found" in summarize_schedule([], 3)
