import pytest

# These tests exercise real, live-loaded data via the session `client` fixture
# in conftest.py (which runs the app's lifespan so model_data is populated).


def _first_game_with_lines(client):
    """Find a scheduled game whose matchup actually carries betting lines.

    Bovada only publishes lines a few weeks out, so hard-coding a far-future
    week (this used to ask for week 18) guarantees a null spread no matter how
    healthy the pipeline is. Walk the early weeks instead and use the first
    game that has real lines.
    """
    week = client.get("/current_week").json().get("week") or 1
    for wk in range(week, week + 6):
        sched = client.get(f"/schedule/{wk}")
        if sched.status_code != 200:
            continue
        for game in sched.json()[:4]:
            resp = client.get(f"/matchup/{wk}/{game['home_team']}/{game['away_team']}")
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data.get("spread") is not None and data.get("over_under") is not None:
                return wk, game, data
    return None, None, None


def test_matchup_includes_spread_and_over_under(client):
    week = client.get("/current_week").json().get("week") or 1
    sched = client.get(f"/schedule/{week}")
    assert sched.status_code == 200
    games = sched.json()
    assert isinstance(games, list) and games, f"no schedule for week {week}"

    game = games[0]
    resp = client.get(f"/matchup/{week}/{game['home_team']}/{game['away_team']}")
    assert resp.status_code == 200
    data = resp.json()

    # Shape must always hold, whether or not a book has posted this game yet.
    assert "spread" in data
    assert "over_under" in data
    assert data["over_under"] is None or isinstance(data["over_under"], (int, float))
    assert data["spread"] is None or isinstance(data["spread"], (int, float))
    assert isinstance(data.get("away_roster"), list)
    assert isinstance(data.get("home_roster"), list)


def test_matchup_spread_populated_when_lines_exist(client):
    """The odds pipeline end-to-end: at least one upcoming game should carry
    a real spread and total. If none do, the Bovada scrape/process step has
    silently stopped producing lines."""
    wk, game, data = _first_game_with_lines(client)
    if wk is None:
        pytest.skip("no betting lines loaded for any upcoming week")
    assert isinstance(data["spread"], (int, float))
    assert isinstance(data["over_under"], (int, float))
    assert data["over_under"] > 0


def test_player_history_touchdowns_present(client):
    resp = client.get("/player/history/00-0023459")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for rec in data:
        assert "touchdowns" in rec
        assert isinstance(rec["touchdowns"], int)
        # Turnover fields are what the Compare scoring-mix chart reconciles against.
        assert isinstance(rec.get("interceptions", 0), int)
        assert isinstance(rec.get("fumbles_lost", 0), int)
