import pytest
from fastapi.testclient import TestClient
from applications.server import app

client = TestClient(app)


def test_matchup_includes_spread_and_over_under():
    resp = client.get('/matchup/18/LV/KC')
    assert resp.status_code == 200
    data = resp.json()
    # Ensure top-level keys exist and are numeric
    assert 'spread' in data
    assert 'over_under' in data
    assert isinstance(data['over_under'], (int, float)) or data['over_under'] is None
    # Spread should be present for this matchup
    assert data['spread'] is not None
    assert isinstance(data['spread'], (int, float))


def test_player_history_touchdowns_present():
    # Choose a player known to be in the DB test dataset
    resp = client.get('/player/history/00-0023459')
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) > 0
    # Ensure each record includes touchdowns and it's an int
    for rec in data:
        assert 'touchdowns' in rec
        assert isinstance(rec['touchdowns'], int)
