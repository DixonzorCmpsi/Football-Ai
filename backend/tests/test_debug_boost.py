import pytest
from fastapi.testclient import TestClient
from applications.server import app

client = TestClient(app)

def test_debug_usage_boost_structure():
    # We can't assert boosting for a real player environment-agnostically, but we can check response shape
    resp = client.get('/debug/usage-boost/some_player/1')
    assert resp.status_code == 200
    data = resp.json()
    assert 'found' in data
    # Either we found player data (found True) or we got an error (found False with error)
    assert isinstance(data['found'], bool)
