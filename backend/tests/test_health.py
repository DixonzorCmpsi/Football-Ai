import pytest
from fastapi.testclient import TestClient
from applications.server import app

client = TestClient(app)

def test_health_ok():
    resp = client.get('/health')
    assert resp.status_code == 200
    data = resp.json()
    assert 'status' in data
    assert 'models_loaded' in data
    assert 'etl_script_exists' in data


def test_health_db_field_present():
    resp = client.get('/health')
    assert resp.status_code == 200
    data = resp.json()
    assert 'db_responding' in data
