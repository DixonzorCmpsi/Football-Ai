"""Shared pytest fixtures.

The API tests talk to the real app, and the app loads all of its data in its
lifespan handler. Starlette only runs lifespan when TestClient is used as a
context manager, so a module-level `TestClient(app)` yields a client whose
`model_data` is empty - every data-backed endpoint then 500s with
KeyError: 'df_profile'. Entering the context once per session fixes that and
keeps the (slow) data load to a single occurrence.
"""
import pytest
from fastapi.testclient import TestClient

from applications.server import app


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c
