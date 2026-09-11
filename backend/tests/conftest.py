"""Shared pytest fixtures.

Import nothing heavy at module scope. CI deliberately runs only the subset of
tests that need neither a live DB nor pre-populated CSVs (see
.github/workflows), and installs the lean requirements-dev.txt. pytest loads
this conftest for the whole tests/ directory regardless, so importing
fastapi.testclient here would break collection for those tests too --
starlette's TestClient needs httpx, which the lean CI deps do not include.
Everything therefore gets imported inside the fixture, which only the
data-backed tests request.
"""
import pytest


@pytest.fixture(scope="session")
def client():
    """A TestClient with the app's lifespan actually run.

    Starlette only executes lifespan when TestClient is used as a context
    manager. A module-level `TestClient(app)` therefore yields a client whose
    model_data is empty, and every data-backed endpoint 500s with
    KeyError: 'df_profile'. Entering the context once per session fixes that
    and keeps the (slow) data load to a single occurrence.
    """
    pytest.importorskip("httpx", reason="starlette's TestClient requires httpx")
    from fastapi.testclient import TestClient

    from applications.server import app

    with TestClient(app) as c:
        yield c