"""Transport hardening for the MCP server.

Every tool call used to cost ~2s. Three causes, all measured against the live
backend rather than guessed at:

  * "localhost" resolves ::1 first on Windows and falls back to IPv4, ~200ms per
    request -- 216ms vs 16ms for the same /health call against 127.0.0.1.
  * a fresh httpx.Client per call, so every request paid a new TCP handshake.
  * no caching, so an agent re-asking the current week or a player's id paid
    full latency each time.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_server import server as S  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_cache():
    S.clear_cache()
    yield
    S.clear_cache()


# --------------------------------------------------------------------------
# Base URL
# --------------------------------------------------------------------------

def test_localhost_is_rewritten_to_the_loopback_address():
    # 13x faster on Windows; the name resolution is the entire difference.
    assert S._normalize_base("http://localhost:8000") == "http://127.0.0.1:8000"
    assert S._normalize_base("http://localhost:8000/") == "http://127.0.0.1:8000"


def test_other_hosts_are_left_alone():
    assert S._normalize_base("http://10.0.0.5:8000") == "http://10.0.0.5:8000"
    # A hostname that merely contains "localhost" is not the loopback name.
    assert S._normalize_base("http://mylocalhost.dev") == "http://mylocalhost.dev"


def test_normalize_handles_empty():
    assert S._normalize_base("") == ""


# --------------------------------------------------------------------------
# Caching
# --------------------------------------------------------------------------

def test_repeat_request_is_served_from_cache(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return {"week": 1}

    class FakeClient:
        def get(self, url, params=None):
            calls.append(url)
            return FakeResponse()

    monkeypatch.setattr(S, "_http", lambda: FakeClient())
    assert S._get("/current_week") == {"week": 1}
    assert S._get("/current_week") == {"week": 1}
    assert len(calls) == 1, "second identical request must not hit the backend"


def test_different_params_are_cached_separately(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return {}

    class FakeClient:
        def get(self, url, params=None):
            calls.append((url, tuple(sorted((params or {}).items()))))
            return FakeResponse()

    monkeypatch.setattr(S, "_http", lambda: FakeClient())
    S._get("/player/x", {"week": 1})
    S._get("/player/x", {"week": 2})
    assert len(calls) == 2


def test_cache_can_be_bypassed_and_cleared(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return {}

    class FakeClient:
        def get(self, url, params=None):
            calls.append(url)
            return FakeResponse()

    monkeypatch.setattr(S, "_http", lambda: FakeClient())
    S._get("/health")
    S._get("/health", use_cache=False)
    assert len(calls) == 2
    S._get("/health")          # served from the entry written by the first call
    assert len(calls) == 2
    S.clear_cache()
    S._get("/health")
    assert len(calls) == 3


def test_expired_entries_are_refetched(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return {}

    class FakeClient:
        def get(self, url, params=None):
            calls.append(url)
            return FakeResponse()

    monkeypatch.setattr(S, "_http", lambda: FakeClient())
    clock = {"t": 1000.0}
    monkeypatch.setattr(S.time, "monotonic", lambda: clock["t"])

    S._get("/health")                 # /health TTL is 10s
    clock["t"] += 5
    S._get("/health")
    assert len(calls) == 1
    clock["t"] += 20
    S._get("/health")
    assert len(calls) == 2, "an entry past its TTL must be refetched"


def test_ttls_are_shorter_for_data_that_moves():
    # A player's projection changes during a game week; the schedule does not.
    assert S._ttl_for("/player/00-0037834") < S._ttl_for("/schedule/1")
    assert S._ttl_for("/matchup/1/SF/LA") < S._ttl_for("/current_week")
    assert S._ttl_for("/something/unknown") == S._DEFAULT_TTL


def test_errors_are_not_cached(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 500
        text = "boom"

        def json(self):
            raise ValueError

    class FakeClient:
        def get(self, url, params=None):
            calls.append(url)
            return FakeResponse()

    monkeypatch.setattr(S, "_http", lambda: FakeClient())
    for _ in range(2):
        with pytest.raises(S.BackendError):
            S._get("/player/x")
    assert len(calls) == 2, "a failure must not be cached as if it were an answer"


def test_unreachable_backend_names_the_url(monkeypatch):
    import httpx

    class FakeClient:
        def get(self, url, params=None):
            raise httpx.ConnectError("refused")

    monkeypatch.setattr(S, "_http", lambda: FakeClient())
    with pytest.raises(S.BackendError) as exc:
        S._get("/health")
    assert S.API_BASE in str(exc.value)
    assert "Is it running?" in str(exc.value)


def test_client_is_reused_across_calls():
    # A new client per call meant a new TCP handshake per call.
    assert S._http() is S._http()


# --------------------------------------------------------------------------
# Launcher
# --------------------------------------------------------------------------

def test_launcher_has_no_absolute_paths():
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "run_mcp_server.py")
    source = open(path, encoding="utf-8").read()
    # Everything resolves from the file's own location, so the checkout can move.
    assert "C:/dev" not in source and "C:\\\\dev" not in source
    assert "HERE = os.path.dirname(os.path.abspath(__file__))" in source


def test_launcher_uses_an_inheriting_subprocess_not_exec():
    """os.exec* on Windows kills this process and severs the client's pipes."""
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "run_mcp_server.py")
    source = open(path, encoding="utf-8").read()
    assert "subprocess.run" in source
    assert "os.execve" not in source


def test_project_config_is_portable():
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg_path = os.path.join(repo, ".mcp.json")
    if not os.path.exists(cfg_path):
        pytest.skip(".mcp.json not present")
    import json
    cfg = json.load(open(cfg_path, encoding="utf-8"))["mcpServers"]["football-ai"]
    # Relative to the repo root, so the config works on any checkout.
    assert not os.path.isabs(cfg["command"])
    assert all(not os.path.isabs(a) for a in cfg["args"])
