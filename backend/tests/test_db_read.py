"""Database reads must survive pyarrow being blocked.

On 2026-09-12 Windows Application Control blocked pyarrow's DLL after a reboot.
Every read went through `pl.read_database_uri`, which needs Arrow, and every
caller swallowed the exception, so the API came up "healthy" with empty tables
and served stale data with no error anywhere.

These tests pin the fix: reads switch to a pyarrow-free path, say so loudly,
keep other errors intact, and nobody can quietly reintroduce a direct Arrow read.
"""

import os
import re
from pathlib import Path

import polars as pl
import pytest

import db_read

BACKEND = Path(__file__).resolve().parents[1]
BLOCKED = ImportError(
    "DLL load failed while importing lib: An Application Control policy has blocked this file."
)


@pytest.fixture(autouse=True)
def _fresh_state():
    db_read._reset_for_tests()
    yield
    db_read._reset_for_tests()


def test_a_blocked_pyarrow_switches_to_the_fallback(monkeypatch):
    calls = {"arrow": 0, "fallback": 0}

    def arrow(query, uri):
        calls["arrow"] += 1
        raise BLOCKED

    def fallback(query, uri):
        calls["fallback"] += 1
        return pl.DataFrame({"n": [2962]})

    monkeypatch.setattr(db_read.pl, "read_database_uri", arrow)
    monkeypatch.setattr(db_read, "read_without_arrow", fallback)

    assert db_read.read_db_uri("SELECT 1", "postgresql://x").item() == 2962
    assert db_read.status()["db_read_path"] == "sqlalchemy-fallback"
    assert "Application Control" in db_read.status()["arrow_unavailable_reason"]

    # Sticky: later reads skip the Arrow attempt instead of failing and retrying every time.
    db_read.read_db_uri("SELECT 1", "postgresql://x")
    assert calls == {"arrow": 1, "fallback": 2}


def test_the_switch_is_logged_as_an_error(monkeypatch, caplog):
    """The original failure was silent. The replacement must not be."""
    monkeypatch.setattr(db_read.pl, "read_database_uri", lambda q, u: (_ for _ in ()).throw(BLOCKED))
    monkeypatch.setattr(db_read, "read_without_arrow", lambda q, u: pl.DataFrame())
    with caplog.at_level("ERROR", logger="football-ai"):
        db_read.read_db_uri("SELECT 1", "postgresql://x")
    assert any("pyarrow cannot be loaded" in r.message and r.levelname == "ERROR" for r in caplog.records)


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError('relation "weekly_player_stats_2031" does not exist'),
        RuntimeError("connection refused"),
        ImportError("No module named 'sentence_transformers'"),
    ],
)
def test_other_failures_are_not_hidden(monkeypatch, error):
    """A missing table or a down database must still raise. Callers rely on that,
    and switching drivers would not fix either one."""
    monkeypatch.setattr(db_read.pl, "read_database_uri", lambda q, u: (_ for _ in ()).throw(error))
    with pytest.raises(type(error)):
        db_read.read_db_uri("SELECT 1", "postgresql://x")
    assert db_read.status()["db_read_path"] == "arrow"


def test_startup_probe_reports_a_block(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name == "pyarrow":
            raise BLOCKED
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    assert db_read.probe_arrow() is False
    assert db_read.status()["db_read_path"] == "sqlalchemy-fallback"


def test_nothing_reads_the_database_around_the_fallback():
    """Any direct `pl.read_database_uri` bypasses the fix, and would fail silently
    again the next time pyarrow is blocked. Route it through read_db / read_db_uri."""
    allowed = {BACKEND / "db_read.py", BACKEND / "rag_data" / "debug_injury_file.py"}
    offenders = []
    for folder in (BACKEND / "applications", BACKEND / "rag_data", BACKEND / "mcp_server", BACKEND / "agent"):
        for path in folder.rglob("*.py"):
            if path in allowed or ".venv" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for n, line in enumerate(text.splitlines(), 1):
                if re.search(r"\bread_database_uri\s*\(", line) and not line.lstrip().startswith("#"):
                    offenders.append(f"{path.relative_to(BACKEND)}:{n}")
    assert not offenders, "direct Arrow reads found: " + ", ".join(offenders)


# --- against the real database, when one is running ---------------------------

DB = os.getenv("DB_CONNECTION_STRING", "postgresql://admin:password@localhost:5432/football_ai")


def _db_reachable() -> bool:
    try:
        db_read.read_without_arrow("SELECT 1", DB)
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _db_reachable(), reason="Postgres not reachable")
@pytest.mark.parametrize("table", ["player_profiles", "schedule", "bovada_game_lines", "weekly_feature_set_2026"])
def test_fallback_matches_the_arrow_path_on_real_tables(table):
    """Same shape and same dtypes, including columns that are entirely NULL: the row
    path types those as pl.Null unless it looks up the declared type."""
    pytest.importorskip("pyarrow")
    query = f'SELECT * FROM "{table}" LIMIT 3000'
    arrow = pl.read_database_uri(query, DB)
    fallback = db_read.read_without_arrow(query, DB)

    assert fallback.shape == arrow.shape
    mismatched = {c: (arrow.schema[c], fallback.schema[c]) for c in arrow.columns if arrow.schema[c] != fallback.schema[c]}
    assert not mismatched, mismatched


@pytest.mark.skipif(not _db_reachable(), reason="Postgres not reachable")
def test_fallback_handles_empty_results_and_like_patterns():
    empty = db_read.read_without_arrow("SELECT player_id FROM player_profiles WHERE false", DB)
    assert empty.height == 0
    assert empty.schema == {"player_id": pl.String}

    # '%' must reach Postgres as a literal. psycopg2 only treats it as a placeholder when parameters are passed.
    liked = db_read.read_without_arrow("SELECT count(*) AS n FROM player_profiles WHERE player_name LIKE '%Purdy%'", DB)
    assert liked.item() >= 1
