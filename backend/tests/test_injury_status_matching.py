"""Injury statuses: never borrowed from a namesake, and the refresh reaches the app.

Kyle Williams (NE WR, active) showed as Inactive: Sleeper has no gsis id for him
and lists two retired Kyle Williams, and a name-only match let one of them win.
Separately, the half-hourly refresh wrote a CSV the app never read, while the
database kept 15 conflicting snapshots per player.
"""

import importlib.util
import os
import uuid
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "rag_data" / "08_update_injuries.py"
spec = importlib.util.spec_from_file_location("update_injuries", SCRIPT)
injuries = importlib.util.module_from_spec(spec)
spec.loader.exec_module(injuries)

PROFILES = [
    {"player_id": "00-0040131", "player_name": "Kyle Williams", "team_abbr": "NE"},
    {"player_id": "00-rams", "player_name": "Puka Nacua", "team_abbr": "LA"},
    {"player_id": "00-allen-qb", "player_name": "Josh Allen", "team_abbr": "BUF", "position": "QB"},
    {"player_id": "00-allen-lb", "player_name": "Josh Allen", "team_abbr": "JAX", "position": "LB"},
]


def statuses(sleeper):
    return {r["player_id"]: r["sleeper_status"] for r in injuries.match_statuses(sleeper, PROFILES)}


def test_a_retired_namesake_cannot_override_an_active_player():
    got = statuses({
        "7437": {"full_name": "Kyle Williams", "team": None, "status": "Inactive"},
        "12547": {"full_name": "Kyle Williams", "team": "NE", "status": "Active"},
        "638": {"full_name": "Kyle Williams", "team": None, "status": "Inactive"},
    })
    assert got == {"00-0040131": "Active"}


def test_name_matches_must_agree_on_team():
    got = statuses({"1": {"full_name": "Kyle Williams", "team": "BUF", "injury_status": "Out"}})
    assert got == {}


def test_team_aliases_are_understood():
    got = statuses({"2": {"full_name": "Puka Nacua", "team": "LAR", "injury_status": "Questionable"}})
    assert got == {"00-rams": "Questionable"}


def test_a_name_two_of_our_players_share_needs_a_position():
    assert statuses({"3": {"full_name": "Josh Allen", "team": "BUF", "injury_status": "Out"}}) == {}
    got = statuses({
        "3": {"full_name": "Josh Allen", "position": "QB", "team": None, "injury_status": "Questionable"},
        "3b": {"full_name": "Josh Allen", "position": "OLB", "team": "JAX", "injury_status": "Out"},
    })
    assert got == {"00-allen-qb": "Questionable", "00-allen-lb": "Out"}


def test_a_unique_name_matches_even_without_a_team_in_sleeper():
    """Dalys Beanum (NO DB, Questionable) has no team in Sleeper; that is common and fine."""
    got = statuses({"8": {"full_name": "Puka Nacua", "team": None, "injury_status": "Questionable"}})
    assert got == {"00-rams": "Questionable"}


def test_stray_whitespace_in_sleeper_ids_still_matches_directly():
    got = statuses({"9": {"full_name": "Someone Else", "gsis_id": " 00-0040131 ", "injury_status": "IR"}})
    assert got == {"00-0040131": "IR"}


def test_a_direct_id_beats_any_name_match():
    got = statuses({
        "4": {"full_name": "Kyle Williams", "team": "NE", "status": "Inactive"},
        "5": {"full_name": "Kyle Williams Jr", "gsis_id": "00-0040131", "status": "Active"},
    })
    assert got == {"00-0040131": "Active"}


def test_without_a_team_the_one_active_record_wins():
    """Devin Neal (NO RB): two Sleeper records, neither with a team, one active."""
    got = statuses({
        "10": {"full_name": "Kyle Williams", "team": None, "active": True, "injury_status": "Questionable"},
        "11": {"full_name": "Kyle Williams", "team": None, "active": False, "status": "Inactive"},
    })
    assert got == {"00-0040131": "Questionable"}


def test_candidates_that_agree_are_trusted():
    got = statuses({
        "12": {"full_name": "Kyle Williams", "team": None, "injury_status": "Questionable"},
        "13": {"full_name": "Kyle Williams", "team": None, "injury_status": "Questionable"},
    })
    assert got == {"00-0040131": "Questionable"}


def test_two_rostered_name_matches_are_ambiguous():
    got = statuses({
        "6": {"full_name": "Kyle Williams", "team": "NE", "status": "Active"},
        "7": {"full_name": "Kyle Williams", "team": "NE", "injury_status": "Out"},
    })
    assert got == {}


# --- the refresh reaches the database ---------------------------------------------------

@pytest.fixture
def pg_schema():
    uri = os.getenv("DB_CONNECTION_STRING")
    if not uri:
        pytest.skip("needs Postgres")
    psycopg2 = pytest.importorskip("psycopg2")
    schema = f"test_inj_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(uri)
    conn.autocommit = True
    conn.cursor().execute(f"CREATE SCHEMA {schema}")
    sep = "&" if "?" in uri else "?"
    yield f"{uri}{sep}options=-csearch_path%3D{schema}", conn, schema
    conn.cursor().execute(f"DROP SCHEMA {schema} CASCADE")
    conn.close()


def test_refresh_replaces_stale_duplicate_snapshots(pg_schema, tmp_path):
    from applications.api.config import CURRENT_SEASON
    from applications.api.services.etl import sync_injuries_to_db

    uri, conn, schema = pg_schema
    table = f"{schema}.weekly_injuries_{CURRENT_SEASON}"
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} (player_id TEXT, player_name TEXT, injury_status TEXT, week BIGINT)")
    stale = [("00-0040131", "Kyle Williams", "Inactive", 1)] * 15 + [("00-x", "X", "Questionable", 1)] * 3 + [("00-x", "X", "Active", 1)] * 3
    cur.executemany(f"INSERT INTO {table} VALUES (%s,%s,%s,%s)", stale)
    cur.execute(f"INSERT INTO {table} VALUES ('00-old','Old','Out',0)")

    csv = tmp_path / "inj.csv"
    csv.write_text("player_id,player_name,injury_status,week\n00-0040131,Kyle Williams,Active,1\n00-x,X,Out,1\n")
    assert sync_injuries_to_db(str(csv), uri) == 2

    cur.execute(f"SELECT player_id, injury_status, week FROM {table} ORDER BY week, player_id")
    assert cur.fetchall() == [("00-old", "Out", 0), ("00-0040131", "Active", 1), ("00-x", "Out", 1)]


def test_pipeline_scripts_run_under_the_event_loop_uvicorn_reload_uses(tmp_path):
    """Windows + uvicorn --reload runs a SelectorEventLoop, which can't spawn subprocesses."""
    import asyncio
    import sys

    from applications.api.services import etl

    script = tmp_path / "ok.py"
    script.write_text("import sys; print('ran'); sys.exit(3)")
    loop = asyncio.SelectorEventLoop() if sys.platform == "win32" else asyncio.new_event_loop()
    try:
        code, out, _ = loop.run_until_complete(etl.run_script(str(script)))
    finally:
        loop.close()
    assert code == 3 and b"ran" in out


def test_scripts_run_with_the_venv_interpreter_not_the_base_install(monkeypatch, tmp_path):
    import sys

    from applications.api.services import etl

    venv = tmp_path / "venv"
    (venv / "Scripts").mkdir(parents=True)
    (venv / "Scripts" / "python.exe").write_text("")
    monkeypatch.setattr(sys, "prefix", str(venv))
    monkeypatch.setattr(sys, "base_prefix", str(tmp_path / "base"))
    assert etl.python_for_scripts() == str(venv / "Scripts" / "python.exe")
