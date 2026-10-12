"""The ETL's closing verdict: a required step that fails must fail the run.

The orchestrator used to print "ETL PIPELINE COMPLETE" and exit 0 whatever happened, so a
step that wrote nothing looked like a success and the data went stale unnoticed.
"""
import importlib.util
from pathlib import Path

import pytest

ETL_PATH = Path(__file__).resolve().parents[1] / "rag_data" / "05_etl_to_postgres.py"


@pytest.fixture()
def etl(monkeypatch):
    monkeypatch.setenv("DB_CONNECTION_STRING", "postgresql://u:p@localhost:5432/none")
    spec = importlib.util.spec_from_file_location("etl_under_test", ETL_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


STEPS = [
    {"script": "02_update_weekly_stats.py", "uploads": []},
    {"script": "10b_bovada_api_props.py", "optional": True, "uploads": []},
]


def test_all_steps_ok_exits_zero(etl, capsys):
    etl.OUTCOMES.update({"02_update_weekly_stats.py": "ok", "10b_bovada_api_props.py": "ok"})
    assert etl.summarize(STEPS) == 0
    assert "ETL PIPELINE COMPLETE" in capsys.readouterr().out


def test_a_failed_required_step_fails_the_run(etl, capsys):
    etl.OUTCOMES.update({"02_update_weekly_stats.py": "failed", "10b_bovada_api_props.py": "ok"})
    assert etl.summarize(STEPS) == 1
    out = capsys.readouterr().out
    assert "ETL INCOMPLETE" in out and "02_update_weekly_stats.py" in out


def test_a_required_step_whose_table_did_not_load_fails_the_run(etl):
    etl.OUTCOMES.update({"02_update_weekly_stats.py": "ok", "10b_bovada_api_props.py": "ok"})
    etl.UPLOAD_PROBLEMS["02_update_weekly_stats.py"] = ["weekly_player_stats_2026"]
    assert etl.summarize(STEPS) == 1


def test_an_unavailable_optional_source_only_degrades(etl, capsys):
    etl.OUTCOMES.update({"02_update_weekly_stats.py": "ok", "10b_bovada_api_props.py": "failed"})
    assert etl.summarize(STEPS) == 0
    assert "DEGRADED" in capsys.readouterr().out


def test_a_step_that_never_ran_is_a_failure(etl):
    assert etl.summarize(STEPS) == 1
