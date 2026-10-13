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


class TestRefreshWhenNew:
    """The 06:00 run can land before results are published, so a cheap hourly check decides."""

    def _run(self, monkeypatch, new, ran_recently=False, scripts_running=False):
        import asyncio

        from applications.api.services import etl

        calls = []

        async def fake_etl(restart_after=True):
            calls.append(restart_after)

        monkeypatch.setattr(etl, "new_final_scores_upstream", lambda *_a, **_k: new)
        monkeypatch.setattr(etl, "etl_ran_recently", lambda hours=None: ran_recently)
        monkeypatch.setattr(etl, "run_daily_etl_async", fake_etl)
        monkeypatch.setattr(etl, "_running_scripts", {object()} if scripts_running else set())
        return asyncio.run(etl.run_new_results_etl_async()), calls

    def test_runs_the_etl_only_when_there_are_unloaded_results(self, monkeypatch):
        assert self._run(monkeypatch, ["2026_05_BUF_LA"]) == (True, [False])
        assert self._run(monkeypatch, []) == (False, [])

    def test_does_not_rerun_soon_after_a_run_or_while_a_script_is_running(self, monkeypatch):
        assert self._run(monkeypatch, ["g"], ran_recently=True) == (False, [])
        assert self._run(monkeypatch, ["g"], scripts_running=True) == (False, [])

    def test_a_failed_check_is_not_an_etl_run(self, monkeypatch):
        import asyncio

        from applications.api.services import etl

        def boom(*_a, **_k):
            raise RuntimeError("feed down")

        monkeypatch.setattr(etl, "new_final_scores_upstream", boom)
        monkeypatch.setattr(etl, "etl_ran_recently", lambda hours=None: False)
        monkeypatch.setattr(etl, "_running_scripts", set())
        assert asyncio.run(etl.run_new_results_etl_async()) is False


def test_the_injury_refresh_stands_down_while_the_full_etl_runs(monkeypatch):
    """Two writers on the injuries table left every player in it twice."""
    import asyncio

    from applications.api.services import etl

    ran = []

    async def fake_run(*_a, **_k):
        ran.append(1)
        return 0, b"", b""

    monkeypatch.setattr(etl, "run_script", fake_run)
    monkeypatch.setattr(etl, "_running_scripts", {object()})
    assert asyncio.run(etl.run_injury_refresh_async()) is False and ran == []
