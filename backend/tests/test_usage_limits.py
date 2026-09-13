"""Daily limits for the free assistant: per browser, per IP, owner bypass, and the shared upstream budget."""

import os
import uuid
from datetime import date

import pytest

from applications.api.services import usage_limits
from applications.api.services.usage_limits import UsageLimiter


@pytest.fixture
def small_limits(monkeypatch):
    monkeypatch.setenv("AGENT_DAILY_QUESTIONS", "3")
    monkeypatch.setenv("AGENT_DAILY_QUESTIONS_PER_IP", "5")
    monkeypatch.setenv("INFERENCE_HOUSE_DAILY_REQUEST_BUDGET", "4")


def _memory() -> UsageLimiter:
    return UsageLimiter(uri=None)


def test_a_browser_gets_its_daily_questions_then_stops(small_limits):
    lim = _memory()
    results = [lim.admit_question("browser-a", "1.1.1.1", owner=False) for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert [r.remaining for r in results[:3]] == [2, 1, 0]
    assert results[3].blocked_by == "client"
    assert results[3].resets_at.endswith("-04:00") or results[3].resets_at.endswith("-05:00"), "resets on US Eastern midnight"


def test_clearing_site_data_does_not_reset_the_ip_cap(small_limits):
    """A new browser id per request still hits the per-IP ceiling."""
    lim = _memory()
    allowed = [lim.admit_question(f"fresh-browser-{i}", "2.2.2.2", owner=False).allowed for i in range(7)]
    assert allowed == [True] * 5 + [False] * 2


def test_an_ip_refusal_does_not_cost_the_browser_a_question(small_limits):
    lim = _memory()
    for i in range(5):
        lim.admit_question(f"other-{i}", "3.3.3.3", owner=False)
    blocked = lim.admit_question("victim-browser", "3.3.3.3", owner=False)
    assert blocked.allowed is False and blocked.blocked_by == "ip"
    assert lim.quota("victim-browser", owner=False).used == 0


def test_the_owner_is_never_limited_and_not_counted(small_limits):
    lim = _memory()
    results = [lim.admit_question("owner-browser", "4.4.4.4", owner=True) for _ in range(10)]
    assert all(r.allowed and r.owner for r in results)
    # ...and the owner's questions don't eat the IP allowance of others behind the same address.
    assert lim.admit_question("roommate", "4.4.4.4", owner=False).allowed


def test_failed_questions_are_refunded(small_limits):
    lim = _memory()
    lim.admit_question("browser-r", "5.5.5.5", owner=False)
    lim.refund_question("browser-r", "5.5.5.5")
    assert lim.quota("browser-r", owner=False).used == 0


def test_house_budget_is_shared_across_everyone(small_limits):
    lim = _memory()
    assert [lim.take_house_call() for _ in range(5)] == [True, True, True, True, False]
    assert lim.house_budget_remaining() == 0


def test_counters_roll_over_at_midnight(small_limits, monkeypatch):
    lim = _memory()
    monkeypatch.setattr(usage_limits, "today", lambda: date(2026, 9, 12))
    for _ in range(3):
        lim.admit_question("browser-d", None, owner=False)
    assert not lim.admit_question("browser-d", None, owner=False).allowed
    monkeypatch.setattr(usage_limits, "today", lambda: date(2026, 9, 13))
    assert lim.admit_question("browser-d", None, owner=False).allowed


def test_a_database_failure_degrades_to_memory_instead_of_blocking(small_limits):
    """Abuse protection getting weaker for a minute beats the assistant going offline."""
    lim = UsageLimiter(uri="postgresql://nobody:nothing@127.0.0.1:1/none")
    assert lim.admit_question("browser-db", None, owner=False).allowed


# --- Postgres, when available ------------------------------------------------------

DB = os.getenv("DB_CONNECTION_STRING", "postgresql://admin:password@localhost:5432/football_ai")


def _pg_available() -> bool:
    try:
        from sqlalchemy import create_engine, text

        with create_engine(DB).connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable")
def test_postgres_limit_holds_under_concurrency(small_limits):
    """Two Cloud Run instances racing for the last question must not both win."""
    from concurrent.futures import ThreadPoolExecutor

    from sqlalchemy import create_engine, text

    subject_client = f"pytest-{uuid.uuid4().hex[:12]}"
    lim = UsageLimiter(uri=DB)
    try:
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: lim.admit_question(subject_client, None, False).allowed, range(12)))
        assert sum(results) == 3
        # A second limiter (another "instance") sees the same count.
        assert UsageLimiter(uri=DB).quota(subject_client, owner=False).used == 3
    finally:
        with create_engine(DB).begin() as conn:
            conn.execute(text("DELETE FROM agent_usage WHERE subject = :s"), {"s": f"client:{subject_client}"})
