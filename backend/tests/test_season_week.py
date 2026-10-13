"""The week rule the injury snapshots are tagged with: from the schedule, never a fallback of 18."""
import os
import sys
from datetime import date

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rag_data"))

from season_week import active_week  # noqa: E402


def _sched(rows):
    return pl.DataFrame(rows, schema=["week", "gameday", "home_score"], orient="row")


SCHEDULE = _sched([
    (5, "2026-10-11", 20), (5, "2026-10-12", None),   # Monday night not final yet
    (6, "2026-10-15", None), (6, "2026-10-18", None),
    (7, "2026-10-25", None),
])


def test_the_first_week_with_a_game_still_to_play():
    assert active_week(SCHEDULE, date(2026, 10, 13)) == 6
    assert active_week(SCHEDULE, date(2026, 10, 12)) == 5     # Monday: the late game is still today's


def test_moves_on_once_every_game_of_the_week_has_a_score():
    done = _sched([(5, "2026-10-11", 20), (5, "2026-10-12", 31), (6, "2026-10-15", None)])
    assert active_week(done, date(2026, 10, 12)) == 6


def test_after_the_last_game_it_stays_on_the_last_week_not_a_made_up_one():
    season_over = _sched([(17, "2027-01-03", 20), (18, "2027-01-10", 17)])
    assert active_week(season_over, date(2027, 2, 1)) == 18
    assert active_week(_sched([(1, "2026-09-10", None)]), date(2026, 7, 1)) == 1


def test_no_schedule_means_no_answer_not_a_guess():
    assert active_week(pl.DataFrame(), date(2026, 10, 13)) is None
    assert active_week(_sched([]), date(2026, 10, 13)) is None


def test_the_app_finds_the_week_at_startup_before_its_own_state_exists():
    """The first injury-map build ran before current_nfl_week was set and fell back to max(week) = 18."""
    from applications.api.config import CURRENT_SEASON
    from applications.api.services.data_loader import _week_from_schedule

    future = [(6, "2999-01-01", None, CURRENT_SEASON), (7, "2999-01-08", None, CURRENT_SEASON), (18, "2999-03-01", None, CURRENT_SEASON)]
    sched = pl.DataFrame(future, schema=["week", "gameday", "home_score", "season"], orient="row")
    assert _week_from_schedule(sched) == 6
    assert _week_from_schedule(None) is None


def test_scheduled_jobs_tolerate_a_slow_start():
    """APScheduler's default grace is 1 s: on the small server a late job was silently skipped."""
    import inspect

    from applications.api import main

    source = inspect.getsource(main)
    assert "misfire_grace_time" in source and "BackgroundScheduler(job_defaults" in source
