"""Roster overrides must never outrank fresher upstream data.

Two production bugs motivate these tests. Kenneth Walker III stayed on SEA in-app
for months after being traded to KC, and DJ Moore was pinned to CHI by an entry
asserting the depth-chart feed was wrong about BUF -- the feed was right. In both
cases a hardcoded correction silently beat a feed that had already caught up.
"""

import polars as pl
import pytest

from applications.api.state import model_data
from applications.api.services import data_loader as dl


def _depth(rows, observed="2026-09-11T12:21:50Z"):
    return pl.DataFrame(
        [{"dt": observed, "gsis_id": pid, "team": team} for pid, team in rows]
    )


@pytest.fixture(autouse=True)
def _restore_state():
    saved = {k: model_data.get(k) for k in ("df_profile", "df_depth_charts")}
    yield
    for k, v in saved.items():
        if v is None:
            model_data.pop(k, None)
        else:
            model_data[k] = v


def test_parse_date_handles_iso_timestamps_and_plain_dates():
    # A format string ending in "Z" applied to a 19-char slice matches nothing,
    # which silently disabled every timestamp comparison below.
    assert dl._parse_date("2026-09-11T12:21:50Z").isoformat() == "2026-09-11"
    assert dl._parse_date("2026-05-23").isoformat() == "2026-05-23"
    assert dl._parse_date(None) is None
    assert dl._parse_date("not-a-date") is None


def test_override_retires_when_feed_is_newer(monkeypatch):
    monkeypatch.setattr(
        dl, "CURRENT_TEAM_OVERRIDES_DATED", {"00-0034827": ("CHI", "2026-05-23")}
    )
    active = dl._active_overrides(_depth([("00-0034827", "BUF")]))
    assert active == {}, "a months-old override must yield to today's feed"


def test_override_wins_when_newer_than_feed(monkeypatch):
    monkeypatch.setattr(
        dl, "CURRENT_TEAM_OVERRIDES_DATED", {"00-0099999": ("KC", "2026-09-12")}
    )
    active = dl._active_overrides(
        _depth([("00-0099999", "SEA")], observed="2026-09-11T12:21:50Z")
    )
    assert active == {"00-0099999": "KC"}, "a confirmed move newer than the feed still applies"


def test_override_applies_when_feed_has_no_row(monkeypatch):
    monkeypatch.setattr(
        dl, "CURRENT_TEAM_OVERRIDES_DATED", {"00-0099999": ("KC", "2026-01-01")}
    )
    active = dl._active_overrides(_depth([("00-0000001", "SEA")]))
    assert active == {"00-0099999": "KC"}, "with no feed opinion the override is all we have"


def test_no_overrides_applied_before_feed_is_loaded(monkeypatch):
    # refresh_db_data() runs this once before depth charts exist. Applying an
    # override there writes a stale team into df_profile that the later pass
    # cannot undo -- the bug that kept DJ Moore on CHI after retirement.
    monkeypatch.setattr(
        dl, "CURRENT_TEAM_OVERRIDES_DATED", {"00-0034827": ("CHI", "2026-05-23")}
    )
    assert dl._active_overrides(pl.DataFrame()) == {}


def test_profile_survives_override_pass_with_no_feed(monkeypatch):
    monkeypatch.setattr(
        dl, "CURRENT_TEAM_OVERRIDES_DATED", {"00-0034827": ("CHI", "2026-05-23")}
    )
    model_data["df_profile"] = pl.DataFrame(
        [{"player_id": "00-0034827", "player_name": "DJ Moore",
          "team_abbr": "BUF", "status": "ACT"}]
    )
    model_data["df_depth_charts"] = pl.DataFrame()

    dl.apply_current_roster_overrides()

    assert model_data["df_profile"]["team_abbr"][0] == "BUF"


def test_redundant_override_is_dropped(monkeypatch):
    monkeypatch.setattr(
        dl, "CURRENT_TEAM_OVERRIDES_DATED", {"00-0038134": ("KC", "2026-09-10")}
    )
    active = dl._active_overrides(_depth([("00-0038134", "KC")]))
    assert active == {}, "an override the feed already agrees with is dead weight"


def test_apply_leaves_fresh_feed_untouched(monkeypatch):
    monkeypatch.setattr(
        dl, "CURRENT_TEAM_OVERRIDES_DATED", {"00-0034827": ("CHI", "2026-05-23")}
    )
    model_data["df_profile"] = pl.DataFrame(
        [{"player_id": "00-0034827", "player_name": "DJ Moore",
          "team_abbr": "BUF", "status": "ACT"}]
    )
    model_data["df_depth_charts"] = _depth([("00-0034827", "BUF")])

    dl.apply_current_roster_overrides()

    assert model_data["df_profile"]["team_abbr"][0] == "BUF"
    assert model_data["df_depth_charts"]["team"][0] == "BUF"


def test_audit_flags_active_mismatches_only():
    model_data["df_profile"] = pl.DataFrame(
        [
            {"player_id": "a", "player_name": "Starter", "team_abbr": "CHI", "status": "ACT"},
            {"player_id": "b", "player_name": "Camp Body", "team_abbr": "CHI", "status": "DEV"},
        ]
    )
    model_data["df_depth_charts"] = _depth([("a", "BUF"), ("b", "GB")])

    result = dl.audit_roster_consistency()

    assert result["checked"] == 2
    # Practice-squad churn is noise; only the active player counts.
    assert result["active_mismatches"] == 1
    assert result["players"][0]["player_name"] == "Starter"
