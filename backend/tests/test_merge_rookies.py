"""Regression guard for `_merge_rows_into_profile`.

The original bug: the function constrained incoming rookie rows to df_profile's
column set, silently dropping fields the profile didn't have (notably
`draft_number`). The CSV-based source of truth was preserved on disk but every
rookie pick was nulled after merge, so the tier list showed all rookies as UDFA.

This test pins the contract — any future regression that re-introduces a
column-stripping merge will fail here, not in production.
"""
import polars as pl
import pytest

from applications.api.routes.tier_list import _merge_rows_into_profile
from applications.api.state import model_data


@pytest.fixture
def reset_model_data():
    """Snapshot model_data so tests don't leak into each other or the running app."""
    snapshot = dict(model_data)
    yield
    model_data.clear()
    model_data.update(snapshot)


def _veteran_profile() -> pl.DataFrame:
    """Mimic the historical pre-schema-fix profile: no `draft_number` column."""
    return pl.DataFrame(
        {
            "player_id": ["vet-1"],
            "player_name": ["Existing Vet"],
            "position": ["RB"],
            "team_abbr": ["KC"],
            "draft_year": [2018],
            "season": [2026],
        }
    )


def _rookie_rows() -> list[dict]:
    """ESPN-shaped rookie rows that carry a draft_number the profile lacks."""
    return [
        {
            "player_id": "rookie-1",
            "player_name": "New Rookie",
            "position": "WR",
            "team_abbr": "DAL",
            "draft_year": 2026,
            "draft_number": 7,
            "season": 2026,
        },
        {
            "player_id": "rookie-2",
            "player_name": "Undrafted Rookie",
            "position": "QB",
            "team_abbr": "MIA",
            "draft_year": 2026,
            "draft_number": None,
            "season": 2026,
        },
    ]


def test_merge_preserves_draft_number_when_profile_lacks_column(reset_model_data):
    """The original bug: rookie draft picks were dropped because the profile had no
    `draft_number` column. The merge must surface that column instead of stripping it.
    """
    model_data["df_profile"] = _veteran_profile()
    assert "draft_number" not in model_data["df_profile"].columns

    added = _merge_rows_into_profile(_rookie_rows())
    assert added == 2

    profile = model_data["df_profile"]
    assert "draft_number" in profile.columns, (
        "draft_number must be added to df_profile when rookie rows carry it"
    )

    rookie_row = profile.filter(pl.col("player_id") == "rookie-1").row(0, named=True)
    assert rookie_row["draft_number"] == 7

    udfa_row = profile.filter(pl.col("player_id") == "rookie-2").row(0, named=True)
    assert udfa_row["draft_number"] is None

    vet_row = profile.filter(pl.col("player_id") == "vet-1").row(0, named=True)
    assert vet_row["draft_number"] is None  # vet legitimately has no pick info


def test_merge_dedupes_by_player_name(reset_model_data):
    """A rookie whose name already exists in df_profile is skipped (no duplicate row)."""
    base = pl.DataFrame(
        {
            "player_id": ["dup"],
            "player_name": ["New Rookie"],  # same name as one of the rookie rows
            "position": ["WR"],
            "team_abbr": ["DAL"],
            "draft_year": [2026],
            "season": [2026],
        }
    )
    model_data["df_profile"] = base

    added = _merge_rows_into_profile(_rookie_rows())
    assert added == 1  # only "Undrafted Rookie" is new

    names = model_data["df_profile"]["player_name"].to_list()
    assert names.count("New Rookie") == 1


def test_merge_preserves_arbitrary_extra_column(reset_model_data):
    """The fix is structural — any new column on rookie rows, not just draft_number,
    must survive the merge. Guards against future regressions of the same shape.
    """
    model_data["df_profile"] = _veteran_profile()
    rows = _rookie_rows()
    rows[0]["college"] = "Notre Dame"  # column profile has never seen

    _merge_rows_into_profile(rows)

    profile = model_data["df_profile"]
    assert "college" in profile.columns
    assert (
        profile.filter(pl.col("player_id") == "rookie-1").row(0, named=True)["college"]
        == "Notre Dame"
    )
