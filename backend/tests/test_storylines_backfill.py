"""Every player's profile should open on storylines, not "No storylines yet".

The league feed alone reached 94 of 2,962 players. These tests pin the per-player
backfill that closes that gap, and the guard that stops a shared name from
attaching another player's news. ESPN is replaced with canned responses, so no
network is touched.
"""

import polars as pl
import pytest

from applications.api.services import storylines as st
from applications.api.state import model_data


def _item(i, published, headline=None, premium=False, description=None, kind="Rotowire"):
    headline = headline or f"Update {i}"
    return {
        "id": 1000 + i,
        "type": kind,
        "headline": headline,
        "description": headline if description is None else description,
        "published": published,
        "premium": premium,
        "links": {"mobile": {"href": f"http://m.espn.go.com/story?storyId={1000 + i}"}},
        "images": [],
    }


@pytest.fixture
def espn(monkeypatch, tmp_path):
    """Canned ESPN: per-player feeds and search results, recording every call."""
    calls = []
    feeds = {}
    search = {}

    def fake_get_json(url):
        calls.append(url)
        if url.startswith(st.PLAYER_NEWS_URL):
            espn_id = url.split("playerId=")[1].split("&")[0]
            if espn_id == "boom":
                raise TimeoutError("ESPN down")
            return {"feed": feeds.get(espn_id, [])}
        if url.startswith(st.ESPN_SEARCH_URL):
            return {"items": search.get("items", [])}
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(st, "_get_json", fake_get_json)
    monkeypatch.setattr(st, "STORYLINES_CSV", str(tmp_path / "storylines.csv"))
    monkeypatch.setattr(st, "_backfilled_at", {})
    saved = {k: model_data.get(k) for k in ("df_storylines", "espn_to_gsis", "gsis_to_espn", "espn_id_search_cache", "df_profile")}
    model_data["espn_to_gsis"] = {"4361741": "00-purdy"}
    model_data["gsis_to_espn"] = {"00-purdy": "4361741"}
    model_data["espn_id_search_cache"] = {}
    model_data["df_storylines"] = None
    model_data["df_profile"] = pl.DataFrame([
        {"player_id": "00-purdy", "player_name": "Brock Purdy", "team_abbr": "SF"},
        {"player_id": "00-lineman", "player_name": "Josh Conerly Jr.", "team_abbr": "WAS"},
        {"player_id": "00-allen-lb", "player_name": "Josh Allen", "team_abbr": "JAX"},
    ])
    yield type("Espn", (), {"calls": calls, "feeds": feeds, "search": search})
    for k, v in saved.items():
        model_data[k] = v


def test_a_player_the_league_feed_missed_still_gets_five(espn):
    espn.feeds["4361741"] = [_item(i, f"2026-0{1 + i % 8}-15T12:00:00Z") for i in range(9)]
    items = st.get_player_storylines("00-purdy", limit=5)
    assert len(items) == 5
    published = [i["published"] for i in items]
    assert published == sorted(published, reverse=True), "newest first"


def test_older_news_fills_in_when_there_is_nothing_recent(espn):
    """The ask: five storylines even when nobody has written about the player lately."""
    espn.feeds["4361741"] = [_item(1, "2025-11-03T00:00:00Z"), _item(2, "2025-12-22T00:00:00Z")]
    items = st.get_player_storylines("00-purdy", limit=5)
    assert [i["published"][:7] for i in items] == ["2025-12", "2025-11"]


def test_backfill_merges_with_league_feed_rows(espn):
    st._merge_into_store([{
        "article_id": "league-1", "player_id": "00-purdy", "espn_id": "4361741",
        "headline": "Breaking: Purdy limited in practice", "description": "", "published": "2026-09-12T18:00:00Z",
        "url": "", "image": "", "story_type": "HeadlineNews", "fetched_at": "2026-09-12T18:05:00Z",
    }], stamp_updated=True)
    espn.feeds["4361741"] = [_item(1, "2026-09-10T00:00:00Z")]
    items = st.get_player_storylines("00-purdy", limit=5)
    assert [i["article_id"] for i in items] == ["league-1", "1001"]


def test_repeat_opens_do_not_refetch_within_the_ttl(espn):
    espn.feeds["4361741"] = [_item(1, "2026-09-10T00:00:00Z")]
    for _ in range(5):
        st.get_player_storylines("00-purdy")
    assert sum(1 for c in espn.calls if c.startswith(st.PLAYER_NEWS_URL)) == 1


def test_an_espn_outage_counts_as_an_attempt(espn):
    """Otherwise every profile open during an outage waits on a doomed request."""
    model_data["gsis_to_espn"]["00-purdy"] = "boom"
    assert st.get_player_storylines("00-purdy") == []
    st.get_player_storylines("00-purdy")
    assert sum(1 for c in espn.calls if c.startswith(st.PLAYER_NEWS_URL)) == 1


def test_premium_items_are_skipped_and_repeated_blurbs_cleaned(espn):
    espn.feeds["4361741"] = [
        _item(1, "2026-09-10T00:00:00Z", headline="ESPN+ deep dive", premium=True),
        _item(2, "2026-09-09T00:00:00Z", headline="Purdy threw three touchdowns."),
        _item(3, "2026-09-08T00:00:00Z", headline="Purdy film", description="A longer breakdown.", kind="Media"),
    ]
    items = st.get_player_storylines("00-purdy")
    assert [i["headline"] for i in items] == ["Purdy threw three touchdowns.", "Purdy film"]
    assert items[0]["description"] == "", "a Rotowire blurb shouldn't print twice"
    assert items[1]["description"] == "A longer breakdown."
    assert items[0]["url"].startswith("http://m.espn.go.com"), "falls back to the mobile link"


# --- resolving players the id table doesn't cover ---------------------------------------

def _search_item(name, espn_id, team, league="nfl"):
    return {"displayName": name, "id": espn_id, "league": league, "teamRelationships": [{"displayName": team}]}


def test_search_resolves_a_player_missing_from_the_id_table(espn):
    espn.search["items"] = [_search_item("Josh Conerly Jr.", "4685326", "Washington Commanders")]
    assert st.resolve_espn_id("00-lineman") == "4685326"


def test_search_rejects_a_same_name_player_on_another_team(espn):
    """Two NFL Josh Allens: the Jaguars linebacker must not get the Bills quarterback's news."""
    espn.search["items"] = [_search_item("Josh Allen", "3918298", "Buffalo Bills")]
    assert st.resolve_espn_id("00-allen-lb") is None


def test_search_rejects_ambiguous_and_non_nfl_matches(espn):
    espn.search["items"] = [
        _search_item("Josh Conerly Jr.", "111", "Washington Commanders"),
        _search_item("Josh Conerly Jr.", "222", "Washington Commanders"),
    ]
    assert st.resolve_espn_id("00-lineman") is None
    model_data["espn_id_search_cache"].clear()
    espn.search["items"] = [_search_item("Josh Conerly Jr.", "333", "Oregon Ducks", league="college-football")]
    assert st.resolve_espn_id("00-lineman") is None


def test_suffixes_and_accents_do_not_break_matching(espn):
    model_data["df_profile"] = pl.DataFrame([{"player_id": "00-x", "player_name": "Jose Nunez Jr.", "team_abbr": "SF"}])
    espn.search["items"] = [_search_item("José Núñez", "999", "San Francisco 49ers")]
    assert st.resolve_espn_id("00-x") == "999"


def test_failed_searches_are_cached(espn):
    espn.search["items"] = []
    st.resolve_espn_id("00-lineman")
    st.resolve_espn_id("00-lineman")
    assert sum(1 for c in espn.calls if c.startswith(st.ESPN_SEARCH_URL)) == 1
