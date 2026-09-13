"""Player comparison verdicts and storyline summaries.

Neither may depend on a model being available: a comparison's reasons come from
the cards' own numbers, and a storyline without a model still opens on the
article's own sentences.
"""

import asyncio
import json

import pytest
from fastapi.responses import JSONResponse

from applications.api.services import llm_proxy as proxy
from applications.api.services import player_compare as pc
from applications.api.services import storyline_summary as ss


def _card(pid, name, pos, proj, season_avg=None, recent=None, adj=None, implied=None, prop=None, td=None,
          snaps=None, status="Active"):
    return {
        "player_id": pid, "player_name": name, "position": pos, "prediction": proj,
        "floor_prediction": proj * 0.8 if proj is not None else None, "injury_status": status,
        "implied_total": implied, "prop_line": prop, "anytime_td_prob": td, "snap_percentage": snaps,
        "projection_breakdown": {"season_avg": season_avg, "recent_form": recent, "model_adjustment": adj},
    }


# --- verdicts ---------------------------------------------------------------------

def test_a_small_gap_is_called_a_toss_up():
    v = pc.verdict([_card("a", "Jalen Coker", "WR", 11.4), _card("b", "Xavier Legette", "WR", 10.6)])
    assert v["confidence"] == "toss-up"
    assert v["leader_id"] == "a"
    assert "Toss-up" in v["headline"]


def test_a_wide_gap_is_clear_and_explained_by_the_numbers():
    top = _card("a", "Puka Nacua", "WR", 19.0, season_avg=17.2, implied=27.5, prop=88.5, snaps=92)
    low = _card("b", "Jalen Coker", "WR", 11.4, season_avg=9.9, implied=21.0, prop=41.5, snaps=70)
    v = pc.verdict([low, top])
    assert v["confidence"] == "clear" and v["leader_id"] == "a"
    text = " ".join(v["reasons"])
    assert "season average (17.2 vs 9.9" in text
    assert "expected to score more (27.5 vs 21.0" in text
    assert "receiving yards from Nacua (88.5 vs 41.5)" in text
    assert "more snaps (92% vs 70%)" in text


def test_what_favors_the_trailing_player_is_reported_too():
    top = _card("a", "Chuba Hubbard", "RB", 14.0, season_avg=13.0, recent=9.0)
    low = _card("b", "Rico Dowdle", "RB", 10.5, season_avg=10.0, recent=16.0)
    v = pc.verdict([top, low])
    assert v["confidence"] == "lean"
    assert any("Dowdle has been hotter lately" in c for c in v["counterpoints"])


def test_an_injured_leader_is_flagged_not_recommended_blindly():
    v = pc.verdict([_card("a", "Star Back", "RB", 18.0, status="Doubtful"), _card("b", "Backup", "RB", 12.0)])
    assert v["confidence"] == "risky"
    assert "safer start" in v["headline"]
    assert any("listed Doubtful" in r for r in v["counterpoints"])


def test_market_volume_lines_only_compare_like_with_like():
    qb = _card("a", "Bryce Young", "QB", 16.0, prop=220.5)
    wr = _card("b", "Jalen Coker", "WR", 11.0, prop=41.5)
    v = pc.verdict([qb, wr])
    assert not any("betting market expects" in r for r in v["reasons"] + v["counterpoints"])
    assert v["notes"], "different positions get a note about flex"


def test_fewer_than_two_projected_players_has_no_verdict():
    assert pc.verdict([_card("a", "Only One", "WR", 9.0)])["leader_id"] is None


# --- storyline text and summaries ----------------------------------------------------

STORY_HTML = (
    '<p><video1></p><p>MELBOURNE -- Brock <a href="x">Purdy</a> threw three touchdowns.</p>'
    "<p>The defense held the Rams to seven points.</p><p>Purdy said he felt sharp.</p>"
)


def test_story_html_becomes_paragraphs_without_embeds():
    assert ss.html_to_paragraphs(STORY_HTML) == [
        "MELBOURNE -- Brock Purdy threw three touchdowns.",
        "The defense held the Rams to seven points.",
        "Purdy said he felt sharp.",
    ]


def test_extractive_summary_prefers_sentences_about_the_player():
    text = "\n\n".join(ss.html_to_paragraphs(STORY_HTML))
    assert ss.extract_summary(text, "Brock Purdy") == "MELBOURNE -- Brock Purdy threw three touchdowns. Purdy said he felt sharp."


def test_rotowire_items_use_their_own_blurb_without_fetching(monkeypatch):
    monkeypatch.setattr(ss, "_fetch_story", lambda aid: pytest.fail("should not fetch"))
    item = {"article_id": "63680928", "story_type": "Rotowire", "headline": "Purdy completed 25 of 34 passes.", "description": ""}
    assert ss.article_text(item) == "Purdy completed 25 of 34 passes."


def test_a_failed_article_fetch_falls_back_to_headline_and_description(monkeypatch):
    def down(aid):
        raise TimeoutError("espn down")
    monkeypatch.setattr(ss, "_fetch_story", down)
    item = {"article_id": "91919191", "story_type": "Story", "headline": "Big win", "description": "Details here."}
    assert ss.article_text(item) == "Big win\n\nDetails here."


def test_model_reply_is_parsed_into_sections():
    raw = ("<think>let me see</think>**SUMMARY:** The 49ers beat the Rams 27-7.\n"
           "POINTS:\n- Purdy threw 3 TDs\n- Defense dominated\nFANTASY: Purdy is a strong QB1 play.")
    parsed = ss.parse_model_summary(raw)
    assert parsed == {
        "text": "The 49ers beat the Rams 27-7.",
        "key_points": ["Purdy threw 3 TDs", "Defense dominated"],
        "fantasy_impact": "Purdy is a strong QB1 play.",
    }
    assert ss.parse_model_summary("I cannot help with that.") is None


def test_house_summaries_are_cached_and_byok_ones_are_not(monkeypatch):
    calls = []

    async def fake_forward(body, upstream):
        calls.append(upstream.tier)
        return JSONResponse({"model": "m", "choices": [{"message": {"content": "SUMMARY: It happened.\nFANTASY: None."}}]})

    monkeypatch.setattr(proxy, "forward", fake_forward)
    ss._summaries.clear()
    provider = proxy.PROVIDERS["openrouter"]
    house = proxy.Upstream("house", provider, "https://x", "k", ["m"])
    byok = proxy.Upstream("byok", provider, "https://x", "k", ["m"])
    item = {"article_id": "555", "headline": "h"}

    asyncio.run(ss.model_summary("P", item, "text", byok))
    assert ss.cached_summary("555") is None, "a user's own key must not fill the shared cache"
    result = asyncio.run(ss.model_summary("P", item, "text", house))
    assert result["source"] == "ai" and ss.cached_summary("555")["text"] == "It happened."
    assert calls == ["byok", "house"]
