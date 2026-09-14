"""Screen context and event translation for the in-app agent.

The screen descriptor is the whole reason the dock can answer "who should I
start here" -- if it stops naming the page, every pronoun in every question
becomes a guess. The translation layer is what the browser actually renders.
"""

from applications.api.routes.agent import (
    MAX_ENTITIES,
    ScreenContext,
    ScreenEntity,
    _translate,
    build_prompt,
    describe_screen,
)


def test_a_game_page_names_both_teams_and_the_week():
    screen = ScreenContext(
        view="GAME",
        title="the SF at LA game page, roster tab",
        week=2,
        entities=[
            ScreenEntity(type="player", name="Brock Purdy", detail="QB, SF"),
            ScreenEntity(type="player", name="Puka Nacua", detail="WR, LA, Questionable"),
        ],
    )
    line = describe_screen(screen)
    assert line.startswith("[On screen:")
    assert "the SF at LA game page, roster tab" in line
    assert "week 2" in line
    assert "Brock Purdy (QB, SF)" in line
    assert "Puka Nacua (WR, LA, Questionable)" in line


def test_the_descriptor_is_prefixed_to_the_question():
    prompt = build_prompt("who should I start?", ScreenContext(view="GAME", title="the SF at LA game page"))
    assert prompt.startswith("[On screen:")
    assert prompt.endswith("who should I start?")


def test_a_bare_question_stays_bare():
    """No screen, no preamble -- not an empty bracket the model has to interpret."""
    assert build_prompt("hello", None) == "hello"
    assert build_prompt("hello", ScreenContext()) == "hello"


def test_the_view_name_stands_in_when_there_is_no_title():
    assert "Game Ranks" in describe_screen(ScreenContext(view="GAME_RANKS"))


def test_entity_lists_are_capped():
    """A roster page has 50+ players; the prompt is not the place for all of them."""
    screen = ScreenContext(
        view="TEAM_PAGE",
        entities=[ScreenEntity(type="player", name=f"Player {i}") for i in range(40)],
    )
    line = describe_screen(screen)
    assert line.count("Player") == MAX_ENTITIES


def test_unnamed_entities_are_skipped():
    screen = ScreenContext(view="GAME", entities=[ScreenEntity(type="player", name="")])
    assert "showing" not in describe_screen(screen)


# --- event translation -----------------------------------------------------

def test_text_deltas_stream_and_accumulate():
    answer: list[str] = []
    assert _translate(
        {"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "22.5 "}},
        answer,
    ) == {"type": "delta", "text": "22.5 "}
    _translate(
        {"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "points"}},
        answer,
    )
    assert "".join(answer) == "22.5 points"


def test_thinking_is_not_surfaced():
    """The dock shows one small box; reasoning scrolling past reads as the answer."""
    answer: list[str] = []
    event = {"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "delta": "hmm"}}
    assert _translate(event, answer) is None
    assert answer == []


def test_tool_activity_is_reported_both_ways():
    assert _translate({"type": "tool_execution_start", "toolName": "get_matchup"}, []) == {
        "type": "tool",
        "name": "get_matchup",
        "state": "start",
    }
    assert _translate({"type": "tool_execution_end", "toolName": "get_matchup"}, [])["state"] == "end"


def test_settling_carries_the_whole_answer():
    """agent_settled, not agent_end: agent_end is followed by retries and
    queued continuations, and treating it as terminal truncates answers."""
    answer = ["Start ", "Kyren."]
    assert _translate({"type": "agent_settled"}, answer) == {"type": "done", "text": "Start Kyren."}
    assert _translate({"type": "agent_end"}, answer) is None


def test_runtime_failures_reach_the_user_verbatim():
    out = _translate({"type": "_error", "message": "the agent process exited (no key)"}, [])
    assert out == {"type": "error", "message": "the agent process exited (no key)"}


def test_unknown_events_are_ignored():
    for kind in ("turn_start", "queue_update", "compaction_start", "message_start"):
        assert _translate({"type": kind}, []) is None
