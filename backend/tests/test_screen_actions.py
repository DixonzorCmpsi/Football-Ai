"""Screen-action tools: name resolution, URL building, and the action queue.

These tools move the user's screen, so the contract is:

* A name that resolves to one player/team builds a URL and enqueues an action.
* An ambiguous name enqueues nothing and returns a disambiguation prompt.
* Only recognized URLs reach the queue (the frontend's parseAppUrl is the
  final gatekeeper, but we never hand it garbage).
* The action queue is per-conversation and thread-safe.
"""

import queue
import threading

import pytest

from agent import screen_actions as sa


# --- URL builder -----------------------------------------------------------

class TestFormatUrl:
    def test_schedule(self):
        assert sa.format_url("SCHEDULE") == "/"

    def test_game(self):
        assert sa.format_url("GAME", away="BUF", home="HOU") == "/game/BUF/HOU"

    def test_compare_with_ids(self):
        assert sa.format_url("COMPARE", ids=["00-001", "00-002"]) == "/compare?ids=00-001,00-002"

    def test_compare_empty(self):
        assert sa.format_url("COMPARE", ids=[]) == "/compare"

    def test_player(self):
        assert sa.format_url("HISTORY", player_id="00-0037834") == "/player/00-0037834"

    def test_team_overview(self):
        assert sa.format_url("TEAM_PAGE", team="BUF") == "/team/BUF"

    def test_team_builder(self):
        assert sa.format_url("TEAM_PAGE", team="BUF", tab="builder") == "/team/BUF?tab=builder"

    def test_my_team_lineup(self):
        assert sa.format_url("MY_TEAM") == "/my-team"

    def test_my_team_waivers(self):
        assert sa.format_url("MY_TEAM", tab="WAIVERS") == "/my-team/waivers"

    def test_my_team_league(self):
        assert sa.format_url("MY_TEAM", tab="LEAGUE") == "/my-team/league"

    def test_ranks(self):
        assert sa.format_url("GAME_RANKS") == "/ranks"

    def test_tiers(self):
        assert sa.format_url("TIERS") == "/tiers"


# --- Team resolution -------------------------------------------------------

class TestResolveTeam:
    @pytest.mark.parametrize("name,expected", [
        ("BUF", "BUF"),
        ("Bills", "BUF"),
        ("buffalo", "BUF"),
        ("JAX", "JAX"),
        ("Jaguars", "JAX"),
        ("Jacksonville", "JAX"),
        ("KC", "KC"),
        ("Chiefs", "KC"),
        ("SF", "SF"),
        ("49ers", "SF"),
        ("NO", "NO"),
        ("Saints", "NO"),
        ("LAC", "LAC"),
        ("Chargers", "LAC"),
        ("WAS", "WAS"),
        ("Commanders", "WAS"),
    ])
    def test_known_teams(self, name, expected):
        assert sa._resolve_team(name) == expected

    def test_unknown_team(self):
        assert sa._resolve_team("ZZZ") is None

    def test_empty(self):
        assert sa._resolve_team("") is None

    def test_case_insensitive(self):
        assert sa._resolve_team("buf") == "BUF"
        assert sa._resolve_team("bills") == "BUF"


# --- Action queue ----------------------------------------------------------

class TestActionQueue:
    def test_install_and_drain(self):
        sa.install("conv-1")
        sa._enqueue("conv-1", sa.ScreenAction(url="/player/00-001", label="test", tool="open_player"))
        actions = list(sa.drain("conv-1"))
        assert len(actions) == 1
        assert actions[0].url == "/player/00-001"
        # Drain again is empty.
        assert list(sa.drain("conv-1")) == []

    def test_drain_without_install(self):
        assert list(sa.drain("never-installed")) == []

    def test_teardown(self):
        sa.install("conv-2")
        sa._enqueue("conv-2", sa.ScreenAction(url="/", label="x", tool="t"))
        sa.teardown("conv-2")
        assert list(sa.drain("conv-2")) == []

    def test_conversations_are_isolated(self):
        sa.install("conv-a")
        sa.install("conv-b")
        sa._enqueue("conv-a", sa.ScreenAction(url="/a", label="a", tool="t"))
        sa._enqueue("conv-b", sa.ScreenAction(url="/b", label="b", tool="t"))
        a = list(sa.drain("conv-a"))
        b = list(sa.drain("conv-b"))
        assert len(a) == 1 and a[0].url == "/a"
        assert len(b) == 1 and b[0].url == "/b"

    def test_thread_safety(self):
        sa.install("conv-ts")
        def enqueue_n(n):
            for i in range(n):
                sa._enqueue("conv-ts", sa.ScreenAction(url=f"/{i}", label="x", tool="t"))
        threads = [threading.Thread(target=enqueue_n, args=(50,)) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        actions = list(sa.drain("conv-ts"))
        assert len(actions) == 200
        sa.teardown("conv-ts")

    def test_screen_action_to_event(self):
        action = sa.ScreenAction(url="/game/BUF/HOU", label="BUF @ HOU", tool="open_game")
        event = action.to_event()
        assert event["type"] == "screen_action"
        assert event["url"] == "/game/BUF/HOU"
        assert event["label"] == "BUF @ HOU"
        assert event["tool"] == "open_game"


# --- Tool: open_screen -----------------------------------------------------

class TestOpenScreen:
    def test_valid_screen(self):
        sa.install("conv-os")
        sa.set_conversation("conv-os")
        try:
            result = sa.open_screen("tiers")
            assert "tier list" in result.lower()
            actions = list(sa.drain("conv-os"))
            assert len(actions) == 1
            assert actions[0].url == "/tiers"
            assert actions[0].tool == "open_screen"
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-os")

    def test_my_team_screen(self):
        sa.install("conv-os2")
        sa.set_conversation("conv-os2")
        try:
            result = sa.open_screen("my-team")
            assert "sleeper" in result.lower()
            actions = list(sa.drain("conv-os2"))
            assert actions[0].url == "/my-team"
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-os2")

    def test_invalid_screen(self):
        sa.install("conv-os3")
        sa.set_conversation("conv-os3")
        try:
            result = sa.open_screen("dashboard")
            assert "not a screen" in result.lower()
            assert list(sa.drain("conv-os3")) == []
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-os3")


# --- Tool: open_game -------------------------------------------------------

class TestOpenGame:
    def test_valid_game(self):
        sa.install("conv-og")
        sa.set_conversation("conv-og")
        try:
            result = sa.open_game("BUF", "HOU")
            assert "BUF @ HOU" in result
            actions = list(sa.drain("conv-og"))
            assert len(actions) == 1
            assert actions[0].url == "/game/BUF/HOU"
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-og")

    def test_team_names_resolved(self):
        sa.install("conv-og2")
        sa.set_conversation("conv-og2")
        try:
            result = sa.open_game("Bills", "Texans")
            assert "BUF @ HOU" in result
            actions = list(sa.drain("conv-og2"))
            assert actions[0].url == "/game/BUF/HOU"
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-og2")

    def test_same_team_rejected(self):
        sa.install("conv-og3")
        sa.set_conversation("conv-og3")
        try:
            result = sa.open_game("BUF", "Bills")
            assert "same" in result.lower()
            assert list(sa.drain("conv-og3")) == []
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-og3")

    def test_unknown_team(self):
        sa.install("conv-og4")
        sa.set_conversation("conv-og4")
        try:
            result = sa.open_game("ZZZ", "BUF")
            assert "not a recognized" in result.lower()
            assert list(sa.drain("conv-og4")) == []
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-og4")


# --- Tool: open_team -------------------------------------------------------

class TestOpenTeam:
    def test_abbreviation(self):
        sa.install("conv-ot")
        sa.set_conversation("conv-ot")
        try:
            result = sa.open_team("BUF")
            assert "BUF" in result
            actions = list(sa.drain("conv-ot"))
            assert actions[0].url == "/team/BUF"
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-ot")

    def test_builder_tab(self):
        sa.install("conv-ot2")
        sa.set_conversation("conv-ot2")
        try:
            sa.open_team("NE", "builder")
            actions = list(sa.drain("conv-ot2"))
            assert actions[0].url == "/team/NE?tab=builder"
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-ot2")


# --- Tool: set_sleeper_tab -------------------------------------------------

class TestSetSleeperTab:
    @pytest.mark.parametrize("tab,expected_url", [
        ("lineup", "/my-team"),
        ("waivers", "/my-team/waivers"),
        ("league", "/my-team/league"),
    ])
    def test_valid_tabs(self, tab, expected_url):
        cid = f"conv-st-{tab}"
        sa.install(cid)
        sa.set_conversation(cid)
        try:
            result = sa.set_sleeper_tab(tab)
            assert tab in result.lower()
            actions = list(sa.drain(cid))
            assert actions[0].url == expected_url
        finally:
            sa.set_conversation(None)
            sa.teardown(cid)

    def test_invalid_tab(self):
        sa.install("conv-st-bad")
        sa.set_conversation("conv-st-bad")
        try:
            result = sa.set_sleeper_tab("stats")
            assert "not a my team tab" in result.lower()
            assert list(sa.drain("conv-st-bad")) == []
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-st-bad")


# --- Tool: go_back ---------------------------------------------------------

class TestGoBack:
    def test_enqueues_back_action(self):
        sa.install("conv-gb")
        sa.set_conversation("conv-gb")
        try:
            result = sa.go_back()
            assert "back" in result.lower()
            actions = list(sa.drain("conv-gb"))
            assert len(actions) == 1
            assert actions[0].url == "app://back"
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-gb")


# --- Tool: open_compare ----------------------------------------------------

class TestOpenCompare:
    def test_too_many_players(self):
        sa.install("conv-oc")
        sa.set_conversation("conv-oc")
        try:
            result = sa.open_compare(["a", "b", "c", "d", "e"])
            assert "at most" in result.lower()
            assert list(sa.drain("conv-oc")) == []
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-oc")

    def test_empty_list(self):
        sa.install("conv-oc2")
        sa.set_conversation("conv-oc2")
        try:
            result = sa.open_compare([])
            assert "at least one" in result.lower()
        finally:
            sa.set_conversation(None)
            sa.teardown("conv-oc2")


# --- Tool registry ---------------------------------------------------------

class TestToolRegistry:
    def test_all_tools_are_callable(self):
        for name, fn in sa.SCREEN_ACTION_TOOLS:
            assert callable(fn), f"{name} is not callable"

    def test_registry_names_match_functions(self):
        names = [n for n, _ in sa.SCREEN_ACTION_TOOLS]
        assert "open_screen" in names
        assert "open_player" in names
        assert "open_game" in names
        assert "open_team" in names
        assert "open_compare" in names
        assert "go_back" in names
        assert "set_sleeper_tab" in names
        assert "resolve_player_name" in names
        assert "add_to_compare" in names

    def test_tools_are_registered_in_agent_tools(self):
        from agent import tools as agent_tools
        exposed = set(agent_tools.EXPOSED_TOOLS)
        for name, _ in sa.SCREEN_ACTION_TOOLS:
            assert name in exposed, f"{name} missing from EXPOSED_TOOLS"