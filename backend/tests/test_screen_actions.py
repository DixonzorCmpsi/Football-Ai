"""Screen-action tools: name resolution, URL building, and the action queue.

The contract that matters most is at the HTTP boundary: pi does not call these
tool functions in-process. It POSTs to /agent/tools/{name} with a session
token, and that route — which alone knows the conversation, from the token —
pushes the action. Tests that call the tool functions directly on a thread that
happened to set some context would pass while the real path drops every action
(the Phase-2 bug), so the queue tests here go through TestClient.

No live backend or model: the HTTP helpers the tools use are monkeypatched.
"""

import time

import pytest
from fastapi.testclient import TestClient

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

    def test_my_team_tabs(self):
        assert sa.format_url("MY_TEAM") == "/my-team"
        assert sa.format_url("MY_TEAM", tab="WAIVERS") == "/my-team/waivers"
        assert sa.format_url("MY_TEAM", tab="LEAGUE") == "/my-team/league"

    def test_ranks(self):
        assert sa.format_url("GAME_RANKS") == "/ranks"


# --- Team resolution -------------------------------------------------------

class TestResolveTeam:
    @pytest.mark.parametrize("name,expected", [
        ("BUF", "BUF"), ("Bills", "BUF"), ("buffalo", "BUF"),
        ("JAX", "JAX"), ("Jaguars", "JAX"), ("Jacksonville", "JAX"),
        # The schedule API calls the Rams LA; LAR is accepted as input only.
        ("LA", "LA"), ("LAR", "LA"), ("Rams", "LA"), ("losangelesrams", "LA"),
        ("KC", "KC"), ("Chiefs", "KC"),
        ("SF", "SF"), ("49ers", "SF"),
        ("WAS", "WAS"), ("Commanders", "WAS"), ("LV", "LV"),
    ])
    def test_known_teams(self, name, expected):
        assert sa._resolve_team(name) == expected

    def test_ambiguous_city_is_not_mapped(self):
        # Los Angeles is both the Rams and the Chargers; only team-specific
        # inputs resolve.
        assert sa._resolve_team("Los Angeles") is None

    def test_unknown_team(self):
        assert sa._resolve_team("ZZZ") is None
        assert sa._resolve_team("") is None


# --- Action queue ----------------------------------------------------------

class TestActionQueue:
    def test_push_and_drain(self):
        sa.push_action("conv-1", "/player/00-001", "test", "open_player")
        actions = sa.drain_actions("conv-1")
        assert len(actions) == 1
        assert actions[0].url == "/player/00-001"
        assert sa.drain_actions("conv-1") == []

    def test_drain_without_push(self):
        assert sa.drain_actions("never-used") == []

    def test_conversations_are_isolated(self):
        sa.push_action("conv-a", "/a", "a", "t")
        sa.push_action("conv-b", "/b", "b", "t")
        a = sa.drain_actions("conv-a")
        b = sa.drain_actions("conv-b")
        assert len(a) == 1 and a[0].url == "/a"
        assert len(b) == 1 and b[0].url == "/b"

    def test_cap_of_five_drops_oldest(self):
        for i in range(7):
            sa.push_action("conv-cap", f"/{i}", f"a{i}", "t")
        actions = sa.drain_actions("conv-cap")
        assert len(actions) == sa.MAX_PENDING_PER_CONVERSATION
        assert [a.url for a in actions] == ["/2", "/3", "/4", "/5", "/6"]

    def test_expired_entries_are_discarded(self):
        sa.push_action("conv-exp", "/old", "old", "t")
        # Age the entry past the TTL by rewinding its stamp in place.
        with sa._actions_guard:
            entries = sa._actions["conv-exp"]
            entries[0] = (time.monotonic() - sa.ACTION_TTL_SECONDS - 1, entries[0][1])
        assert sa.drain_actions("conv-exp") == []
        # The expired entry did not linger either.
        assert "conv-exp" not in sa._actions

    def test_discard(self):
        sa.push_action("conv-drop", "/x", "x", "t")
        sa.discard_actions("conv-drop")
        assert sa.drain_actions("conv-drop") == []

    def test_push_ignores_empty(self):
        sa.push_action("", "/x", "x", "t")
        sa.push_action("", "/x", "x", "t")
        assert sa.drain_actions("") == []

    def test_thread_safety(self):
        """200 concurrent pushes lose none to races; the cap then keeps the
        newest 5, which is exactly the oldest-dropped behaviour."""
        def push_n(n):
            for i in range(n):
                sa.push_action("conv-ts", f"/{i}", "x", "t")
        import threading
        threads = [threading.Thread(target=push_n, args=(50,)) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        actions = sa.drain_actions("conv-ts")
        assert len(actions) == sa.MAX_PENDING_PER_CONVERSATION
        # All 200 pushes arrived intact (each thread's sequence is intact within
        # the survivors' urls) — no corruption, cap respected.
        assert all(a.url.startswith("/") for a in actions)
        sa.discard_actions("conv-ts")

    def test_screen_action_to_event(self):
        event = sa.ScreenAction(url="/game/BUF/HOU", label="BUF @ HOU", tool="open_game").to_event()
        assert event == {
            "type": "screen_action",
            "url": "/game/BUF/HOU",
            "label": "BUF @ HOU",
            "tool": "open_game",
        }


# --- Tools (paths only; resolution helpers are monkeypatched where needed) --

@pytest.fixture(autouse=True)
def _clean_queue():
    """The action store is module-global; tests must not see each other's entries."""
    sa._actions.clear()
    yield
    sa._actions.clear()


@pytest.fixture(autouse=True)
def _no_http(monkeypatch):
    """Screen tools never touch the real backend in these tests."""
    monkeypatch.setattr(sa, "_current_week", lambda: 1)
    monkeypatch.setattr(sa, "_get", lambda path, params=None: [])


class TestOpenScreen:
    def test_paths_match_frontend(self):
        assert sa.open_screen("ranks").path == "/ranks"
        assert sa.open_screen("tiers").path == "/tiers"
        assert sa.open_screen("my_team").path == "/my-team"
        assert sa.open_screen("my_team", "league").path == "/my-team/league"
        assert sa.open_screen("my_team", "waivers").path == "/my-team/waivers"
        assert sa.open_screen("my_team", "lineup").path == "/my-team"

    def test_invalid_screen(self):
        out = sa.open_screen("dashboard")
        assert out.path is None
        assert "not a screen" in out.text.lower()

    def test_invalid_my_team_tab(self):
        out = sa.open_screen("my_team", "stats")
        assert out.path is None

    def test_tab_on_non_my_team_is_ignored(self):
        out = sa.open_screen("ranks", "lineup")
        assert out.path == "/ranks"


class TestOpenPlayer:
    def test_resolved_name_builds_path(self, monkeypatch):
        monkeypatch.setattr(sa, "_get", lambda path, params=None: [
            {"player_id": "00-0037834", "player_name": "Brock Purdy",
             "position": "QB", "team_abbr": "SF"}])
        out = sa.open_player("purdy")
        assert out.path == "/player/00-0037834"
        assert "Brock Purdy" in out.text

    def test_label_uses_resolved_name_not_model_spelling(self, monkeypatch):
        monkeypatch.setattr(sa, "_get", lambda path, params=None: [
            {"player_id": "00-0031234", "player_name": "Kyle Williams",
             "position": "WR", "team_abbr": "NE"}])
        out = sa.open_player("kyle wiliams")  # model's typo
        assert out.path == "/player/00-0031234"
        assert "Kyle Williams" in out.label
        assert "wiliams" not in out.label

    def test_gsis_id_short_circuits(self):
        out = sa.open_player("00-0037834")
        assert out.path == "/player/00-0037834"

    def test_bare_digits_are_names_not_ids(self, monkeypatch):
        """A jersey number is not an id; it goes through search."""
        monkeypatch.setattr(sa, "_get", lambda path, params=None: [])
        out = sa.open_player("1234567")
        assert out.path is None

    def test_ambiguous_queues_nothing(self, monkeypatch):
        monkeypatch.setattr(sa, "_get", lambda path, params=None: [
            {"player_id": "00-1", "player_name": "Kyle Williams",
             "position": "WR", "team_abbr": "NE"},
            {"player_id": "00-2", "player_name": "Kyle Williams",
             "position": "WR", "team_abbr": "BUF"},
        ])
        out = sa.open_player("Kyle Williams")
        assert out.path is None
        assert "Kyle Williams (WR, NE)" in out.text
        assert "Kyle Williams (WR, BUF)" in out.text


class TestOpenCompare:
    def test_two_resolved_players(self, monkeypatch):
        def fake_get(path, params=None):
            q = (params or {}).get("q", "")
            return [{"player_id": f"00-{q.replace(' ', '')}",
                     "player_name": q, "position": "WR", "team_abbr": "NE"}]
        monkeypatch.setattr(sa, "_get", fake_get)
        out = sa.open_compare(["Kyle Williams", "DJ Moore"])
        assert out.path is not None
        assert out.path.startswith("/compare?ids=00-KyleWilliams,00-DJMoore")

    def test_one_ambiguous_player_rejects_the_whole_call(self, monkeypatch):
        def fake_get(path, params=None):
            q = (params or {}).get("q", "")
            if q == "Kyle Williams":
                return [
                    {"player_id": "00-1", "player_name": "Kyle Williams",
                     "position": "WR", "team_abbr": "NE"},
                    {"player_id": "00-2", "player_name": "Kyle Williams",
                     "position": "WR", "team_abbr": "BUF"},
                ]
            return [{"player_id": "00-3", "player_name": q,
                     "position": "WR", "team_abbr": "BUF"}]
        monkeypatch.setattr(sa, "_get", fake_get)
        out = sa.open_compare(["Kyle Williams", "DJ Moore"])
        assert out.path is None

    def test_too_many_players(self):
        out = sa.open_compare(["a", "b", "c", "d", "e"])
        assert out.path is None

    def test_single_player_rejected(self):
        out = sa.open_compare(["a"])
        assert out.path is None


class TestOpenGame:
    def test_resolves_from_schedule(self, monkeypatch):
        monkeypatch.setattr(sa, "_get", lambda path, params=None: (
            [{"away_team": "BUF", "home_team": "HOU"}]
            if path.startswith("/schedule/") else {"week": 1}
        ))
        out = sa.open_game("Bills", 1)
        assert out.path == "/game/BUF/HOU?week=1"
        assert "BUF @ HOU" in out.text

    def test_rams_open_as_la(self, monkeypatch):
        monkeypatch.setattr(sa, "_get", lambda path, params=None: (
            [{"away_team": "LA", "home_team": "SEA"}]
            if path.startswith("/schedule/") else {"week": 1}
        ))
        out = sa.open_game("Rams", 1)
        assert out.path == "/game/LA/SEA?week=1"

    def test_bye_week_queues_nothing(self, monkeypatch):
        monkeypatch.setattr(sa, "_get", lambda path, params=None: (
            [] if path.startswith("/schedule/") else {"week": 1}
        ))
        out = sa.open_game("BUF", 1)
        assert out.path is None
        assert "no game" in out.text.lower()

    def test_week_zero_uses_current_week(self, monkeypatch):
        # The autouse fixture pins the current week to 1, so this test used to
        # pass whatever week open_game picked. Make the current week distinct.
        monkeypatch.setattr(sa, "_current_week", lambda: 7)
        asked = []
        monkeypatch.setattr(sa, "_get", lambda path, params=None: (
            asked.append(path) or [{"away_team": "BUF", "home_team": "HOU"}]
        ))
        out = sa.open_game("BUF")
        assert out.path == "/game/BUF/HOU?week=7"
        # The real route is /schedule/{week}; the ?week= form 404s.
        assert asked == ["/schedule/7"]


class TestOpenTeam:
    def test_abbreviation(self):
        out = sa.open_team("BUF")
        assert out.path == "/team/BUF"

    def test_builder_tab(self):
        out = sa.open_team("NE", "builder")
        assert out.path == "/team/NE?tab=builder"

    def test_unknown_team(self):
        out = sa.open_team("ZZZ")
        assert out.path is None


class TestAddToCompare:
    def test_single_id_path(self, monkeypatch):
        monkeypatch.setattr(sa, "_get", lambda path, params=None: [
            {"player_id": "00-1", "player_name": "Kyle Williams",
             "position": "WR", "team_abbr": "NE"}])
        out = sa.add_to_compare("Kyle Williams")
        assert out.path == "/compare?ids=00-1"
        assert out.tool == "add_to_compare"


# --- Registry ---------------------------------------------------------------

class TestToolRegistry:
    def test_registered_names(self):
        names = {n for n, _ in sa.SCREEN_ACTION_TOOLS}
        assert names == {
            "open_screen", "open_player", "open_compare",
            "open_team", "open_game", "add_to_compare", "go_back",
        }

    def test_in_agent_tools_after_data_tools(self):
        from agent import tools as agent_tools
        exposed = agent_tools.EXPOSED_TOOLS
        for name in agent_tools.SCREEN_TOOL_NAMES if hasattr(agent_tools, "SCREEN_TOOL_NAMES") else []:
            assert name in exposed
        assert "open_screen" in exposed
        assert "go_back" in exposed
        # Data tools come first.
        assert exposed.index("search_players") < exposed.index("open_screen")

    def test_specs_have_no_hidden_conversation_param(self):
        from agent import tools as agent_tools
        for spec in agent_tools.list_tools():
            if spec["name"] in sa.SCREEN_TOOL_NAMES:
                assert "conversation_id" not in spec["parameters"]["properties"]

    def test_call_tool_result_returns_screen_result(self):
        from agent import tools as agent_tools
        agent_tools._registry = {
            "open_screen": sa.open_screen,
            "search_players": lambda query: "data text",
        }
        try:
            text, screen = agent_tools.call_tool_result("open_screen", {"screen": "ranks"})
            assert text == "Opened the start/sit ranks board."
            assert screen is not None and screen.path == "/ranks"
            text2, screen2 = agent_tools.call_tool_result("search_players", {"query": "x"})
            assert text2 == "data text" and screen2 is None
        finally:
            agent_tools._registry = None


# --- HTTP boundary (the Phase-2 blocker) -----------------------------------

@pytest.fixture()
def client(monkeypatch):
    """The real app with screen tools' HTTP helpers monkeypatched."""
    from applications.api.routes import agent as agent_route
    from applications.server import app

    monkeypatch.setattr(sa, "_current_week", lambda: 1)
    monkeypatch.setattr(sa, "_get", lambda path, params=None: [])
    return TestClient(app)


class TestToolsOverHttp:
    def test_open_screen_queues_for_the_tokens_conversation(self, client, monkeypatch):
        from applications.api.services import agent_tokens

        cid_a = "conv-aaaaaaaa"
        cid_b = "conv-bbbbbbbb"
        sa.discard_actions(cid_a)
        sa.discard_actions(cid_b)

        response = client.post(
            "/agent/tools/open_screen",
            json={"arguments": {"screen": "ranks"}},
            headers={"authorization": f"Bearer {agent_tokens.mint(cid_a, 'test-client')}", "x-client-id": "t" * 8},
        )
        assert response.status_code == 200, response.text
        assert "ranks" in response.json()["text"].lower()

        # Conversation A holds exactly one action; B holds none.
        a = sa.drain_actions(cid_a)
        assert len(a) == 1
        assert a[0].url == "/ranks"
        assert sa.drain_actions(cid_b) == []

    def test_ambiguous_player_queues_nothing_over_http(self, client, monkeypatch):
        from applications.api.services import agent_tokens

        cid = "conv-cccccccc"
        sa.discard_actions(cid)
        monkeypatch.setattr(sa, "_get", lambda path, params=None: [
            {"player_id": "00-1", "player_name": "Kyle Williams",
             "position": "WR", "team_abbr": "NE"},
            {"player_id": "00-2", "player_name": "Kyle Williams",
             "position": "WR", "team_abbr": "BUF"},
        ])

        response = client.post(
            "/agent/tools/open_player",
            json={"arguments": {"player": "Kyle Williams"}},
            headers={"authorization": f"Bearer {agent_tokens.mint(cid, 'testclient')}", "x-client-id": "t" * 8},
        )
        assert response.status_code == 200
        assert response.json()["text"]  # the model still gets the candidates
        assert sa.drain_actions(cid) == []

    def test_bad_token_is_rejected(self, client):
        response = client.post(
            "/agent/tools/open_screen",
            json={"arguments": {"screen": "ranks"}},
            headers={"authorization": "Bearer spt_bogus.sig", "x-client-id": "t" * 8},
        )
        assert response.status_code == 401