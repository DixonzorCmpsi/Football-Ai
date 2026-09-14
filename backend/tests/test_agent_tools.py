"""The in-app agent's tool surface is derived, not hand-written.

These tests pin the derivation: specs come from the MCP functions' own
signatures and docstrings, so a tool added there needs no schema written by
hand -- and a tool whose parameter types we cannot express fails loudly instead
of reaching the model as something it has to guess at.
"""

import inspect

import pytest

from agent import tools as agent_tools


def test_specs_are_derived_from_the_mcp_functions():
    specs = {spec["name"]: spec for spec in agent_tools.list_tools()}
    assert specs, "no tools were exposed"

    projection = specs["get_player_projection"]
    props = projection["parameters"]["properties"]
    assert props["player"] == {"type": "string"}
    assert props["week"] == {"type": "integer", "default": 0}
    assert props["include_props"] == {"type": "boolean", "default": True}
    # Only the argument without a default is required.
    assert projection["parameters"]["required"] == ["player"]
    # The docstring is the description the model reads.
    assert "Projected fantasy points" in projection["description"]


def test_list_parameters_survive_the_round_trip():
    """compare_players takes list[str]; an untyped array would break the call."""
    spec = next(s for s in agent_tools.list_tools() if s["name"] == "compare_players")
    assert spec["parameters"]["properties"]["players"] == {
        "type": "array",
        "items": {"type": "string"},
    }


def test_every_exposed_tool_resolves():
    exposed = {spec["name"] for spec in agent_tools.list_tools()}
    missing = set(agent_tools.EXPOSED_TOOLS) - exposed
    assert not missing, f"listed but not found in mcp_server: {sorted(missing)}"


def test_the_escape_hatch_tools_are_not_exposed():
    """get_raw takes an arbitrary backend path and refresh clears server cache.

    Neither belongs in a surface a fan drives from a floating chat box.
    """
    assert "get_raw" not in agent_tools.EXPOSED_TOOLS
    assert "refresh" not in agent_tools.EXPOSED_TOOLS


def test_unknown_arguments_are_dropped_rather_than_raising():
    """A model that invents a keyword should get an answer, not a TypeError.

    Passed straight through, a stray keyword surfaces as a backend failure deep
    inside the tool; dropped, the call still does what was asked.
    """
    calls = {}

    def fake(query: str) -> str:
        calls["query"] = query
        return "ok"

    agent_tools._registry = {"search_players": fake}
    try:
        assert agent_tools.call_tool("search_players", {"query": "purdy", "limit": 5}) == "ok"
        assert calls["query"] == "purdy"
    finally:
        agent_tools._registry = None


def test_unknown_tool_names_raise_keyerror():
    with pytest.raises(KeyError):
        agent_tools.call_tool("delete_everything", {})


def test_unmappable_parameter_types_fail_loudly():
    """Silently defaulting to "string" would hand the model a lie."""

    def odd(payload: dict) -> str:
        """Takes something we cannot express."""
        return ""

    with pytest.raises(TypeError, match="no JSON Schema mapping"):
        agent_tools._spec("odd", odd)


def test_tools_import_without_the_mcp_sdk():
    """The API process serves these; it must not need the MCP SDK to do so.

    mcp_server.server falls back to a no-op registrar when FastMCP is absent,
    and @mcp.tool() returns the undecorated function either way.
    """
    from mcp_server import server

    assert inspect.isfunction(server.get_player_projection)

    def sample() -> str:
        return ""

    # The stand-in registers nothing and hands the function straight back, which
    # is what makes the tool bodies importable with no MCP SDK present.
    assert server._NoRegistrar().tool()(sample) is sample
