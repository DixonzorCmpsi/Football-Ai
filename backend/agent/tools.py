"""The football tool surface handed to the in-app agent.

pi ships no MCP client -- that is a deliberate design choice of the project,
not an oversight -- so the in-app agent cannot reuse `backend/mcp_server`
over stdio the way Claude Code does.

Reimplementing sixteen tools in TypeScript would mean two copies of every
formatter, drifting apart on the first bug fix. Instead this module exposes
the *same Python functions* the MCP server publishes, and a thin pi extension
proxies to them over one HTTP endpoint. One implementation, one formatter,
two agents.

Tool specs are derived from the functions themselves (signature + docstring),
so adding a tool to the MCP server and listing its name here is the whole job.

Screen-action tools (open_player, open_game, ...) live in
``agent.screen_actions`` and sit in the same registry after the data tools.
They return a ``ScreenResult`` rather than plain text: the route pushes its
path onto the conversation's action queue, because only the route knows which
conversation a call belongs to (from the session token).

UI-control tools (read_screen, click, type_text, ...) live in
``agent.ui_control``. They return a ``UiCommand``: the route sends it to the
user's browser and waits for the page's answer.

Web tools (web_search, fetch_page) live in ``agent.web_tools`` and come last.
They return a ``WebRequest`` the route runs with the conversation's key and
per-question budget.
"""

from __future__ import annotations

import inspect
import logging
import typing
from typing import Any, Callable

from agent.screen_actions import SCREEN_ACTION_TOOLS, SCREEN_TOOL_NAMES, ScreenResult
from agent.ui_control import UI_CONTROL_TOOLS, UI_TOOL_NAMES, UiCommand
from agent.web_tools import WEB_TOOL_NAMES, WEB_TOOLS, WebRequest

logger = logging.getLogger(__name__)

# Ordered: the model reads these top to bottom, so the everyday ones come first.
#
# `get_raw` and `refresh` are deliberately absent. get_raw takes an arbitrary
# backend path, which invites invented endpoints and gives a user-facing agent
# a surface nobody reviewed; refresh exists for an operator clearing cache after
# an ETL run, which is not a thing a fan asks about mid-conversation.
EXPOSED_TOOLS: tuple[str, ...] = (
    "search_players",
    "get_player_projection",
    "get_player_history",
    "get_player_storylines",
    "compare_players",
    "get_schedule",
    "get_matchup",
    "get_matchup_insights",
    "get_parlays",
    "sleeper_find_leagues",
    "sleeper_list_teams",
    "sleeper_analyze_roster",
    "sleeper_matchup",
    "sleeper_waiver_targets",
    "get_status",
) + tuple(name for name, _fn in SCREEN_ACTION_TOOLS) + tuple(name for name, _fn in UI_CONTROL_TOOLS) + tuple(
    name for name, _fn in WEB_TOOLS
)

# Python annotation -> JSON Schema. The tool signatures only use these;
# anything else should fail loudly at import rather than reach the model as an
# untyped parameter it will guess at.
_JSON_TYPES: dict[Any, dict] = {
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
    bool: {"type": "boolean"},
    list[str]: {"type": "array", "items": {"type": "string"}},
}


class ToolsUnavailable(RuntimeError):
    """The MCP tool module could not be imported; the agent has no tools."""


_registry: dict[str, Callable[..., Any]] | None = None


def _load() -> dict[str, Callable[..., Any]]:
    """Import the MCP server module and pick out the exposed functions.

    FastMCP's @mcp.tool() returns the undecorated function, so these are plain
    callables -- no MCP session, no transport, just a function call.

    Screen-action tools come from agent.screen_actions, not mcp_server, and are
    merged in so the model sees one flat list.
    """
    global _registry
    if _registry is not None:
        return _registry
    try:
        from mcp_server import server as mcp_server
    except Exception as exc:  # pragma: no cover - import environment specific
        raise ToolsUnavailable(f"cannot import mcp_server.server: {exc}") from exc

    found: dict[str, Callable[..., Any]] = {}
    for name in EXPOSED_TOOLS:
        if name in SCREEN_TOOL_NAMES:
            found[name] = dict(SCREEN_ACTION_TOOLS)[name]
            continue
        if name in UI_TOOL_NAMES:
            found[name] = dict(UI_CONTROL_TOOLS)[name]
            continue
        if name in WEB_TOOL_NAMES:
            found[name] = dict(WEB_TOOLS)[name]
            continue
        fn = getattr(mcp_server, name, None)
        if not callable(fn):
            logger.warning("agent tool %r is listed but missing from mcp_server", name)
            continue
        found[name] = fn
    _registry = found
    return found


def _spec(name: str, fn: Callable[..., Any]) -> dict:
    sig = inspect.signature(fn)
    # get_type_hints, not __annotations__: mcp_server uses `from __future__
    # import annotations`, so the raw annotations are strings like "int".
    hints = typing.get_type_hints(fn)
    properties: dict[str, dict] = {}
    required: list[str] = []

    for param_name, param in sig.parameters.items():
        annotation = hints.get(param_name, str)
        schema = _JSON_TYPES.get(annotation)
        if schema is None:
            raise TypeError(
                f"{name}.{param_name}: no JSON Schema mapping for {annotation!r}. "
                "Add one to _JSON_TYPES rather than letting the model guess."
            )
        schema = dict(schema)
        if param.default is inspect.Parameter.empty:
            required.append(param_name)
        else:
            schema["default"] = param.default
        properties[param_name] = schema

    doc = inspect.getdoc(fn) or ""
    return {
        "name": name,
        "description": doc,
        # First line only: pi renders this next to the tool name in its UI.
        "label": doc.split("\n", 1)[0].rstrip("."),
        "parameters": {
            "type": "object",
            "properties": properties,
            "required": required,
        },
    }


def list_tools() -> list[dict]:
    """JSON Schema specs for every tool the in-app agent may call."""
    return [_spec(name, fn) for name, fn in _load().items()]


def is_screen_tool(name: str) -> bool:
    return name in SCREEN_TOOL_NAMES


def _clean_args(fn: Callable, name: str, arguments: dict | None) -> dict:
    """Drop anything the model invented. A stray keyword would raise TypeError
    deep inside the tool, which reads to the user as a backend failure."""
    args = dict(arguments or {})
    accepted = set(inspect.signature(fn).parameters)
    unknown = [k for k in args if k not in accepted]
    for key in unknown:
        args.pop(key)
    if unknown:
        logger.info("agent tool %s: ignored unknown argument(s) %s", name, unknown)
    return args


def call_tool(name: str, arguments: dict | None = None) -> str:
    """Run one tool and return its text. Raises KeyError for an unknown name."""
    text, _screen = call_tool_result(name, arguments)
    return text


def call_tool_result(name: str, arguments: dict | None = None) -> tuple[str, ScreenResult | UiCommand | WebRequest | None]:
    """Run one tool; also return its ScreenResult or UiCommand when it has one.

    A screen tool that resolved a movement returns (text, ScreenResult) and the
    route queues the path for the conversation. A UI tool returns
    (text, UiCommand) for the route to run in the browser; its text is only the
    fallback when there's no browser. A data tool returns (text, None). Raises
    KeyError for an unknown name.
    """
    registry = _load()
    if name not in registry:
        raise KeyError(name)
    fn = registry[name]
    args = _clean_args(fn, name, arguments)
    out = fn(**args)
    if isinstance(out, ScreenResult):
        return out.text, out
    if isinstance(out, UiCommand):
        return (out.error or "This needs the user's app open in a browser."), out
    if isinstance(out, WebRequest):
        return (out.error or "This needs a conversation to run in."), out
    return str(out), None