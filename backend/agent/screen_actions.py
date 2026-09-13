"""Screen-action tools for the in-app agent.

The agent can move the user's screen the same way a mouse and keyboard can: open
a page, jump to a player, start a comparison, switch the My Team tab. Each tool
resolves a name to an id (when needed), builds the web address the frontend
recognizes, and enqueues an action that the chat SSE stream ships to the browser
in the same pass as text deltas.

Design:

* **One action queue per conversation.** Tools run on the agent's thread (pi's
  tool-execution worker); the SSE pump runs on the event loop. A thread-safe
  queue bridges them, the same pattern the chat pump uses for pi events.
* **The backend resolves names; the browser applies the URL.** A tool never
  trusts the model's spelling of "00-0037834". It calls the same
  ``/players/search`` endpoint the MCP tools use, and an ambiguous name returns
  a list to pick from rather than a wrong guess.
* **Only recognized URLs.** ``format_url`` builds every path from validated
  components; the browser's ``parseAppUrl`` is the final gatekeeper. A path that
  does not parse to a location is dropped before it reaches the DOM.
* **Ambiguous = nothing moves.** If a name matches two or more players, no action
  is queued. The tool's return text tells the agent to disambiguate.

The tools are registered alongside the MCP football tools (see ``agent/tools.py``)
and proxied to this module by the same HTTP endpoint the pi extension already
calls. They are pure-Python functions that return ``str`` — the same contract as
every other tool — and side-effect the queue via the ``active_conversation``
context manager the route sets up before each prompt.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# Team abbreviations the app's schedule/matchup endpoints accept. Sourced from
# the 32 active NFL franchises. A tool that receives "Jacksonville" or "JAX"
# both resolve to "JAX".
TEAM_ALIASES: dict[str, str] = {
    "ARI": "ARI", "ARZ": "ARI", "CARDINALS": "ARI", "ARIZONA": "ARI",
    "ATL": "ATL", "FALCONS": "ATL", "ATLANTA": "ATL",
    "BAL": "BAL", "RAVENS": "BAL", "BALTIMORE": "BAL",
    "BUF": "BUF", "BILLS": "BUF", "BUFFALO": "BUF",
    "CAR": "CAR", "PANTHERS": "CAR", "CAROLINA": "CAR",
    "CHI": "CHI", "BEARS": "CHI", "CHICAGO": "CHI",
    "CIN": "CIN", "BENGALS": "CIN", "CINCINNATI": "CIN",
    "CLE": "CLE", "BROWNS": "CLE", "CLEVELAND": "CLE",
    "DAL": "DAL", "COWBOYS": "DAL", "DALLAS": "DAL",
    "DEN": "DEN", "BRONCOS": "DEN", "DENVER": "DEN",
    "DET": "DET", "LIONS": "DET", "DETROIT": "DET",
    "GB": "GB", "GNB": "GB", "PACKERS": "GB", "GREENBAY": "GB",
    "HOU": "HOU", "TEXANS": "HOU", "HOUSTON": "HOU",
    "IND": "IND", "COLTS": "IND", "INDIANAPOLIS": "IND",
    "JAX": "JAX", "JAC": "JAX", "JAGUARS": "JAX", "JACKSONVILLE": "JAX",
    "KC": "KC", "KAN": "KC", "CHIEFS": "KC", "KANSASCITY": "KC",
    "LV": "LV", "LVR": "LV", "RAIDERS": "LV", "OAK": "LV", "LASVEGAS": "LV",
    "LAC": "LAC", "CHARGERS": "LAC", "SD": "LAC", "LOSANGELESCHARGERS": "LAC",
    "LAR": "LAR", "RAMS": "LAR", "LOSANGELESRAMS": "LAR", "LOSANGELES": "LAR",
    "MIA": "MIA", "DOLPHINS": "MIA", "MIAMI": "MIA",
    "MIN": "MIN", "VIKINGS": "MIN", "MINNESOTA": "MIN",
    "NE": "NE", "NWE": "NE", "PATRIOTS": "NE", "NEWENGLAND": "NE",
    "NO": "NO", "NOR": "NO", "SAINTS": "NO", "NEWORLEANS": "NO",
    "NYG": "NYG", "GIANTS": "NYG", "NEWYORKGIANTS": "NYG",
    "NYJ": "NYJ", "JETS": "NYJ", "NEWYORKJETS": "NYJ",
    "PHI": "PHI", "EAGLES": "PHI", "PHILADELPHIA": "PHI",
    "PIT": "PIT", "STEELERS": "PIT", "PITTSBURGH": "PIT",
    "SF": "SF", "SFO": "SF", "49ERS": "SF", "SANFRANCISCO": "SF",
    "SEA": "SEA", "SEAHAWKS": "SEA", "SEATTLE": "SEA",
    "TB": "TB", "TAM": "TB", "BUCCANEERS": "TB", "TAMPABAY": "TB",
    "TEN": "TEN", "TITANS": "TEN", "TENNESSEE": "TEN",
    "WAS": "WAS", "WSH": "WAS", "COMMANDERS": "WAS", "WASHINGTON": "WAS",
}

# The screens the agent can send the user to. Kept in sync with the frontend's
# AppLocation view names (see src/lib/appUrl.ts).
NAV_SCREENS = {
    "schedule", "lookup", "trending", "picks", "playoffs",
    "tiers", "teams", "ranks", "my-team",
}

MAX_COMPARE_IDS = 4


# ---------------------------------------------------------------------------
# Action queue
# ---------------------------------------------------------------------------

@dataclass
class ScreenAction:
    """One movement the browser should perform."""

    url: str
    label: str
    tool: str

    def to_event(self) -> dict[str, Any]:
        return {
            "type": "screen_action",
            "url": self.url,
            "label": self.label,
            "tool": self.tool,
        }


@dataclass
class _ConversationActions:
    actions: queue.Queue = field(default_factory=queue.Queue)
    lock: threading.Lock = field(default_factory=threading.Lock)


# One queue per conversation_id. The chat route installs one before prompting pi
# and drains it alongside pi's own events.
_queues: dict[str, _ConversationActions] = {}
_queues_guard = threading.Lock()


def install(conversation_id: str) -> None:
    """Create (or reset) the action queue for this conversation."""
    with _queues_guard:
        _queues[conversation_id] = _ConversationActions()


def drain(conversation_id: str) -> Iterator[ScreenAction]:
    """Yield every queued action without blocking, then stop."""
    ca = _queues.get(conversation_id)
    if not ca:
        return
    while True:
        try:
            yield ca.actions.get_nowait()
        except queue.Empty:
            return


def teardown(conversation_id: str) -> None:
    _queues.pop(conversation_id, None)


def _enqueue(conversation_id: str, action: ScreenAction) -> None:
    ca = _queues.get(conversation_id)
    if not ca:
        logger.debug("screen action enqueued with no queue for %s", conversation_id)
        return
    ca.actions.put(action)


# The conversation_id is set by the route before the prompt runs. Tools read it
# from here — they don't receive it as a parameter because the model would have
# to supply it, and the model does not know it.
_current_conversation: threading.local = threading.local()


def set_conversation(conversation_id: str | None) -> None:
    _current_conversation.cid = conversation_id  # type: ignore[attr-defined]


def _cid() -> str:
    return getattr(_current_conversation, "cid", None) or ""


# ---------------------------------------------------------------------------
# URL builder — mirrors src/lib/appUrl.ts formatAppUrl
# ---------------------------------------------------------------------------

def _enc(s: str) -> str:
    return s.replace("/", "%2F")


def format_url(view: str, **kw: Any) -> str:
    """Build the path the frontend's parseAppUrl will accept."""
    v = view.upper()
    if v == "SCHEDULE":
        return "/"
    if v == "GAME":
        return f"/game/{_enc(kw['away'])}/{_enc(kw['home'])}"
    if v == "LOOKUP":
        return "/lookup"
    if v == "COMPARE":
        ids = kw.get("ids", [])
        return f"/compare?ids={','.join(ids)}" if ids else "/compare"
    if v == "HISTORY":
        return f"/player/{_enc(kw['player_id'])}"
    if v == "TRENDING":
        return "/trending"
    if v == "PICKS":
        return "/picks"
    if v == "PLAYOFFS":
        return "/playoffs"
    if v == "TIERS":
        return "/tiers"
    if v == "TEAMS":
        return "/teams"
    if v == "GAME_RANKS":
        return "/ranks"
    if v == "TEAM_PAGE":
        tab = kw.get("tab", "overview")
        return f"/team/{_enc(kw['team'])}?tab=builder" if tab == "builder" else f"/team/{_enc(kw['team'])}"
    if v == "MY_TEAM":
        tab = kw.get("tab", "LINEUP")
        if tab == "WAIVERS":
            return "/my-team/waivers"
        if tab == "LEAGUE":
            return "/my-team/league"
        return "/my-team"
    return "/"


# ---------------------------------------------------------------------------
# Name resolution — reuses the backend's /players/search
# ---------------------------------------------------------------------------

def _resolve_team(name: str) -> str | None:
    """Accept 'JAX', 'Jacksonville', 'jaguars' and return 'JAX'."""
    key = name.strip().upper().replace(".", "").replace(" ", "")
    if key in TEAM_ALIASES:
        return TEAM_ALIASES[key]
    # Try the first word (e.g. "Kansas City" -> "KANSASCITY" -> no match, but
    # "Chiefs" -> "CHIEFS" -> "KC"). Also try the full city name.
    return TEAM_ALIASES.get(key)


def _http_get(path: str, params: dict | None = None) -> Any:
    """Lightweight GET to the backend, used for player search."""
    import os
    import httpx

    base = (os.getenv("FOOTBALL_AI_API", "http://127.0.0.1:8000")).rstrip("/")
    url = f"{base}{path}"
    try:
        resp = httpx.get(url, params=params, timeout=10.0)
        if resp.status_code == 404:
            return None
        if resp.status_code >= 400:
            return None
        return resp.json()
    except Exception:
        return None


def _resolve_player(name_or_id: str) -> tuple[str | None, str]:
    """Accept a gsis id or a name. Returns (player_id, note).

    Same logic as mcp_server._resolve_player, duplicated here so this module
    has no import dependency on mcp_server (which may not be installed in the
    agent's interpreter).
    """
    probe = (name_or_id or "").strip()
    if not probe:
        return None, "No player given."
    if probe.startswith("00-") or probe.replace("-", "").isdigit():
        return probe, ""

    results = _http_get("/players/search", {"q": probe}) or []
    if not results:
        return None, f"No player matched '{probe}'."
    exact = [r for r in results
             if (r.get("player_name") or "").strip().lower() == probe.lower()]
    if len(exact) == 1:
        return exact[0]["player_id"], ""
    if len(results) == 1:
        return results[0]["player_id"], ""
    pool = exact or results
    if len(pool) == 1:
        return pool[0]["player_id"], ""
    listing = ", ".join(
        f"{r.get('player_name')} ({r.get('team')}, {r.get('position')})"
        for r in pool[:6]
    )
    return None, f"'{probe}' matched {len(pool)} players: {listing}. Pick one and call the tool again with the full name."


def _enqueue_action(tool: str, url: str, label: str) -> None:
    _enqueue(_cid(), ScreenAction(url=url, label=label, tool=tool))


# ---------------------------------------------------------------------------
# Tools — each returns str (same contract as MCP tools) and side-effects the queue
# ---------------------------------------------------------------------------

# --- Navigation: the five core tools from the prompt -------------------------

def open_screen(screen: str) -> str:
    """Send the user to a named screen: schedule, lookup, trending, picks,
    playoffs, tiers, teams, ranks, or my-team.

    Use this when the question implies a page change but no specific player,
    team or game. For "show me the Bills game" use open_game; for "pull up
    Purdy" use open_player.
    """
    key = (screen or "").strip().lower().replace(" ", "-")
    if key not in NAV_SCREENS:
        return (
            f"'{screen}' is not a screen I can open. "
            f"Choose from: {', '.join(sorted(NAV_SCREENS))}."
        )
    label_map = {
        "schedule": "the weekly schedule",
        "lookup": "player lookup",
        "trending": "trending players",
        "picks": "your saved picks",
        "playoffs": "the playoff picture",
        "tiers": "the tier list",
        "teams": "the team index",
        "ranks": "the start/sit ranks board",
        "my-team": "your Sleeper roster",
    }
    view = {
        "schedule": "SCHEDULE", "lookup": "LOOKUP", "trending": "TRENDING",
        "picks": "PICKS", "playoffs": "PLAYOFFS", "tiers": "TIERS",
        "teams": "TEAMS", "ranks": "GAME_RANKS", "my-team": "MY_TEAM",
    }[key]
    url = format_url(view)
    _enqueue_action("open_screen", url, label_map[key])
    return f"Opening {label_map[key]}."


def open_player(player: str) -> str:
    """Open a player's full projection page (game log, props, injury status).

    Accepts a name ("Brock Purdy") or a gsis id ("00-0037834"). If the name
    matches more than one player, nothing opens — the return text lists the
    matches so you can disambiguate and call again.
    """
    pid, note = _resolve_player(player)
    if not pid:
        return note
    url = format_url("HISTORY", player_id=pid)
    _enqueue_action("open_player", url, f"{player}'s page")
    return f"Opening {player}'s page."


def open_compare(players: list[str]) -> str:
    """Open the side-by-side comparison view for up to 4 players.

    Pass full names or gsis ids. Each is resolved independently; if any name is
    ambiguous the whole call is rejected so the user doesn't land on a partial
    comparison.
    """
    if not players:
        return "Pass at least one player name."
    if len(players) > MAX_COMPARE_IDS:
        return f"Compare supports at most {MAX_COMPARE_IDS} players; you passed {len(players)}."

    ids: list[str] = []
    names: list[str] = []
    for p in players:
        pid, note = _resolve_player(p)
        if not pid:
            return f"Cannot compare: {note}"
        ids.append(pid)
        names.append(p)
    url = format_url("COMPARE", ids=ids)
    label = " vs ".join(names[:2]) if len(names) <= 2 else f"{len(names)} players"
    _enqueue_action("open_compare", url, f"comparing {label}")
    return f"Opening comparison of {', '.join(names)}."


def open_team(team: str, tab: str = "overview") -> str:
    """Open a team's page (offense overview or team builder).

    ``team`` accepts an abbreviation ("BUF"), a full name ("Bills") or a city
    ("Buffalo"). ``tab`` is "overview" (default) or "builder" for the team
    builder.
    """
    abbr = _resolve_team(team)
    if not abbr:
        return f"'{team}' is not a recognized team. Use an abbreviation like BUF, KC, or SF."
    tab_lower = (tab or "overview").strip().lower()
    if tab_lower not in ("overview", "builder"):
        tab_lower = "overview"
    url = format_url("TEAM_PAGE", team=abbr, tab=tab_lower)
    _enqueue_action("open_team", url, f"the {abbr} team page ({tab_lower})")
    return f"Opening the {abbr} team page, {tab_lower} tab."


def open_game(away: str, home: str) -> str:
    """Open the matchup page for a specific game (away team at home team).

    Both arguments accept abbreviations ("BUF"), full names ("Bills") or cities
    ("Buffalo"). The away team is listed first.
    """
    away_abbr = _resolve_team(away)
    home_abbr = _resolve_team(home)
    if not away_abbr:
        return f"'{away}' is not a recognized away team."
    if not home_abbr:
        return f"'{home}' is not a recognized home team."
    if away_abbr == home_abbr:
        return "The away and home teams are the same."
    url = format_url("GAME", away=away_abbr, home=home_abbr)
    _enqueue_action("open_game", url, f"{away_abbr} @ {home_abbr}")
    return f"Opening the {away_abbr} @ {home_abbr} game."


# --- UI interaction tools ---------------------------------------------------

def add_to_compare(player: str) -> str:
    """Add a player to the comparison tray and switch to the compare view.

    Use this when the user is already looking at someone and says "add him to
    my comparison" — it resolves the name and opens compare with the player
    included. The compare view holds up to 4 players.
    """
    pid, note = _resolve_player(player)
    if not pid:
        return note
    # We don't know the existing compare list from here; the browser merges.
    # The URL carries just this player; the frontend's applyLocation for
    # COMPARE replaces the list, so we use open_compare semantics.
    url = format_url("COMPARE", ids=[pid])
    _enqueue_action("add_to_compare", url, f"comparing {player}")
    return f"Added {player} to the comparison and opened the view."


def go_back() -> str:
    """Send the user back one screen, like the browser's Back button."""
    # A special URL the frontend intercepts: it calls history.back().
    _enqueue_action("go_back", "app://back", "the previous screen")
    return "Going back."


def set_sleeper_tab(tab: str) -> str:
    """Switch the My Team (Sleeper) view to a tab: lineup, waivers, or league.

    The user must already be on the My Team page, or this will navigate there
    first.
    """
    t = (tab or "lineup").strip().lower()
    if t not in ("lineup", "waivers", "league"):
        return f"'{tab}' is not a My Team tab. Use lineup, waivers, or league."
    url = format_url("MY_TEAM", tab=t.upper())
    _enqueue_action("set_sleeper_tab", url, f"my team, {t} tab")
    return f"Switching My Team to the {t} tab."


def resolve_player_name(name: str) -> str:
    """Look up a player's id and team without opening a page.

    Use this when you need to confirm a name resolves to exactly one player
    before calling open_player or open_compare, or when the user asks "is there
    a player named X" without wanting to navigate.
    """
    pid, note = _resolve_player(name)
    if not pid:
        return note
    return f"{name} -> player_id {pid}."


# ---------------------------------------------------------------------------
# Tool registry: names the pi extension discovers via /agent/tools
# ---------------------------------------------------------------------------

# (name, function) in the order the model should see them. Navigation first,
# then interaction, then the resolver.
SCREEN_ACTION_TOOLS: tuple[tuple[str, Any], ...] = (
    ("open_screen", open_screen),
    ("open_player", open_player),
    ("open_compare", open_compare),
    ("open_team", open_team),
    ("open_game", open_game),
    ("add_to_compare", add_to_compare),
    ("go_back", go_back),
    ("set_sleeper_tab", set_sleeper_tab),
    ("resolve_player_name", resolve_player_name),
)

# JSON Schema types the tool signatures use. Mirrors agent/tools._JSON_TYPES
# but adds list[str] (already there) — kept local so this module is standalone.
_JSON_TYPES: dict[Any, dict] = {
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
    bool: {"type": "boolean"},
    list[str]: {"type": "array", "items": {"type": "string"}},
}