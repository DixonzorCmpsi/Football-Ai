"""Screen-action tools: let the in-app agent move the user's browser.

Every navigation a mouse can perform has a tool here: open a page, jump to a
player, start a comparison, go back. The tools **return** a ``ScreenResult``
carrying the web address; they never touch a queue. The route that serves pi's
tool calls knows the conversation from the session token (``ProxyGrant``), so it
pushes the action onto that conversation's queue, and the chat SSE stream
delivers it to the browser. A tool therefore cannot claim "Opening ..." unless
the route actually queued the movement: the tool either resolved a path or it
returned text saying why it didn't.

Design:

* **The backend resolves names; the browser applies the URL.** A tool never
  trusts the model's spelling of an id. It calls the same ``/players/search``
  endpoint the MCP tools use, and an ambiguous name returns a list to pick
  from rather than a silent wrong pick.
* **Only recognized URLs.** ``format_url`` mirrors the frontend's
  ``formatAppUrl`` (``Dashboard/predictor-frontend/src/lib/appUrl.ts``); that
  parser is the final gatekeeper in the browser.
* **Ambiguous = nothing moves.** A name matching two or more players means no
  path at all, and the text lists candidates to disambiguate.

Registered in ``agent/tools.py`` after the MCP data tools. Not published to the
MCP server: Claude Code has no browser to move.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# --- Team abbreviations ------------------------------------------------------
# Matches what GET /schedule/{week} returns (the Rams are "LA" there), which is
# the same vocabulary /team/{TEAM} and /game/{away}/{home} accept. LAR/Los
# Angeles Rams inputs normalize to LA; bare LOSANGELES is deliberately absent
# because it is ambiguous between the Rams and the Chargers.
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
    "LA": "LA", "LAR": "LA", "RAMS": "LA", "LOSANGELESRAMS": "LA",
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

# Screens the agent can open. Mirrors the frontend's AppLocation views.
NAV_SCREENS = {
    "schedule", "lookup", "compare", "trending", "picks", "playoffs",
    "tiers", "teams", "ranks", "my_team",
}
# Tabs only valid with my_team (see open_screen).
MY_TEAM_TABS = {"lineup", "waivers", "league"}

MAX_COMPARE_IDS = 4

# A gsis id is exactly "00-" plus 7 digits. Anything else is a name and goes
# through search.
GSIS_ID_RE = re.compile(r"^00-\d{7}$")

# What the user asks for -> the tab that shows it. The same words work for the
# full player page and the card over a game, so "stats", "a chart", "what's going
# on with him" and "his odds" land on the right tab either way.
PLAYER_VIEW_WORDS = {
    "stats": "stats", "table": "stats", "log": "stats", "game_log": "stats", "gamelog": "stats", "numbers": "stats",
    "visuals": "visuals", "visual": "visuals", "charts": "visuals", "chart": "visuals", "graphs": "visuals", "trends": "visuals",
    "storylines": "storylines", "storyline": "storylines", "news": "storylines", "situation": "storylines", "updates": "storylines",
    "vegas": "vegas", "odds": "vegas", "props": "vegas", "lines": "vegas", "betting": "vegas",
}
PAGE_VIEW = {"stats": "table", "visuals": "visual", "storylines": "storylines"}
CARD_TAB = {"stats": "log", "visuals": "visuals", "storylines": "storylines", "vegas": "vegas"}


def _player_view(view: str) -> str | None:
    """"stats", "a visual", "his game log", "latest news" -> the tab. None if no word fits."""
    text = (view or "stats").strip().lower()
    joined = re.sub(r"[\s-]+", "_", text)
    if joined in PLAYER_VIEW_WORDS:
        return PLAYER_VIEW_WORDS[joined]
    for word in re.findall(r"[a-z]+", text):
        if word in PLAYER_VIEW_WORDS:
            return PLAYER_VIEW_WORDS[word]
    return None

SCREEN_LABELS = {
    "schedule": "the weekly schedule",
    "lookup": "player lookup",
    "compare": "the comparison tray",
    "trending": "trending players",
    "picks": "your saved picks",
    "playoffs": "the playoff picture",
    "tiers": "the tier list",
    "teams": "the team index",
    "ranks": "the start/sit ranks board",
    "my_team": "your Sleeper roster",
}
_SCREEN_VIEW = {
    "schedule": "SCHEDULE", "lookup": "LOOKUP", "compare": "COMPARE",
    "trending": "TRENDING", "picks": "PICKS", "playoffs": "PLAYOFFS",
    "tiers": "TIERS", "teams": "TEAMS", "ranks": "GAME_RANKS",
    "my_team": "MY_TEAM",
}


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScreenResult:
    """What a screen tool returns: text for the model, plus the movement.

    ``path`` is None when nothing should move (invalid input, ambiguous name,
    no game that week). The route pushes non-None results onto the
    conversation's action queue.
    """

    text: str
    path: str | None = None
    label: str | None = None
    tool: str | None = None


# ---------------------------------------------------------------------------
# Action queue
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
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


MAX_PENDING_PER_CONVERSATION = 5
ACTION_TTL_SECONDS = 120.0

# conversation_id -> list of (monotonic stamp, ScreenAction). A list under a
# lock rather than queue.Queue because of two behaviours a Queue doesn't give:
# an oldest-dropped cap and expiry filtering on drain.
_actions: dict[str, list[tuple[float, ScreenAction]]] = {}
_actions_guard = threading.Lock()


def push_action(conversation_id: str, path: str, label: str, tool: str = "") -> None:
    """Queue one movement for this conversation; the oldest drops past the cap."""
    if not conversation_id or not path:
        return
    with _actions_guard:
        entries = _actions.setdefault(conversation_id, [])
        entries.append((time.monotonic(), ScreenAction(url=path, label=label, tool=tool)))
        while len(entries) > MAX_PENDING_PER_CONVERSATION:
            entries.pop(0)


def drain_actions(conversation_id: str) -> list[ScreenAction]:
    """Remove and return every unexpired queued action for this conversation."""
    now = time.monotonic()
    with _actions_guard:
        entries = _actions.pop(conversation_id, None)
        if not entries:
            return []
    return [a for ts, a in entries if now - ts < ACTION_TTL_SECONDS]


def discard_actions(conversation_id: str) -> None:
    """Drop leftovers from an earlier question so its stale actions never replay."""
    with _actions_guard:
        _actions.pop(conversation_id, None)


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
    if v == "LOOKUP":
        return "/lookup"
    if v == "COMPARE":
        ids = kw.get("ids", [])
        return f"/compare?ids={','.join(ids)}" if ids else "/compare"
    if v == "HISTORY":
        show = kw.get("show")
        return f"/player/{_enc(kw['player_id'])}" + (f"?view={show}" if show and show != "storylines" else "")
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
    if v == "GAME":
        params = []
        if kw.get("week"):
            params.append(f"week={int(kw['week'])}")
        if kw.get("player"):
            params.append(f"player={_enc(kw['player'])}")
            if kw.get("card") and kw["card"] != "log":
                params.append(f"tab={kw['card']}")
        suffix = f"?{'&'.join(params)}" if params else ""
        return f"/game/{_enc(kw['away'])}/{_enc(kw['home'])}{suffix}"
    return "/"


# ---------------------------------------------------------------------------
# Backend helpers — same HTTP surface the MCP tools use
# ---------------------------------------------------------------------------

_API_BASE = os.getenv("FOOTBALL_AI_API", "http://127.0.0.1:8000").rstrip("/")


def _get(path: str, params: dict | None = None) -> Any:
    """GET the backend. 404 -> None; other failures raise, so a tool can say so."""
    resp = httpx.get(f"{_API_BASE}{path}", params=params, timeout=10.0)
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        resp.raise_for_status()
    return resp.json()


def _current_week() -> int:
    data = _get("/current_week") or {}
    try:
        return int(data.get("week") or 1)
    except (TypeError, ValueError):
        return 1


def _resolve_team(name: str) -> str | None:
    """Accept 'JAX', 'Jaguars', 'jacksonville' and return the schedule's abbr."""
    key = (name or "").strip().upper().replace(".", "").replace(" ", "")
    return TEAM_ALIASES.get(key)


def _resolve_player(name_or_id: str) -> tuple[str | None, str | None, str]:
    """Accept a gsis id or a name. Returns (player_id, player_name, note).

    An exact case-insensitive match wins, then a single result. Two or more
    candidates means (None, None, listing) and the caller navigates nowhere.
    """
    probe = (name_or_id or "").strip()
    if not probe:
        return None, None, "No player given."
    if GSIS_ID_RE.match(probe):
        return probe, probe, ""

    results = _get("/players/search", {"q": probe}) or []
    if not results:
        return None, None, f"No player matched '{probe}'."

    def name_of(r: dict) -> str:
        return (r.get("player_name") or "").strip()

    exact = [r for r in results if name_of(r).lower() == probe.lower()]
    if len(exact) == 1:
        return exact[0]["player_id"], name_of(exact[0]), ""
    if len(results) == 1:
        return results[0]["player_id"], name_of(results[0]), ""

    pool = exact or results
    listing = ", ".join(
        f"{name_of(r)} ({r.get('position')}, {r.get('team_abbr')})"
        for r in pool[:5]
    )
    return None, None, (
        f"'{probe}' matched {len(pool)} players: {listing}. "
        "Pick one and call the tool again with the full name."
    )


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

def open_screen(screen: str, tab: str = "") -> ScreenResult:
    """Send the user to a named screen: schedule, lookup, compare, trending,
    picks, playoffs, tiers, teams, ranks, or my_team.

    Use this when the user asks to go somewhere or see something, or when the
    page answers better than words. For a specific player use open_player; for
    a game use open_game. ``tab`` applies to my_team only: lineup, waivers, or
    league.
    """
    key = (screen or "").strip().lower().replace("-", "_").replace(" ", "_")
    if key not in NAV_SCREENS:
        return ScreenResult(
            f"'{screen}' is not a screen I can open. "
            f"Choose from: {', '.join(sorted(NAV_SCREENS))}."
        )
    if key == "my_team":
        t = (tab or "lineup").strip().lower()
        if t not in MY_TEAM_TABS:
            return ScreenResult(
                f"'{tab}' is not a my_team tab. Use lineup, waivers, or league."
            )
        path = format_url("MY_TEAM", tab=t.upper())
        return ScreenResult(
            text=f"Opened your Sleeper roster, {t} tab.",
            path=path,
            label=f"My team, {t} tab",
            tool="open_screen",
        )
    path = format_url(_SCREEN_VIEW[key])
    label = SCREEN_LABELS[key]
    return ScreenResult(
        text=f"Opened {label}.", path=path, label=label, tool="open_screen"
    )


def open_player(player: str, view: str = "stats") -> ScreenResult:
    """Open a player's full page, on the tab that answers the question.

    ``view``: "stats" (game-log table, the default: "show me his stats"),
    "visuals" (charts: "show me a visual", "his trend"), or "storylines" (news:
    "what's going on with him", "any updates"). Accepts a name or a gsis id; an
    ambiguous name opens nothing and lists the matches. If the user is on, or
    asking about, a game, prefer open_player_card, which keeps them on the game.
    """
    chosen = _player_view(view)
    if chosen not in PAGE_VIEW:
        return ScreenResult("view is stats, visuals or storylines. (Odds and props are on the card: use open_player_card with tab vegas.)")
    pid, resolved_name, note = _resolve_player(player)
    if not pid:
        return ScreenResult(note or f"Could not resolve '{player}'.")
    path = format_url("HISTORY", player_id=pid, show=PAGE_VIEW[chosen])
    return ScreenResult(
        text=f"Opened {resolved_name}'s page on {chosen}.",
        path=path,
        label=f"{resolved_name} · {chosen}",
        tool="open_player",
    )


def open_player_card(player: str, tab: str = "stats", week: int = 0) -> ScreenResult:
    """Open a player's card over his game's page, on the tab that answers the question.

    Use it for "take me to the Jets game and show me AD Mitchell's stats": the
    user stays on the game with the card open. ``tab``: "stats" (game log),
    "visuals" (charts), "storylines" (news, "what's his situation"), or "vegas"
    (odds, props, the game line). ``week`` 0 is the current week. The game is
    found from the player's team, so there's no need to open the game first.
    """
    chosen = _player_view(tab)
    if chosen not in CARD_TAB:
        return ScreenResult("tab is stats, visuals, storylines or vegas.")
    pid, resolved_name, note = _resolve_player(player)
    if not pid:
        return ScreenResult(note or f"Could not resolve '{player}'.")
    team = _team_of(pid, resolved_name)
    if not team:
        return ScreenResult(f"Couldn't tell which team {resolved_name} plays for; use open_player instead.")
    wk = week or _current_week()
    games = _get(f"/schedule/{wk}") or []
    row = next((g for g in games if team in (g.get("home_team"), g.get("away_team"))), None)
    if not row:
        return ScreenResult(
            f"{resolved_name}'s team ({team}) has no game in week {wk}; use open_player to show his page instead."
        )
    away, home = row.get("away_team"), row.get("home_team")
    path = format_url("GAME", away=away, home=home, week=wk, player=pid, card=CARD_TAB[chosen])
    return ScreenResult(
        text=f"Opened {away} @ {home} (week {wk}) with {resolved_name}'s card on {chosen}.",
        path=path,
        label=f"{resolved_name} · {chosen} ({away} @ {home})",
        tool="open_player_card",
    )


def _team_of(player_id: str, name: str) -> str | None:
    """The player's current team, from the same search the name came from."""
    for row in _get("/players/search", {"q": name}) or []:
        if row.get("player_id") == player_id:
            return row.get("team_abbr")
    return None


def open_compare(players: list[str]) -> ScreenResult:
    """Open the side-by-side comparison view for 2 to 4 players.

    Pass full names or gsis ids. Each is resolved independently; if any name is
    ambiguous the whole call is rejected so the user never lands on a partial
    comparison.
    """
    if not players:
        return ScreenResult("Pass at least two players to compare.")
    if len(players) > MAX_COMPARE_IDS:
        return ScreenResult(
            f"Compare holds at most {MAX_COMPARE_IDS} players; you passed {len(players)}."
        )

    ids: list[str] = []
    names: list[str] = []
    for p in players:
        pid, resolved_name, note = _resolve_player(p)
        if not pid:
            return ScreenResult(f"Cannot compare: {note}")
        ids.append(pid)
        names.append(resolved_name or p)
    path = format_url("COMPARE", ids=ids)
    label = "Compare: " + " vs ".join(names)
    return ScreenResult(
        text=f"Opened the comparison of {' vs '.join(names)} on the user's screen.",
        path=path,
        label=label,
        tool="open_compare",
    )


def open_team(team: str, tab: str = "overview") -> ScreenResult:
    """Open a team's page (offense overview or team builder).

    ``team`` accepts an abbreviation ("BUF"), a nickname ("Bills") or a city
    ("Buffalo"). ``tab`` is "overview" (default) or "builder".
    """
    abbr = _resolve_team(team)
    if not abbr:
        return ScreenResult(
            f"'{team}' is not a recognized team. Use an abbreviation like BUF, KC, or SF."
        )
    tab_lower = (tab or "overview").strip().lower()
    if tab_lower not in ("overview", "builder"):
        tab_lower = "overview"
    path = format_url("TEAM_PAGE", team=abbr, tab=tab_lower)
    return ScreenResult(
        text=f"Opened the {abbr} team page, {tab_lower} tab.",
        path=path,
        label=f"{abbr} team ({tab_lower})",
        tool="open_team",
    )


def open_game(team: str, week: int = 0) -> ScreenResult:
    """Open the matchup page for the game a team plays in a given week.

    ``team`` accepts an abbreviation ("BUF"), a nickname ("Bills") or a city
    ("Buffalo"). ``week`` 0 means the current week, so you never need to know
    who is home. A bye week opens nothing and says so.
    """
    abbr = _resolve_team(team)
    if not abbr:
        return ScreenResult(
            f"'{team}' is not a recognized team. Use an abbreviation like BUF, KC, or SF."
        )
    wk = week or _current_week()
    # /schedule/{week}, not ?week=: the query form 404s, which read as "bye week".
    games = _get(f"/schedule/{wk}") or []
    row = next(
        (g for g in games if g.get("home_team") == abbr or g.get("away_team") == abbr),
        None,
    )
    if not row:
        return ScreenResult(f"The {abbr} have no game in week {wk} (bye week).")
    away, home = row.get("away_team"), row.get("home_team")
    path = format_url("GAME", away=away, home=home, week=wk)
    return ScreenResult(
        text=f"Opened {away} @ {home} (week {wk}).",
        path=path,
        label=f"{away} @ {home}",
        tool="open_game",
    )


def add_to_compare(player: str) -> ScreenResult:
    """Add one player to the comparison tray the user is building.

    Use this when the user says "add him to my comparison" rather than naming
    the whole set at once. The browser merges the id into its existing list.
    For a fresh comparison of named players, prefer open_compare.
    """
    pid, resolved_name, note = _resolve_player(player)
    if not pid:
        return ScreenResult(note or f"Could not resolve '{player}'.")
    path = format_url("COMPARE", ids=[pid])
    return ScreenResult(
        text=f"Added {resolved_name} to the comparison on the user's screen.",
        path=path,
        label=f"Compare {resolved_name}",
        tool="add_to_compare",
    )


def go_back() -> ScreenResult:
    """Send the user back one screen, like the browser's Back button.

    Use it when the user says "go back" after you (or they) changed screens.
    """
    return ScreenResult(
        text="Went back one screen.",
        path="app://back",
        label="Back to the previous screen",
        tool="go_back",
    )


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

# (name, function), after the data tools in agent/tools.py's list. Fewer,
# non-overlapping tools means fewer wrong picks by small models: resolve_player
# and set_sleeper_tab were removed in favour of search_players and open_screen.
SCREEN_ACTION_TOOLS: tuple[tuple[str, Any], ...] = (
    ("open_screen", open_screen),
    ("open_player", open_player),
    ("open_compare", open_compare),
    ("open_team", open_team),
    ("open_game", open_game),
    ("open_player_card", open_player_card),
    ("add_to_compare", add_to_compare),
    ("go_back", go_back),
)

SCREEN_TOOL_NAMES: frozenset[str] = frozenset(name for name, _ in SCREEN_ACTION_TOOLS)