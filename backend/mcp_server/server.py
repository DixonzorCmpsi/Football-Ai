"""MCP server over the Football-Ai backend.

Exposes the app's projections, matchup context, betting lines and Sleeper
analysis as MCP tools, so an agent can answer "should I start Purdy this week"
against the same model the UI uses.

It talks to the running FastAPI backend over HTTP rather than importing it, so
one server can point at a local dev instance or a deployed one, and the agent
never holds the model, the DB connection, or a 2 GB process.

Run:
    python -m mcp_server.server                  # stdio, for Claude Code / Desktop
    FOOTBALL_AI_API=http://host:8000 python -m mcp_server.server

Tools deliberately return prose-shaped text rather than raw JSON: the REST
responses are built for a UI (a single matchup is ~31 player cards with a dozen
prop rows each) and handing that over verbatim buries the few numbers that
matter. `get_raw` is the escape hatch when the full payload is wanted.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:  # pragma: no cover - depends on the interpreter
    # The tool functions below are also imported by the API process (see
    # backend/agent/tools.py), which serves them to the in-app agent over HTTP
    # and has no business requiring the MCP SDK. @mcp.tool() returns the
    # undecorated function either way, so a no-op registrar changes nothing
    # except that `main()` refuses to start a server it cannot build.
    FastMCP = None  # type: ignore[assignment]

from .formatting import (
    player_line,
    props_summary,
    summarize_history,
    summarize_matchup,
    summarize_roster_analysis,
    summarize_schedule,
    summarize_search,
    summarize_storylines,
    summarize_waivers,
)

def _normalize_base(url: str) -> str:
    """Prefer 127.0.0.1 over the literal "localhost".

    On Windows, resolving "localhost" tries ::1 first and falls back to IPv4,
    which costs ~200ms on EVERY request. Measured against this backend:
    localhost 216ms vs 127.0.0.1 16ms for the same /health call -- a 13x
    difference that is entirely name resolution, not the server.
    """
    url = (url or "").rstrip("/")
    return url.replace("//localhost:", "//127.0.0.1:").replace("//localhost/", "//127.0.0.1/")


API_BASE = _normalize_base(os.getenv("FOOTBALL_AI_API", "http://127.0.0.1:8000"))
TIMEOUT = float(os.getenv("FOOTBALL_AI_TIMEOUT", "30"))
CACHE_ENABLED = os.getenv("FOOTBALL_AI_CACHE", "1") not in ("0", "false", "False")

# How long a response stays reusable, by path prefix. An agent exploring a
# question re-asks the same things (the current week, a player's id) many times
# in a row; without this every tool call pays full latency again. Values are
# deliberately short for anything that moves during a game week.
_CACHE_TTL = (
    ("/current_week", 600.0),
    ("/players/search", 600.0),
    ("/schedule/", 300.0),
    ("/player/history/", 300.0),
    ("/player/", 60.0),
    ("/matchup/", 60.0),
    ("/sleeper/", 60.0),
    ("/parlays/", 120.0),
    ("/health", 10.0),
)
_DEFAULT_TTL = 30.0

class _NoRegistrar:
    """Stands in for FastMCP when the SDK is absent. Registers nothing."""

    def tool(self, *args, **kwargs):
        return lambda fn: fn

    def run(self, *args, **kwargs):
        raise RuntimeError(
            "The MCP SDK is not installed in this interpreter. "
            "Install it with `pip install 'mcp>=1.9,<2'` to run the MCP server."
        )


mcp = FastMCP("football-ai") if FastMCP is not None else _NoRegistrar()

# One pooled client for the process. The first version built a new httpx.Client
# per call, so every request paid a fresh TCP handshake on top of the name
# resolution above.
_client: httpx.Client | None = None
_cache: dict[tuple, tuple[float, Any]] = {}


def _http() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(
            timeout=TIMEOUT,
            limits=httpx.Limits(max_keepalive_connections=4, max_connections=8),
            headers={"User-Agent": "football-ai-mcp"},
        )
    return _client


def _ttl_for(path: str) -> float:
    for prefix, ttl in _CACHE_TTL:
        if path.startswith(prefix):
            return ttl
    return _DEFAULT_TTL


def _cache_key(path: str, params: dict | None) -> tuple:
    return (path, tuple(sorted((params or {}).items())))


def clear_cache() -> None:
    """Drop every cached response. Used by the refresh tool and by tests."""
    _cache.clear()


class BackendError(RuntimeError):
    pass


def _get(path: str, params: dict | None = None, use_cache: bool = True) -> Any:
    key = _cache_key(path, params)
    if CACHE_ENABLED and use_cache:
        hit = _cache.get(key)
        if hit and (time.monotonic() - hit[0]) < _ttl_for(path):
            return hit[1]

    url = f"{API_BASE}{path}"
    try:
        resp = _http().get(url, params=params)
    except httpx.HTTPError as exc:
        raise BackendError(
            f"Could not reach the Football-Ai backend at {API_BASE} ({exc}). "
            "Is it running?"
        ) from exc
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        detail = resp.text[:300]
        try:
            detail = resp.json().get("detail", detail)
        except Exception:
            pass
        raise BackendError(f"Backend returned {resp.status_code}: {detail}")

    data = resp.json()
    if CACHE_ENABLED and use_cache:
        _cache[key] = (time.monotonic(), data)
    return data


def _post(path: str, payload: dict) -> Any:
    url = f"{API_BASE}{path}"
    try:
        resp = _http().post(url, json=payload)
    except httpx.HTTPError as exc:
        raise BackendError(f"Could not reach the backend at {API_BASE} ({exc}).") from exc
    if resp.status_code >= 400:
        raise BackendError(f"Backend returned {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _current_week() -> int:
    data = _get("/current_week") or {}
    try:
        return int(data.get("week") or 1)
    except (TypeError, ValueError):
        return 1


def _resolve_player(name_or_id: str) -> tuple[str | None, str]:
    """Accept a gsis id or a name. Returns (player_id, note).

    Agents have names, not `00-0037834`. Resolving here means the caller never
    has to chain a search before every question, and an ambiguous name comes
    back as a list to choose from rather than a silent wrong pick.
    """
    probe = (name_or_id or "").strip()
    if not probe:
        return None, "No player given."
    if probe.startswith("00-") or probe.replace("-", "").isdigit():
        return probe, ""

    results = _get("/players/search", {"q": probe}) or []
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
    listing = summarize_search(pool)
    return None, f"'{probe}' is ambiguous. {listing}"


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

@mcp.tool()
def get_status() -> str:
    """Backend health: current week, whether models and data are loaded, freshness."""
    h = _get("/health") or {}
    lines = [
        f"API: {API_BASE}",
        f"status: {h.get('status')}",
        f"current week: {h.get('current_week')}",
        f"models loaded: {h.get('models_loaded')}",
        f"database connected: {h.get('db_connection_string_set')}",
        f"data loaded at: {h.get('data_loaded_at')}",
        f"injuries updated: {h.get('injuries_updated_at')}",
        f"storylines updated: {h.get('storylines_updated_at')}",
    ]
    counts = h.get("data_counts") or {}
    if counts:
        lines.append("row counts: " + ", ".join(f"{k}={v}" for k, v in counts.items()))
    return "\n".join(lines)


@mcp.tool()
def refresh() -> str:
    """Drop cached responses so the next call re-reads the backend.

    Responses are cached briefly (10s-10min depending on how fast the data
    moves) because an agent re-asks the same things while working through a
    question. Call this after an ETL run, or when a number looks stale.
    """
    n = len(_cache)
    clear_cache()
    return f"Cleared {n} cached response(s). The next call will hit the backend."


@mcp.tool()
def search_players(query: str) -> str:
    """Find players by (partial) name. Returns names, teams, positions and ids."""
    if not query or not query.strip():
        return "Provide a name or part of one."
    return summarize_search(_get("/players/search", {"q": query.strip()}) or [])


@mcp.tool()
def get_schedule(week: int = 0) -> str:
    """The NFL schedule for a week, with scores where games are final.

    Pass week=0 (the default) for the current week.
    """
    wk = week or _current_week()
    return summarize_schedule(_get(f"/schedule/{wk}") or [], wk)


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

@mcp.tool()
def get_player_projection(player: str, week: int = 0, include_props: bool = True) -> str:
    """Projected fantasy points for a player, with betting lines and injury status.

    `player` accepts a name ("Brock Purdy") or a gsis id ("00-0037834").
    """
    wk = week or _current_week()
    pid, note = _resolve_player(player)
    if not pid:
        return note
    card = _get(f"/player/{pid}", {"week": wk})
    if not card:
        return f"No projection found for '{player}' in week {wk}."

    lines = [player_line(card)]
    extra = []
    if card.get("overunder") is not None:
        extra.append(f"game O/U {card['overunder']}")
    if card.get("spread") is not None:
        extra.append(f"spread {card['spread']}")
    if card.get("implied_total") is not None:
        extra.append(f"team implied total {card['implied_total']}")
    if extra:
        lines.append("  " + ", ".join(extra))
    if card.get("average_points") is not None:
        lines.append(f"  season avg {card.get('average_points')}, "
                     f"4wk avg {card.get('rolling_4wk_avg')}")
    if include_props:
        summary = props_summary(card.get("props") or [], limit=14)
        lines.append(f"  props: {summary}" if summary
                     else "  props: none offered for this player")
    return "\n".join(lines)


@mcp.tool()
def get_player_history(player: str, limit: int = 12) -> str:
    """Recent weekly game log for a player, newest first."""
    pid, note = _resolve_player(player)
    if not pid:
        return note
    return summarize_history(_get(f"/player/history/{pid}"), limit=limit)


@mcp.tool()
def get_player_storylines(player: str, limit: int = 5) -> str:
    """Latest news storylines about a player."""
    pid, note = _resolve_player(player)
    if not pid:
        return note
    data = _get(f"/player/{pid}/storylines") or {}
    rows = data.get("storylines") if isinstance(data, dict) else data
    return summarize_storylines(rows or [], limit=limit)


@mcp.tool()
def compare_players(players: list[str], week: int = 0) -> str:
    """Side-by-side projections for several players. Use for start/sit calls."""
    if not players:
        return "Give at least one player."
    wk = week or _current_week()
    out, unresolved = [], []
    for name in players:
        pid, note = _resolve_player(name)
        if not pid:
            unresolved.append(note)
            continue
        card = _get(f"/player/{pid}", {"week": wk})
        if card:
            out.append(card)
    if not out:
        return "\n".join(unresolved) or "No players resolved."

    out.sort(key=lambda c: float(c.get("prediction") or 0), reverse=True)
    lines = [f"Week {wk}, highest projection first:"]
    for card in out:
        lines.append("  " + player_line(card, include_props=True))
    lines.extend(unresolved)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Matchups
# ---------------------------------------------------------------------------

@mcp.tool()
def get_matchup(home_team: str, away_team: str, week: int = 0, top_n: int = 8) -> str:
    """Game context: line, weather, top projected players per side, and injuries.

    Teams are abbreviations ("SF", "KC", "LA").
    """
    wk = week or _current_week()
    home, away = home_team.strip().upper(), away_team.strip().upper()
    data = _get(f"/matchup/{wk}/{home}/{away}")
    # The endpoint answers 200 with empty rosters for teams that do not exist or
    # do not play each other, so an empty board is the real "not found" signal.
    if not data or not (data.get("home_roster") or data.get("away_roster")):
        return (f"No matchup found for {away} @ {home} in week {wk}. "
                "Check the team abbreviations and that these two play each other "
                "that week (get_schedule lists them).")
    return summarize_matchup(data, wk, home, away, top_n=top_n)


@mcp.tool()
def get_matchup_insights(home_team: str, away_team: str, week: int = 0) -> str:
    """Model-derived narrative insights for a specific game."""
    wk = week or _current_week()
    home, away = home_team.strip().upper(), away_team.strip().upper()
    data = _get(f"/matchup/{wk}/{home}/{away}/insights")
    if not data:
        return f"No insights available for {away} @ {home} in week {wk}."
    return json.dumps(data, indent=2)[:6000]


@mcp.tool()
def get_parlays(week: int = 0, home_team: str = "", away_team: str = "") -> str:
    """Suggested parlay legs for a week, optionally narrowed to one game."""
    wk = week or _current_week()
    if home_team and away_team:
        path = f"/parlays/{wk}/{home_team.strip().upper()}/{away_team.strip().upper()}"
    else:
        path = f"/parlays/{wk}"
    data = _get(path)
    if not data:
        return f"No parlay suggestions for week {wk}."
    return json.dumps(data, indent=2)[:6000]


# ---------------------------------------------------------------------------
# Sleeper
# ---------------------------------------------------------------------------

@mcp.tool()
def sleeper_find_leagues(username: str, season: int = 0) -> str:
    """List a Sleeper user's NFL leagues. Public API - username only, no password."""
    if not username.strip():
        return "Give a Sleeper username."
    params = {"season": season} if season else None
    data = _get(f"/sleeper/user/{username.strip()}", params)
    if not data:
        return f"No Sleeper user named '{username}'."
    leagues = data.get("leagues") or []
    user = data.get("user") or {}
    if not leagues:
        return (f"{user.get('display_name', username)} has no NFL leagues for "
                f"{data.get('season')}. Sleeper keeps seasons separately - try another year.")
    lines = [f"{user.get('display_name', username)} - {len(leagues)} league(s):"]
    for lg in leagues:
        lines.append(f"  {lg.get('name')} [id {lg.get('league_id')}] "
                     f"{lg.get('total_rosters')} teams, {lg.get('scoring_type')}, "
                     f"{lg.get('season')}")
    return "\n".join(lines)


@mcp.tool()
def sleeper_list_teams(league_id: str) -> str:
    """Teams in a Sleeper league, with the roster_id needed to analyze one."""
    data = _get(f"/sleeper/league/{league_id.strip()}")
    if not data:
        return f"League {league_id} not found."
    league = data.get("league") or {}
    teams = data.get("teams") or []
    lines = [f"{league.get('name')} ({league.get('scoring_type')}, "
             f"{league.get('total_rosters')} teams):"]
    for t in teams:
        lines.append(f"  roster {t.get('roster_id')}: {t.get('team_name')} "
                     f"({t.get('display_name')}) {t.get('wins')}-{t.get('losses')}"
                     f"{'-' + str(t['ties']) if t.get('ties') else ''}, "
                     f"{t.get('player_count')} players")
    return "\n".join(lines)


@mcp.tool()
def sleeper_analyze_roster(league_id: str, roster_id: int, week: int = 0) -> str:
    """Project a Sleeper roster and compare the model's lineup to the saved one.

    Use sleeper_list_teams first to get the roster_id.
    """
    wk = week or _current_week()
    data = _get(f"/sleeper/league/{league_id.strip()}/roster/{roster_id}/analysis",
                {"week": wk})
    if not data:
        return f"No analysis returned for roster {roster_id} in league {league_id}."
    return summarize_roster_analysis(data)


@mcp.tool()
def sleeper_waiver_targets(league_id: str, week: int = 0, limit: int = 15) -> str:
    """Free agents in a Sleeper league, ranked by this app's projection."""
    wk = week or _current_week()
    data = _get(f"/sleeper/league/{league_id.strip()}/waivers",
                {"week": wk, "limit": max(limit, 1)})
    return summarize_waivers(data or {}, limit=limit)


# ---------------------------------------------------------------------------
# Raw escape hatch
# ---------------------------------------------------------------------------

@mcp.tool()
def get_raw(path: str, params_json: str = "") -> str:
    """Call a backend GET endpoint directly and return raw JSON.

    For data the shaped tools do not cover (e.g. /team/SF/offense, /tier_list).
    `path` must start with '/'. Response is truncated to keep context usable.
    """
    if not path.startswith("/"):
        return "path must start with '/'."
    params = None
    if params_json.strip():
        try:
            params = json.loads(params_json)
        except json.JSONDecodeError as exc:
            return f"params_json is not valid JSON: {exc}"
    data = _get(path, params)
    if data is None:
        return f"No data at {path} (404)."
    text = json.dumps(data, indent=2)
    if len(text) > 12000:
        return text[:12000] + f"\n... truncated ({len(text)} chars total)"
    return text


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
