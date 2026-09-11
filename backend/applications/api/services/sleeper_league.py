"""Sleeper league import and analysis.

Lets a user point Football-Ai at their actual fantasy team: resolve a Sleeper
username, list their leagues, then score the roster and the waiver wire with the
same projections the rest of the app uses.

Sleeper's read API is public and unauthenticated, so nothing here needs or
stores a credential. Responses are cached briefly because a league's roster does
not change between two clicks, and the player universe changes daily at most.
"""

from __future__ import annotations

import time
from typing import Any

import requests

from ..config import logger
from ..state import model_data

SLEEPER_BASE = "https://api.sleeper.app/v1"
HEADERS = {"User-Agent": "Football-Ai/1.0"}
TIMEOUT = 8

# (value, fetched_at) keyed by url
_CACHE: dict[str, tuple[Any, float]] = {}
_CACHE_TTL = 300.0

# Positions we actually project. Sleeper rosters also carry K/DEF, which the
# model has no opinion on -- they are returned but never ranked or recommended.
PROJECTED_POSITIONS = {"QB", "RB", "WR", "TE"}
FLEX_POSITIONS = {"RB", "WR", "TE"}


class SleeperError(RuntimeError):
    """A Sleeper lookup failed in a way the caller should surface to the user."""


def _get(path: str, ttl: float = _CACHE_TTL):
    url = f"{SLEEPER_BASE}{path}"
    now = time.time()
    hit = _CACHE.get(url)
    if hit and (now - hit[1]) < ttl:
        return hit[0]
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    except requests.RequestException as exc:
        raise SleeperError(f"Could not reach Sleeper: {exc}") from exc
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise SleeperError(f"Sleeper returned {resp.status_code} for {path}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise SleeperError("Sleeper returned a malformed response") from exc
    _CACHE[url] = (data, now)
    return data


def resolve_user(username: str) -> dict | None:
    """Sleeper username (or numeric user id) -> user record."""
    if not username or not str(username).strip():
        return None
    user = _get(f"/user/{str(username).strip()}")
    if not user:
        return None
    return {
        "user_id": user.get("user_id"),
        "username": user.get("username"),
        "display_name": user.get("display_name"),
        "avatar": user.get("avatar"),
    }


def get_user_leagues(user_id: str, season: int) -> list[dict]:
    leagues = _get(f"/user/{user_id}/leagues/nfl/{season}") or []
    out = []
    for lg in leagues:
        out.append({
            "league_id": lg.get("league_id"),
            "name": lg.get("name"),
            "season": lg.get("season"),
            "total_rosters": lg.get("total_rosters"),
            "status": lg.get("status"),
            "scoring_type": _scoring_label(lg.get("scoring_settings") or {}),
            "roster_positions": lg.get("roster_positions") or [],
            "avatar": lg.get("avatar"),
        })
    return out


def _scoring_label(scoring: dict) -> str:
    """PPR / Half PPR / Standard, from the league's reception value."""
    try:
        rec = float(scoring.get("rec", 0) or 0)
    except (TypeError, ValueError):
        rec = 0.0
    if rec >= 1.0:
        return "PPR"
    if rec > 0:
        return "Half PPR"
    return "Standard"


def get_league(league_id: str) -> dict | None:
    lg = _get(f"/league/{league_id}")
    if not lg:
        return None
    return {
        "league_id": lg.get("league_id"),
        "name": lg.get("name"),
        "season": lg.get("season"),
        "total_rosters": lg.get("total_rosters"),
        "roster_positions": lg.get("roster_positions") or [],
        "scoring_type": _scoring_label(lg.get("scoring_settings") or {}),
    }


def get_rosters(league_id: str) -> list[dict]:
    return _get(f"/league/{league_id}/rosters") or []


def get_league_users(league_id: str) -> list[dict]:
    return _get(f"/league/{league_id}/users") or []


def roster_owners(league_id: str) -> list[dict]:
    """Roster id -> the human who owns it, for a team picker."""
    users = {u.get("user_id"): u for u in get_league_users(league_id)}
    out = []
    for r in get_rosters(league_id):
        owner = users.get(r.get("owner_id")) or {}
        settings = r.get("settings") or {}
        out.append({
            "roster_id": r.get("roster_id"),
            "owner_id": r.get("owner_id"),
            "display_name": owner.get("display_name") or owner.get("username") or "Unknown",
            "team_name": ((owner.get("metadata") or {}).get("team_name")
                          or owner.get("display_name") or "Team"),
            "wins": settings.get("wins", 0),
            "losses": settings.get("losses", 0),
            "ties": settings.get("ties", 0),
            "player_count": len(r.get("players") or []),
        })
    return sorted(out, key=lambda x: (x["roster_id"] or 0))


def _sleeper_to_gsis(sleeper_id: str) -> str | None:
    return (model_data.get("sleeper_map") or {}).get(str(sleeper_id))


def _gsis_to_sleeper(gsis_id: str) -> str | None:
    return (model_data.get("gsis_to_sleeper") or {}).get(gsis_id)


def rostered_sleeper_ids(league_id: str) -> set[str]:
    """Every player on any roster in the league -- i.e. NOT a free agent."""
    taken: set[str] = set()
    for r in get_rosters(league_id):
        for pid in (r.get("players") or []):
            taken.add(str(pid))
    return taken


async def analyze_roster(league_id: str, roster_id: int, week: int) -> dict:
    """Project every player on one roster and suggest a lineup.

    Returns starters/bench split by the model's projection rather than by
    Sleeper's saved lineup, so the user can see where their saved lineup and the
    model disagree.
    """
    from .prediction import get_player_card  # local import avoids a cycle

    rosters = get_rosters(league_id)
    roster = next((r for r in rosters if int(r.get("roster_id") or -1) == int(roster_id)), None)
    if roster is None:
        raise SleeperError(f"Roster {roster_id} not found in league {league_id}")

    league = get_league(league_id) or {}
    saved_starters = {str(p) for p in (roster.get("starters") or []) if p}

    cards, unmatched = [], []
    for sleeper_id in (roster.get("players") or []):
        gsis = _sleeper_to_gsis(sleeper_id)
        if not gsis:
            unmatched.append(str(sleeper_id))
            continue
        try:
            card = await get_player_card(gsis, week)
        except Exception as exc:
            logger.warning("Sleeper analysis: card failed for %s: %s", gsis, exc)
            continue
        if not card:
            continue
        card["sleeper_id"] = str(sleeper_id)
        card["in_saved_lineup"] = str(sleeper_id) in saved_starters
        cards.append(card)

    def _proj(c):
        try:
            return float(c.get("prediction") or 0)
        except (TypeError, ValueError):
            return 0.0

    ranked = sorted(cards, key=_proj, reverse=True)
    recommended = _recommend_lineup(ranked, league.get("roster_positions") or [])
    rec_ids = {c["sleeper_id"] for c in recommended}

    # Where the model disagrees with what the user actually has set.
    bench_but_should_start = [c for c in recommended if not c.get("in_saved_lineup")]
    start_but_should_sit = [
        c for c in cards if c.get("in_saved_lineup") and c["sleeper_id"] not in rec_ids
    ]

    return {
        "league": league,
        "roster_id": roster_id,
        "week": week,
        "players": ranked,
        "recommended_starters": recommended,
        "bench_but_should_start": bench_but_should_start,
        "start_but_should_sit": start_but_should_sit,
        "projected_total": round(sum(_proj(c) for c in recommended), 2),
        "position_counts": _position_counts(cards),
        "unmatched_sleeper_ids": unmatched,
    }


def _position_counts(cards: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for c in cards:
        pos = (c.get("position") or "").upper()
        if pos:
            counts[pos] = counts.get(pos, 0) + 1
    return counts


def _recommend_lineup(ranked: list[dict], roster_positions: list[str]) -> list[dict]:
    """Fill the league's actual lineup slots greedily by projection.

    Only QB/RB/WR/TE/FLEX are filled: the model does not project kickers or team
    defenses, so recommending one would be inventing a number.
    """
    slots = [s for s in roster_positions if s not in ("BN", "IR", "TAXI")]
    if not slots:
        slots = ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX"]

    used: set[str] = set()
    chosen: list[dict] = []

    def take(pred):
        for c in ranked:
            if c["sleeper_id"] in used:
                continue
            if pred(c):
                used.add(c["sleeper_id"])
                chosen.append(c)
                return True
        return False

    # Fixed positions first, then FLEX from whatever is left.
    for slot in [s for s in slots if s in PROJECTED_POSITIONS]:
        take(lambda c, s=slot: (c.get("position") or "").upper() == s)
    for slot in [s for s in slots if s in ("FLEX", "WRRB_FLEX", "REC_FLEX")]:
        allowed = FLEX_POSITIONS if slot == "FLEX" else (
            {"WR", "RB"} if slot == "WRRB_FLEX" else {"WR", "TE"})
        take(lambda c, a=allowed: (c.get("position") or "").upper() in a)
    for slot in [s for s in slots if s == "SUPER_FLEX"]:
        take(lambda c: (c.get("position") or "").upper() in PROJECTED_POSITIONS)

    return chosen


async def waiver_targets(league_id: str, week: int, limit: int = 25,
                         positions: set[str] | None = None) -> dict:
    """Rank free agents in the league by the app's own projection.

    Availability comes from the league (anyone on a roster is excluded); the
    ranking is ours, not Sleeper's popularity contest -- though trending add
    counts are attached so a genuine breakout still surfaces.
    """
    from .prediction import get_player_card

    taken = rostered_sleeper_ids(league_id)
    positions = positions or PROJECTED_POSITIONS

    trending: dict[str, int] = {}
    try:
        data = _get("/players/nfl/trending/add?lookback_hours=24&limit=200", ttl=900) or []
        for item in data:
            trending[str(item.get("player_id"))] = int(item.get("count") or 0)
    except SleeperError:
        pass  # ranking still works without popularity data

    profile = model_data.get("df_profile")
    if profile is None or profile.is_empty():
        return {"week": week, "players": [], "considered": 0}

    candidates = []
    for row in profile.iter_rows(named=True):
        pos = (row.get("position") or "").strip().upper()
        if pos not in positions:
            continue
        if (row.get("status") or "").upper() not in ("ACT", "", "NA"):
            continue
        gsis = row.get("player_id")
        sleeper_id = _gsis_to_sleeper(gsis)
        if not sleeper_id or str(sleeper_id) in taken:
            continue
        candidates.append((gsis, str(sleeper_id)))

    # Score the plausible ones: trending first, then the rest, capped so a
    # waiver lookup never turns into 1,500 model runs.
    candidates.sort(key=lambda c: trending.get(c[1], 0), reverse=True)
    considered = candidates[: max(limit * 6, 120)]

    cards = []
    for gsis, sleeper_id in considered:
        try:
            card = await get_player_card(gsis, week)
        except Exception:
            continue
        if not card:
            continue
        card["sleeper_id"] = sleeper_id
        card["trending_adds"] = trending.get(sleeper_id, 0)
        cards.append(card)

    def _proj(c):
        try:
            return float(c.get("prediction") or 0)
        except (TypeError, ValueError):
            return 0.0

    cards.sort(key=lambda c: (_proj(c), c.get("trending_adds", 0)), reverse=True)
    return {
        "week": week,
        "league_id": league_id,
        "considered": len(considered),
        "rostered": len(taken),
        "players": cards[:limit],
    }
