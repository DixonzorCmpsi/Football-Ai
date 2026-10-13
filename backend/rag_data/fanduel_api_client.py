"""Game lines and player props from FanDuel Sportsbook's public JSON.

Why this exists
---------------
Bovada answers an empty board (`{}`) to requests from cloud-datacenter addresses, so the
scraper worked on a laptop and returned nothing on the server. FanDuel's sportsbook serves
the same kind of board to those addresses, so it is the primary source and Bovada the
fallback (see 10b_bovada_api_props.py).

The endpoints are the ones FanDuel's own web app calls (with the public application key
that app ships), not a documented API. They can change without notice, which is why the
parsing is kept free of network calls and tested against fixtures, why the pipeline keeps a
second source, and why the audit (audit_data.py) checks the numbers against an independent
feed.

Output rows use exactly the shape bovada_api_client produces, so nothing downstream changes.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

from bovada_api_client import (
    BOVADA_NAME_MAP,
    GAME_LINE_COLUMNS,
    _signed,
    american_to_implied_prob,
    is_team_entry,
)

BASE = os.getenv("FANDUEL_API_BASE", "https://sbapi.nj.sportsbook.fanduel.com/api")
# The public key embedded in FanDuel's web app; override it if they rotate it.
APP_KEY = os.getenv("FANDUEL_APP_KEY", "FhMFpcPWXMeyZxOx")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0", "Accept": "application/json"}

# The tabs that hold player markets (a game's page loads one tab at a time).
PROP_TABS = ("popular", "passing-props", "receiving-props", "rushing-props", "td-scorer-props")

# Player market type -> our prop_type. Quarter and drive markets are different wagers and are skipped.
_MAIN = {
    "RECEIVING_YARDS": "Receiving Yards",
    "RECEPTIONS": "Receptions",
    "RUSHING_YARDS": "Rushing Yards",
    "PASSING_YARDS": "Passing Yards",
    "PASSING_TOUCHDOWNS": "Passing Touchdowns",
}
_MAIN_RE = re.compile(r"^PLAYER_X_(?P<alt>ALT_)?(?P<kind>RECEIVING_YARDS|RECEPTIONS|RUSHING_YARDS|PASSING_YARDS|PASSING_TOUCHDOWNS)_(LOW|MEDIUM|HIGH)$")
_SCORER = {
    "ANY_TIME_TOUCHDOWN_SCORER": "Anytime TD",
    "FIRST_TOUCHDOWN_SCORER": "First TD",
    "TO_SCORE_2+_TOUCHDOWNS": "2+ TDs",
    "TO_SCORE_3+_TOUCHDOWNS": "3+ TDs",
}
_ALT_RUNNER_RE = re.compile(r"^(?P<name>.+?)\s+(?P<n>\d+(?:\.\d+)?)\+")


def _get(url: str, timeout: int = 40, retries: int = 3, backoff: float = 1.5):
    last = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # network flakiness is expected here
            last = exc
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"FanDuel request failed after {retries} attempts: {url.split('?')[0]} ({last})")


def _american(runner: dict) -> str | None:
    odds = ((runner.get("winRunnerOdds") or {}).get("americanDisplayOdds") or {}).get("americanOdds")
    if odds in (None, ""):
        return None
    try:
        return f"{int(odds):+d}"
    except (TypeError, ValueError):
        return None


def split_event_name(name: str) -> tuple[str | None, str | None]:
    """'Seattle Seahawks @ Denver Broncos' -> (home 'DEN', away 'SEA')."""
    if " @ " not in (name or ""):
        return None, None
    away, _, home = name.partition(" @ ")
    return BOVADA_NAME_MAP.get(home.strip()), BOVADA_NAME_MAP.get(away.strip())


def parse_events(page: dict, now: datetime | None = None) -> list[dict]:
    """Upcoming NFL games from the league page: [{id, name, kickoff(ms)}], soonest first."""
    now = now or datetime.now(timezone.utc)
    games = []
    for eid, ev in ((page.get("attachments") or {}).get("events") or {}).items():
        name = ev.get("name") or ""
        home, away = split_event_name(name)
        if not home or not away:
            continue  # futures, the draft, specials
        try:
            kickoff = datetime.fromisoformat(str(ev.get("openDate")).replace("Z", "+00:00"))
        except ValueError:
            continue
        if kickoff <= now:
            continue  # started: in-play prices are a different wager and go stale in seconds
        games.append({"id": str(ev.get("eventId") or eid), "name": name, "kickoff": kickoff})
    return sorted(games, key=lambda g: g["kickoff"])


def parse_game_lines(markets: dict, event_name: str, season: int, week: int | None,
                     game_id: str | None, processed_at: str | None = None) -> dict | None:
    """Full-game spread, moneyline and total, or None when the game has none."""
    row: dict = {c: None for c in GAME_LINE_COLUMNS}
    home, away = split_event_name(event_name)
    row.update({
        "game_id": game_id, "week": week, "season": season, "home_team": home, "away_team": away,
        "processed_at": processed_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    found = False
    for m in markets.values():
        if m.get("marketStatus") != "OPEN" or m.get("inPlay"):
            continue
        kind = m.get("marketType")
        for r in m.get("runners") or []:
            if r.get("runnerStatus") != "ACTIVE":
                continue
            odds = _american(r)
            side = ((r.get("result") or {}).get("type") or "").upper()
            prob = american_to_implied_prob(odds)
            if odds is None:
                continue
            if kind == "MONEY_LINE" and side in ("HOME", "AWAY"):
                prefix = "home" if side == "HOME" else "away"
                row[f"{prefix}_ml"], row[f"{prefix}_ml_prob"] = odds, prob
                found = True
            elif kind == "MATCH_HANDICAP_(2-WAY)" and side in ("HOME", "AWAY") and _signed(r.get("handicap")):
                prefix = "home" if side == "HOME" else "away"
                row[f"{prefix}_spread"] = _signed(r.get("handicap"))
                row[f"{prefix}_spread_odds"], row[f"{prefix}_spread_prob"] = odds, prob
                found = True
            elif kind == "TOTAL_POINTS_(OVER/UNDER)" and side in ("OVER", "UNDER") and r.get("handicap") not in (None, ""):
                prefix = "total_over" if side == "OVER" else "total_under"
                row[prefix] = float(r["handicap"])
                row[f"{prefix}_odds"], row[f"{prefix}_prob"] = odds, prob
                found = True
    return row if found else None


def parse_player_props(markets: dict, season: int, week: int | None,
                       game_id: str | None, scraped_at: str | None = None) -> list[dict]:
    """Flatten a game's markets into player-prop rows (same columns as the Bovada parser)."""
    scraped_at = scraped_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: list[dict] = []

    def add(name, prop_type, line, odds, side):
        if not name or is_team_entry(name) or odds is None:
            return
        rows.append({
            "player_name": name.strip(), "bovada_team": None, "prop_type": prop_type, "line": float(line),
            "odds": odds, "side": side, "implied_prob": american_to_implied_prob(odds),
            "week": week, "game_id": game_id, "season": season, "scraped_at": scraped_at,
        })

    for m in markets.values():
        if m.get("marketStatus") != "OPEN" or m.get("inPlay"):
            continue
        mtype = m.get("marketType") or ""
        runners = [r for r in (m.get("runners") or []) if r.get("runnerStatus") == "ACTIVE"]

        if mtype in _SCORER:
            for r in runners:
                if r.get("isPlayerSelection", True):
                    add(r.get("runnerName"), _SCORER[mtype], 1.0, _american(r), "Yes")
            continue

        match = _MAIN_RE.match(mtype)
        if mtype == "PLAYER_X_RUSHING_+_RECEIVING_YARDS":
            label, alt = "Rushing & Receiving Yards", False
        elif match:
            label, alt = _MAIN[match.group("kind")], bool(match.group("alt"))
        else:
            continue
        if alt:
            # An alternate ladder: "RJ Harvey 20+ Yards" is an over on 19.5.
            for r in runners:
                hit = _ALT_RUNNER_RE.match(r.get("runnerName") or "")
                if hit:
                    add(hit.group("name"), f"Alternate {label}", float(hit.group("n")) - 0.5, _american(r), "over")
            continue
        for r in runners:
            side = ((r.get("result") or {}).get("type") or "").lower()
            handicap = r.get("handicap")
            if side not in ("over", "under") or handicap in (None, ""):
                continue
            name = re.sub(r"\s+(Over|Under)$", "", r.get("runnerName") or "")
            add(name, label, float(handicap), _american(r), side)
    return rows


# --------------------------------------------------------------------------
# Network
# --------------------------------------------------------------------------

def fetch_nfl_page() -> dict:
    return _get(f"{BASE}/content-managed-page?page=CUSTOM&customPageId=nfl&_ak={APP_KEY}")


def fetch_nfl_events() -> list[dict]:
    return parse_events(fetch_nfl_page())


def fetch_event_markets(event_id: str, pause: float = 0.3) -> dict:
    """Every market FanDuel lists for a game, across the tabs that hold player props."""
    markets: dict = {}
    for tab in PROP_TABS:
        try:
            data = _get(f"{BASE}/event-page?_ak={APP_KEY}&eventId={event_id}&tab={tab}", retries=2)
        except RuntimeError:
            continue  # a missing tab costs that tab's markets, not the whole game
        markets.update((data.get("attachments") or {}).get("markets") or {})
        time.sleep(pause)
    return markets
