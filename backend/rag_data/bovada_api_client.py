"""Structured Bovada odds ingestion.

Why this exists
---------------
The original pipeline (10_bovada_crawler.py -> 11_bovada_scraper.py) drives a
headless Chrome session and dumps the *rendered page text* into Menu.json. Bovada
renders only the default market tab, so that dump captures the "POPULAR" view --
mostly Anytime Touchdown plus a handful of yardage lines for well-known players.
Every other market (PASSING PROPS, RUSHING PROPS, RECEIVING PROPS, ALTERNATE
LINES, ...) lives behind a tab the scraper never clicks, so those lines were
never in the data at all.

Bovada serves the same board as JSON. Asking for `marketFilterId=all` returns
every market for an event in one request:

    .../events/A/description/football/nfl/<event-slug>?marketFilterId=all&lang=en

Measured on CHI@CAR, week 1 2026:
    default coupon ("def")   ->   3 markets
    rendered-text scrape     ->  ~1 tab of markets, 774 player rows for ALL games
    marketFilterId=all       -> 184 markets, 566 player prop rows for ONE game

The over/under sides also come back properly here; the text scrape produced only
`Yes` and `over` rows, which left betting_insights' UNDER logic with no data.

Parsing is kept free of network calls so it can be tested against fixtures.
"""

from __future__ import annotations

import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

BASE = "https://www.bovada.lv/services/sports/event/coupon/events/A/description"
NFL_COUPON = BASE + "/football/nfl?marketFilterId=def&preMatchOnly=true&lang=en"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bovada.lv/sports/football/nfl",
}

BOVADA_NAME_MAP = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LA", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS",
}

# Bovada's in-line team codes differ from nflverse in a few places.
TEAM_CODE_FIXUP = {
    "LAR": "LA", "JAC": "JAX", "WSH": "WAS", "KAN": "KC", "TAM": "TB",
    "NOR": "NO", "SFO": "SF", "GNB": "GB", "NWE": "NE", "LVR": "LV",
}

# Market title prefix -> canonical prop_type. Downstream consumers
# (prediction.py, betting_insights.py, parlay_recommender.py) match prop_type by
# lowercase substring, so these names are load-bearing: "Passing TDs" and
# "Passing Touchdowns" arriving as separate labels for one market is what split
# that market across two buckets in the old data.
OVER_UNDER_MARKETS = {
    "Total Passing Yards": "Passing Yards",
    "Total Passing Touchdowns": "Passing Touchdowns",
    "Total Passing Attempts": "Passing Attempts",
    "Total Completions": "Completions",
    "Total Interceptions Thrown": "Interceptions Thrown",
    "Longest Pass Completion": "Longest Pass Completion",
    "Total Passing & Rushing Yards O/U": "Passing & Rushing Yards",
    "Total Rushing Yards": "Rushing Yards",
    "Total Rush Attempts": "Rush Attempts",
    "Total Longest Rushing Attempt O/U": "Longest Rush",
    "Total Receiving Yards": "Receiving Yards",
    "Total Receptions": "Receptions",
    "Longest Reception": "Longest Reception",
    "Total Touchdowns": "Total Touchdowns",
    "Total Kicking Points": "Kicking Points",
    "Alternate Passing Yards": "Alternate Passing Yards",
    "Alternate Passing Touchdowns": "Alternate Passing Touchdowns",
    "Alternate Passing Completions": "Alternate Completions",
    "Alternate Rushing Yards": "Alternate Rushing Yards",
    "Alternate Receiving Yards": "Alternate Receiving Yards",
    "Alternate Receptions": "Alternate Receptions",
}

# Markets whose OUTCOMES are the players themselves (no handicap).
PLAYER_LIST_MARKETS = {
    "Anytime Touchdown Scorer": "Anytime TD",
    "First Touchdown Scorer": "First TD",
    "Player to Score 2 or More Touchdowns": "2+ TDs",
    "Player to Score 3 or More Touchdowns": "3+ TDs",
}

_YARD_RECEPTION_RE = re.compile(r"^Player to Record a (\d+)\+ Yard Reception$")
_PLAYER_SUFFIX_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<team>[A-Z]{2,4})\)\s*$")


def american_to_implied_prob(odds) -> float | None:
    """Convert American odds to an implied probability percentage."""
    if odds is None:
        return None
    text = str(odds).strip().upper()
    if text in ("EVEN", "EV"):
        return 50.0
    try:
        value = int(text.replace("+", ""))
    except ValueError:
        return None
    if value == 0:
        return None
    if value > 0:
        prob = 100.0 / (value + 100.0)
    else:
        prob = abs(value) / (abs(value) + 100.0)
    return round(prob * 100.0, 2)


def normalize_team_code(code: str | None) -> str | None:
    if not code:
        return None
    code = code.strip().upper()
    return TEAM_CODE_FIXUP.get(code, code)


def split_player_and_team(text: str) -> tuple[str | None, str | None]:
    """'Caleb Williams (CHI)' -> ('Caleb Williams', 'CHI'); tolerates ' - 1H'."""
    if not text:
        return None, None
    cleaned = re.sub(r"\s*-\s*(1H|2H|1Q|2Q|3Q|4Q)\s*$", "", text.strip())
    m = _PLAYER_SUFFIX_RE.match(cleaned)
    if not m:
        return cleaned or None, None
    return m.group("name").strip(), normalize_team_code(m.group("team"))


def classify_market(description: str) -> tuple[str | None, str | None, str | None]:
    """Return (kind, prop_type, player_text) for a market description.

    kind is 'ou' (market titled "<Market> - <Player>"), 'list' (outcomes are
    players), or None when the market is not a player prop.
    """
    if not description:
        return None, None, None
    desc = description.strip()

    if desc in PLAYER_LIST_MARKETS:
        return "list", PLAYER_LIST_MARKETS[desc], None
    m = _YARD_RECEPTION_RE.match(desc)
    if m:
        return "list", f"{m.group(1)}+ Yard Reception", None

    if " - " in desc:
        prefix, _, player = desc.partition(" - ")
        prop = OVER_UNDER_MARKETS.get(prefix.strip())
        if prop:
            return "ou", prop, player.strip()
    return None, None, None


def parse_event_player_props(event: dict, season: int, week: int | None,
                             game_id: str | None, scraped_at: str | None = None) -> list[dict]:
    """Flatten one Bovada event into player-prop rows.

    Only full-game markets are kept: 1H/1Q variants describe a different wager
    and would otherwise collide with the game line for the same player.
    """
    scraped_at = scraped_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: list[dict] = []

    for group in event.get("displayGroups") or []:
        for market in group.get("markets") or []:
            period = ((market.get("period") or {}).get("description") or "").strip()
            if period and period not in ("Game", "Regulation Time"):
                continue
            kind, prop_type, player_text = classify_market(market.get("description", ""))
            if not kind:
                continue

            if kind == "ou":
                name, team = split_player_and_team(player_text or "")
                if not name:
                    continue
                for outcome in market.get("outcomes") or []:
                    price = outcome.get("price") or {}
                    side = (outcome.get("description") or "").strip().lower()
                    if side not in ("over", "under"):
                        continue
                    handicap = price.get("handicap")
                    if handicap in (None, ""):
                        continue
                    try:
                        line = float(handicap)
                    except (TypeError, ValueError):
                        continue
                    odds = price.get("american")
                    rows.append({
                        "player_name": name,
                        "bovada_team": team,
                        "prop_type": prop_type,
                        "line": line,
                        "odds": odds,
                        "side": side,
                        "implied_prob": american_to_implied_prob(odds),
                        "week": week,
                        "game_id": game_id,
                        "season": season,
                        "scraped_at": scraped_at,
                    })
            else:
                for outcome in market.get("outcomes") or []:
                    price = outcome.get("price") or {}
                    name, team = split_player_and_team(outcome.get("description") or "")
                    if not name:
                        continue
                    odds = price.get("american")
                    rows.append({
                        "player_name": name,
                        "bovada_team": team,
                        "prop_type": prop_type,
                        "line": 1.0,
                        "odds": odds,
                        "side": "Yes",
                        "implied_prob": american_to_implied_prob(odds),
                        "week": week,
                        "game_id": game_id,
                        "season": season,
                        "scraped_at": scraped_at,
                    })
    return rows


# --------------------------------------------------------------------------
# Network
# --------------------------------------------------------------------------

def _get(url: str, timeout: int = 40, retries: int = 3, backoff: float = 1.5):
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # network flakiness is expected here
            last = exc
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Bovada request failed after {retries} attempts: {url} ({last})")


def fetch_nfl_events() -> list[dict]:
    data = _get(NFL_COUPON)
    if not data:
        return []
    return data[0].get("events") or []


def fetch_event_full(link: str) -> dict | None:
    data = _get(f"{BASE}{link}?marketFilterId=all&lang=en")
    if not data:
        return None
    events = data[0].get("events") or []
    return events[0] if events else None


def event_teams(event: dict) -> tuple[str | None, str | None]:
    """(home_abbr, away_abbr) using the event's own competitor list."""
    home = away = None
    for c in event.get("competitors") or []:
        abbr = BOVADA_NAME_MAP.get((c.get("name") or "").strip())
        if abbr is None:
            continue
        if c.get("home"):
            home = abbr
        else:
            away = abbr
    return home, away
