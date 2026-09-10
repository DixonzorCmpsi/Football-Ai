"""Real average-draft-position lookups via FantasyFootballCalculator's free,
no-key-required public API — used to order the tier-list pool the way a
real draft board would, instead of our own model's projection.
"""
import time

import requests

from ..config import logger
from .utils import normalize_name

_CACHE: dict[int, tuple[float, dict]] = {}
_CACHE_TTL_SECONDS = 6 * 3600


def _fetch_adp_rows(season: int) -> list[dict]:
    url = "https://fantasyfootballcalculator.com/api/v1/adp/standard"
    try:
        resp = requests.get(url, params={"teams": 12, "year": season}, timeout=8)
        resp.raise_for_status()
        return resp.json().get("players", [])
    except Exception as e:
        logger.warning("ADP fetch failed for %s: %s", season, e)
        return []


def get_adp_map(season: int) -> dict[str, float]:
    """normalized_player_name -> ADP (lower = drafted earlier / more valuable)."""
    now = time.time()
    cached = _CACHE.get(season)
    if cached and (now - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]

    rows = _fetch_adp_rows(season)
    if not rows:
        # Fall back to the prior season's board rather than showing no ADP at
        # all — still real draft data, just one year stale (typical for very
        # early in a new season before the current-year board fills in).
        rows = _fetch_adp_rows(season - 1)

    adp_map = {normalize_name(r["name"]): r["adp"] for r in rows if r.get("name") and r.get("adp") is not None}
    _CACHE[season] = (now, adp_map)
    return adp_map


def lookup_adp(player_name: str, adp_map: dict[str, float], candidate_names: list[str]) -> float | None:
    """Exact normalized-name match only.

    `candidate_names` is unused here — kept in the signature so callers (and
    the precomputed candidate list) don't need to change if fuzzy matching is
    reintroduced later with a sturdier heuristic.

    A same-surname similarity-ratio fuzzy match was tried and removed: common
    real nickname pairs ("Mike"/"Michael" 0.55, "Will"/"William" 0.73) score
    LOWER than the false positive that motivated tightening it ("Brian" vs
    "Bijan" Robinson, 0.80) — there's no threshold that keeps the former and
    rejects the latter. A wrong ADP is worse than none, so unmatched names
    just fall back to the projection-based sort instead of guessing.
    """
    return adp_map.get(normalize_name(player_name))
