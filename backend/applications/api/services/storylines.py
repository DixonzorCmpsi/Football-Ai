"""Player storylines: NFL news, indexed per player, with something to show for everyone.

Two ESPN sources feed one local store keyed by (article_id, player_id):

1. **League feed** (`ESPN_NEWS_URL`), polled on a timer. Capped at 50 articles,
   about the last 15 hours, tagged with athlete ids. Good for breaking news,
   but on its own it reached only 94 of 2,962 players (3.2%), so most profiles
   opened on "No storylines yet".
2. **Per-player feed** (`PLAYER_NEWS_URL`, ESPN's fantasy news), fetched when a
   profile is opened and at most once per `BACKFILL_TTL_SECONDS` per player. It
   goes back months: 16 items to January for Brock Purdy, 10 items to November for
   a reserve defensive back. This is what lets every covered player show their
   five most recent storylines, however old those are.

An earlier version of this docstring said per-player queries were impossible. The
endpoints it tried (`/athletes/{id}/news` variants) do 404, and
`now.core.api.espn.com?athlete=` silently ignores the filter. The fantasy endpoint
does work, verified 2026-09-12.

ESPN ids come from nflreadpy's cross-platform id table, which covers 99% of skill
players but only 77% of depth-chart starters (it skips many linemen and rookies).
The gaps are resolved through ESPN's player search, accepted only on an exact
name, NFL, and team match, and only when exactly one result matches, so a shared
name can't attach another player's news.
"""
import json
import os
import re
import threading
import time
import unicodedata
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import polars as pl

from ..config import logger, RAG_DIR, TEAM_ABBR_MAP
from ..state import model_data

ESPN_NEWS_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/news"
PLAYER_NEWS_URL = "https://site.web.api.espn.com/apis/fantasy/v2/games/ffl/news/players"
ESPN_SEARCH_URL = "https://site.web.api.espn.com/apis/common/v3/search"
STORYLINES_CSV = os.path.join(RAG_DIR, "player_storylines.csv")

# Keep the store bounded; well past what any UI shows, but enough for history.
MAX_ROWS_PER_PLAYER = 40
# How often one player's own feed is re-checked. Opening a profile repeatedly must
# not mean a request to ESPN every time.
BACKFILL_TTL_SECONDS = float(os.getenv("STORYLINES_BACKFILL_TTL", str(6 * 3600)))
HTTP_TIMEOUT = 12

_SCHEMA = {
    "article_id": pl.Utf8,
    "player_id": pl.Utf8,
    "espn_id": pl.Utf8,
    "headline": pl.Utf8,
    "description": pl.Utf8,
    "published": pl.Utf8,
    "url": pl.Utf8,
    "image": pl.Utf8,
    "story_type": pl.Utf8,
    "fetched_at": pl.Utf8,
}

# The league poll and profile backfills both rewrite the CSV. One lock keeps a
# backfill from overwriting rows a concurrent poll just added, or the reverse.
_store_lock = threading.Lock()
# player_id -> monotonic time of the last per-player fetch attempt, successful or not.
_backfilled_at: dict[str, float] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.load(resp) or {}


def _load_id_table() -> None:
    """Both directions of ESPN <-> gsis, built once from nflreadpy."""
    if "espn_to_gsis" in model_data and "gsis_to_espn" in model_data:
        return
    espn_to_gsis: dict = {}
    try:
        import nflreadpy as nfl

        ids = nfl.load_ff_playerids()
        if "espn_id" in ids.columns and "gsis_id" in ids.columns:
            sub = ids.select(["espn_id", "gsis_id"]).drop_nulls()
            espn_to_gsis = {
                str(int(e)): g
                for e, g in zip(sub["espn_id"].to_list(), sub["gsis_id"].to_list())
                if e is not None and g
            }
    except Exception:
        logger.exception("Storylines: could not build espn<->gsis map")
    model_data["espn_to_gsis"] = espn_to_gsis
    model_data["gsis_to_espn"] = {g: e for e, g in espn_to_gsis.items()}


def _espn_to_gsis() -> dict:
    """ESPN athlete id -> gsis id. Cached in model_data after first build."""
    _load_id_table()
    return model_data.get("espn_to_gsis") or {}


def _normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    text = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", text.lower())
    return re.sub(r"[^a-z]", "", text)


_ABBR_BY_TEAM_NAME = {name.lower(): abbr for name, abbr in TEAM_ABBR_MAP.items()}


def _search_espn_id(name: str, team_abbr: str | None) -> str | None:
    """ESPN id by player search, only when exactly one NFL player matches name AND team."""
    if not name:
        return None
    query = urllib.parse.urlencode({"query": name, "limit": 10, "type": "player"})
    try:
        items = _get_json(f"{ESPN_SEARCH_URL}?{query}").get("items") or []
    except Exception as exc:
        logger.info("Storylines: ESPN search failed for %s: %s", name, type(exc).__name__)
        return None

    wanted = _normalize_name(name)
    matches = []
    for item in items:
        if (item.get("league") or "").lower() != "nfl":
            continue
        if _normalize_name(item.get("displayName") or "") != wanted:
            continue
        teams = [
            _ABBR_BY_TEAM_NAME.get((rel.get("displayName") or "").lower())
            for rel in item.get("teamRelationships") or []
        ]
        # No team on file (free agent) is only acceptable when the profile has none either.
        if team_abbr and team_abbr not in teams:
            continue
        if item.get("id"):
            matches.append(str(item["id"]))
    return matches[0] if len(set(matches)) == 1 else None


def resolve_espn_id(player_id: str) -> str | None:
    _load_id_table()
    known = (model_data.get("gsis_to_espn") or {}).get(player_id)
    if known:
        return known

    cache: dict = model_data.setdefault("espn_id_search_cache", {})
    if player_id in cache:
        return cache[player_id]

    profile = model_data.get("df_profile")
    name, team = None, None
    if profile is not None and not profile.is_empty() and "player_id" in profile.columns:
        row = profile.filter(pl.col("player_id") == player_id).head(1).to_dicts()
        if row:
            name, team = row[0].get("player_name"), row[0].get("team_abbr")
    found = _search_espn_id(name, team) if name else None
    # Negative results are cached too: a player ESPN doesn't know shouldn't cost a
    # search on every profile open.
    cache[player_id] = found
    if found:
        model_data.setdefault("espn_to_gsis", {})[found] = player_id
    return found


def _read_store() -> pl.DataFrame:
    if not os.path.exists(STORYLINES_CSV):
        return pl.DataFrame(schema=_SCHEMA)
    try:
        return pl.read_csv(STORYLINES_CSV, schema_overrides=_SCHEMA)
    except Exception:
        logger.exception("Storylines: store unreadable; starting fresh")
        return pl.DataFrame(schema=_SCHEMA)


def _current_store() -> pl.DataFrame:
    df = model_data.get("df_storylines")
    if df is None or df.is_empty():
        df = _read_store()
        model_data["df_storylines"] = df
    return df


def _merge_into_store(rows: list, stamp_updated: bool) -> tuple[pl.DataFrame, int]:
    """Merge rows into the store and persist. Returns (store, rows added)."""
    with _store_lock:
        store = _read_store()
        before = store.height
        if not rows:
            return store, 0
        merged = (
            pl.concat([store, pl.DataFrame(rows, schema=_SCHEMA)], how="diagonal_relaxed")
            # Newest fetch wins for a given (article, player) pair.
            .unique(subset=["article_id", "player_id"], keep="last")
            .sort("published", descending=True)
        )
        # Bound per-player history so the file cannot grow without limit.
        merged = (
            merged.with_columns(pl.col("player_id").cum_count().over("player_id").alias("_n"))
            .filter(pl.col("_n") <= MAX_ROWS_PER_PLAYER)
            .drop("_n")
        )
        os.makedirs(os.path.dirname(STORYLINES_CSV), exist_ok=True)
        merged.write_csv(STORYLINES_CSV)
        model_data["df_storylines"] = merged
        if stamp_updated:
            model_data["storylines_updated_at"] = _now()
        return merged, max(0, merged.height - before)


# --- league feed ----------------------------------------------------------------

def _fetch_feed(limit: int = 50) -> list:
    return _get_json(f"{ESPN_NEWS_URL}?limit={limit}").get("articles", []) or []


def _article_rows(article: dict, espn_map: dict, now: str) -> list:
    """One row per tagged athlete we can resolve to a gsis id."""
    links = article.get("links") or {}
    web = (links.get("web") or {}).get("href") or ""
    images = article.get("images") or []
    image = images[0].get("url", "") if images else ""
    article_id = str(article.get("id") or web or article.get("headline", ""))[:120]

    rows = []
    for cat in article.get("categories") or []:
        if cat.get("type") != "athlete":
            continue
        athlete = cat.get("athlete") or {}
        espn_id = athlete.get("id")
        if espn_id is None:
            continue
        gsis = espn_map.get(str(int(espn_id)))
        if not gsis:
            continue
        rows.append({
            "article_id": article_id,
            "player_id": gsis,
            "espn_id": str(int(espn_id)),
            "headline": (article.get("headline") or "").strip(),
            "description": (article.get("description") or "").strip(),
            "published": article.get("published") or now,
            "url": web,
            "image": image,
            "story_type": article.get("type") or "",
            "fetched_at": now,
        })
    return rows


def refresh_storylines() -> dict:
    """Poll ESPN's league feed and merge anything new into the local store."""
    now = _now()
    try:
        articles = _fetch_feed()
    except Exception as exc:
        logger.warning("Storylines: feed fetch failed: %s", exc)
        return {"status": "error", "error": str(exc)}

    espn_map = _espn_to_gsis()
    if not espn_map:
        return {"status": "error", "error": "no espn->gsis mapping available"}

    fresh = []
    for art in articles:
        fresh.extend(_article_rows(art, espn_map, now))

    if not fresh:
        logger.info("Storylines: %d articles, none tagged to known players", len(articles))
        return {"status": "ok", "articles": len(articles), "new_rows": 0, "total_rows": _read_store().height}

    try:
        merged, added = _merge_into_store(fresh, stamp_updated=True)
    except Exception:
        logger.exception("Storylines: could not write store")
        return {"status": "error", "error": "write failed"}

    logger.info(
        "Storylines: %d articles -> %d tagged rows (%d new), %d stored for %d players",
        len(articles), len(fresh), added, merged.height, merged.get_column("player_id").n_unique(),
    )
    return {
        "status": "ok",
        "articles": len(articles),
        "tagged_rows": len(fresh),
        "new_rows": added,
        "total_rows": merged.height,
        "players_covered": merged.get_column("player_id").n_unique(),
        "updated_at": model_data.get("storylines_updated_at"),
    }


# --- per-player feed ------------------------------------------------------------

def _player_feed_rows(player_id: str, espn_id: str, items: list, now: str) -> list:
    rows = []
    for item in items:
        # Paywalled ESPN+ stories would be a dead end for most readers.
        if item.get("premium"):
            continue
        headline = (item.get("headline") or "").strip()
        if not headline:
            continue
        description = (item.get("description") or "").strip()
        # Rotowire blurbs repeat the headline as the description; showing it twice reads as a bug.
        if description == headline:
            description = ""
        links = item.get("links") or {}
        url = (links.get("web") or {}).get("href") or (links.get("mobile") or {}).get("href") or ""
        images = item.get("images") or []
        rows.append({
            "article_id": str(item.get("id") or url or headline)[:120],
            "player_id": player_id,
            "espn_id": espn_id,
            "headline": headline,
            "description": description,
            "published": item.get("published") or item.get("lastModified") or now,
            "url": url,
            "image": images[0].get("url", "") if images else "",
            "story_type": item.get("type") or "",
            "fetched_at": now,
        })
    return rows


def backfill_player(player_id: str, force: bool = False) -> int:
    """Pull one player's own news feed into the store. Returns rows added.

    Rate-limited per player by BACKFILL_TTL_SECONDS. Failures are logged and count
    as an attempt, so an ESPN outage doesn't turn every profile open into a
    stalled request.
    """
    now_mono = time.monotonic()
    last = _backfilled_at.get(player_id)
    if not force and last is not None and now_mono - last < BACKFILL_TTL_SECONDS:
        return 0
    _backfilled_at[player_id] = now_mono

    espn_id = resolve_espn_id(player_id)
    if not espn_id:
        return 0
    try:
        items = _get_json(f"{PLAYER_NEWS_URL}?playerId={espn_id}&limit=25").get("feed") or []
    except Exception as exc:
        logger.info("Storylines: player feed failed for %s: %s", player_id, type(exc).__name__)
        return 0
    rows = _player_feed_rows(player_id, espn_id, items, _now())
    try:
        _, added = _merge_into_store(rows, stamp_updated=False)
    except Exception:
        logger.exception("Storylines: could not write backfill for %s", player_id)
        return 0
    return added


def get_player_storylines(player_id: str, limit: int = 5, backfill: bool = True) -> list:
    """The player's most recent storylines, newest first, however far back that goes.

    Tops up from the player's own feed first (at most once per TTL), so a player
    the league feed never mentioned still gets their last `limit` items rather
    than an empty list.
    """
    if backfill:
        backfill_player(player_id)
    df = _current_store()
    if df.is_empty():
        return []
    rows = (
        df.filter(pl.col("player_id") == player_id)
        .sort("published", descending=True)
        .head(max(1, limit))
        .to_dicts()
    )
    for r in rows:
        r.pop("espn_id", None)
    return rows


def storylines_updated_at() -> str | None:
    """When the store was last written.

    Falls back to the newest fetched_at in the store, so a freshly started
    process reports real freshness instead of null just because it has not run
    a refresh itself yet.
    """
    stamped = model_data.get("storylines_updated_at")
    if stamped:
        return stamped
    df = _current_store()
    if df.is_empty() or "fetched_at" not in df.columns:
        return None
    try:
        return df.get_column("fetched_at").max()
    except Exception:
        return None


def storylines_wrapper():
    """Sync entry point for APScheduler."""
    try:
        refresh_storylines()
    except Exception:
        logger.exception("Storylines refresh raised")
