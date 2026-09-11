"""Player storylines: recent NFL news, indexed per player.

ESPN's league news feed is the only usable source here. Its athlete-scoped
endpoints 404, team-scoped ones return nothing, and the league feed is hard
capped at 50 articles no matter what `limit` is passed. What it does give us is
good tagging: in sampling, 50 articles carried athlete tags covering 58
distinct players, spanning roughly the last 15 hours.

So rather than querying per player (impossible), we poll that feed on a timer
and ACCUMULATE into a local store keyed by (article_id, player_id). Each poll
adds whatever is new; over days this builds real per-player history, and we
serve the five most recent items for any given player.

Articles tag athletes by ESPN id, so we translate to the gsis ids the rest of
the app uses via nflreadpy's cross-platform id table.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import polars as pl

from ..config import logger, RAG_DIR
from ..state import model_data

ESPN_NEWS_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/news"
STORYLINES_CSV = os.path.join(RAG_DIR, "player_storylines.csv")

# Keep the store bounded; well past what any UI shows, but enough for history.
MAX_ROWS_PER_PLAYER = 40

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


def _espn_to_gsis() -> dict:
    """ESPN athlete id -> gsis id. Cached in model_data after first build."""
    cached = model_data.get("espn_to_gsis")
    if cached:
        return cached
    mapping = {}
    try:
        import nflreadpy as nfl

        ids = nfl.load_ff_playerids()
        if "espn_id" in ids.columns and "gsis_id" in ids.columns:
            sub = ids.select(["espn_id", "gsis_id"]).drop_nulls()
            mapping = {
                str(int(e)): g
                for e, g in zip(sub["espn_id"].to_list(), sub["gsis_id"].to_list())
                if e is not None and g
            }
    except Exception:
        logger.exception("Storylines: could not build espn->gsis map")
    model_data["espn_to_gsis"] = mapping
    return mapping


def _read_store() -> pl.DataFrame:
    if not os.path.exists(STORYLINES_CSV):
        return pl.DataFrame(schema=_SCHEMA)
    try:
        return pl.read_csv(STORYLINES_CSV, schema_overrides=_SCHEMA)
    except Exception:
        logger.exception("Storylines: store unreadable; starting fresh")
        return pl.DataFrame(schema=_SCHEMA)


def _fetch_feed(limit: int = 50) -> list:
    req = urllib.request.Request(
        f"{ESPN_NEWS_URL}?limit={limit}", headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        return (json.load(resp) or {}).get("articles", []) or []


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
    """Poll ESPN and merge anything new into the local store."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
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

    new_df = pl.DataFrame(fresh, schema=_SCHEMA)
    store = _read_store()
    before = store.height

    merged = (
        pl.concat([store, new_df], how="diagonal_relaxed")
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

    try:
        os.makedirs(os.path.dirname(STORYLINES_CSV), exist_ok=True)
        merged.write_csv(STORYLINES_CSV)
    except Exception:
        logger.exception("Storylines: could not write store")
        return {"status": "error", "error": "write failed"}

    model_data["df_storylines"] = merged
    model_data["storylines_updated_at"] = now
    added = merged.height - before
    logger.info(
        "Storylines: %d articles -> %d tagged rows (%d new), %d stored for %d players",
        len(articles), len(fresh), max(0, added), merged.height,
        merged.get_column("player_id").n_unique(),
    )
    return {
        "status": "ok",
        "articles": len(articles),
        "tagged_rows": len(fresh),
        "new_rows": max(0, added),
        "total_rows": merged.height,
        "players_covered": merged.get_column("player_id").n_unique(),
        "updated_at": now,
    }


def get_player_storylines(player_id: str, limit: int = 5) -> list:
    """Most recent storylines for one player, newest first."""
    df = model_data.get("df_storylines")
    if df is None or df.is_empty():
        df = _read_store()
        model_data["df_storylines"] = df
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
    df = model_data.get("df_storylines")
    if df is None or df.is_empty():
        df = _read_store()
        model_data["df_storylines"] = df
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