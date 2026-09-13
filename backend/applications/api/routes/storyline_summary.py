"""Open a storyline in the app: its summary and what it means for the player."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..rate_limit import limiter as ip_limiter
from ..services import llm_proxy as proxy
from ..services import storyline_summary as summaries
from ..services.inference_providers import UnsafeUpstream
from ..services.usage_limits import limiter as usage
from ..state import model_data
from .agent import ByokSettings, client_identity

router = APIRouter(tags=["storylines"])


class SummaryRequest(BaseModel):
    byok: ByokSettings | None = None


def _player_name(player_id: str) -> str:
    profile = model_data.get("df_profile")
    if profile is None or profile.is_empty():
        return ""
    import polars as pl
    hit = profile.filter(pl.col("player_id") == player_id)
    return hit.row(0, named=True).get("player_name") or "" if not hit.is_empty() else ""


def _find_item(player_id: str, article_id: str) -> dict | None:
    from ..services.storylines import get_player_storylines
    for item in get_player_storylines(player_id, limit=60, backfill=False):
        if str(item.get("article_id")) == str(article_id):
            return item
    return None


@router.post("/player/{player_id}/storylines/{article_id}/summary")
@ip_limiter.limit("30/minute")
async def storyline_summary(request: Request, player_id: str, article_id: str, body: SummaryRequest | None = None):
    item = await run_in_threadpool(_find_item, player_id, article_id)
    if item is None:
        return JSONResponse({"detail": "That storyline isn't on file for this player."}, status_code=404)

    name = _player_name(player_id)
    text = await run_in_threadpool(summaries.article_text, item)
    result = {
        "article_id": str(article_id),
        "headline": item.get("headline"),
        "published": item.get("published"),
        "story_type": item.get("story_type"),
        "url": item.get("url"),
        "image": item.get("image"),
        "excerpt": summaries.excerpt(text),
    }

    byok = body.byok if body else None
    if byok is None:
        cached = summaries.cached_summary(article_id)
        if cached:
            return {**result, "summary": cached}

    try:
        client_id, ip, owner = client_identity(request)
    except Exception:
        client_id, ip, owner = None, None, False

    upstream = None
    quota = None
    if byok is not None:
        try:
            upstream = proxy.resolve_byok(byok.provider, byok.api_key.get_secret_value(), byok.model, byok.base_url)
        except (ValueError, UnsafeUpstream) as exc:
            return {**result, "summary": summaries.fallback_summary(name, text, f"Your key settings: {exc}")}
    elif not proxy.house_configured():
        return {**result, "summary": summaries.fallback_summary(
            name, text, "AI summaries need the free assistant or your own key. Showing the article's own lines.")}
    elif client_id is None:
        return {**result, "summary": summaries.fallback_summary(name, text, None)}
    else:
        quota = await run_in_threadpool(usage().admit_question, client_id, ip, owner)
        if not quota.allowed:
            return {**result, "quota": quota.as_dict(), "summary": summaries.fallback_summary(
                name, text, "Today's free AI questions are used up. Showing the article's own lines.")}
        if not await run_in_threadpool(usage().take_house_call):
            await run_in_threadpool(usage().refund_question, client_id, ip)
            return {**result, "summary": summaries.fallback_summary(
                name, text, "The free assistant has used today's capacity. Showing the article's own lines.")}
        upstream = proxy.house_upstream()

    try:
        summary = await summaries.model_summary(name, item, text, upstream)
    except Exception as exc:
        if quota is not None and not owner:
            await run_in_threadpool(usage().refund_question, client_id, ip)
        return {**result, "summary": summaries.fallback_summary(name, text, f"AI summary failed: {exc}")}
    out = {**result, "summary": summary}
    if quota is not None:
        out["quota"] = quota.as_dict()
    return out
