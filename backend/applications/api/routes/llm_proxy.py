"""OpenAI-compatible endpoint pi sends its model calls to. See services/llm_proxy.py."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool

from ..rate_limit import limiter as ip_limiter
from ..services import llm_proxy as proxy
from ..services.agent_tokens import verify
from ..services.inference_providers import UnsafeUpstream
from ..services.usage_limits import limiter as usage

router = APIRouter(prefix="/llm/v1", tags=["inference"])

# pi auto-retries a failed model call when the error text contains "429", "503",
# "rate limit" and similar, unless it also contains a terminal phrase such as
# "quota exceeded" (NON_RETRYABLE_PROVIDER_LIMIT_ERROR_PATTERN in pi's bundle).
# Deliberate refusals therefore carry that phrase, or use a status pi won't retry.
# Otherwise the user watches ~15s of backoff before seeing a "no" that was final
# from the start.
QUOTA_PREFIX = "Quota exceeded:"


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization") or ""
    return header[7:].strip() if header.lower().startswith("bearer ") else None


# Exempt from the per-IP limiter: every call arrives from pi over loopback, so all
# users would share one 127.0.0.1 bucket. The signed token is the gate here, and
# the house budget and per-question cap are the limits.
@router.post("/chat/completions")
@ip_limiter.exempt
async def chat_completions(request: Request):
    grant = verify(_bearer(request))
    if grant is None:
        return proxy._error(401, "Invalid or expired assistant session. Start a new conversation.")

    try:
        body = await request.json()
    except Exception:
        return proxy._error(400, "Request body must be JSON")

    try:
        upstream = proxy.upstream_for(grant.conversation_id, grant.client_id)
    except proxy.HouseUnavailable:
        return proxy._error(403, "The free assistant isn't set up yet. Add your own API key in the assistant settings.")
    except UnsafeUpstream as exc:
        return proxy._error(400, f"Custom provider URL refused: {exc}")

    if not proxy.note_call(grant.conversation_id):
        return proxy._error(
            429,
            f"{QUOTA_PREFIX} that question needed more than {proxy.MAX_CALLS_PER_QUESTION} steps, so it was stopped. Try asking something narrower.",
        )

    if upstream.tier == "house" and not await run_in_threadpool(usage().take_house_call):
        return proxy._error(
            429,
            f"{QUOTA_PREFIX} the free assistant has used today's capacity. It resets at midnight ET, or add your own API key in settings.",
        )

    return await proxy.forward(body, upstream)
