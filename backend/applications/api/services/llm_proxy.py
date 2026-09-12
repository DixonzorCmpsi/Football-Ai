"""The inference proxy: the only code that ever holds a model provider's API key.

pi is configured (see `pi_session_env`) to send OpenAI Chat Completions to
`/llm/v1` on this backend, with a signed per-conversation token as its "API key".
For each call the proxy decides the real upstream:

* **house** (default): our key from the secret store, the operator's free model
  list, drawn from the shared daily budget.
* **byok**: the provider, key and model the user entered, held in memory for their
  conversation only. Never written to disk, the database, or logs.

Moving the proxy to its own Cloud Run service later means moving this module and
`routes/llm_proxy.py`; pi only knows a base URL.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field

import httpx
from fastapi.responses import JSONResponse, StreamingResponse

from ..config import logger
from ..secret_store import get_secret
from .inference_providers import PROVIDERS, Provider, UnsafeUpstream, check_custom_base_url

# The model id pi is told about. The proxy maps it to real models, so switching
# the house model or a user switching providers never touches pi's config.
PI_PROVIDER = "spot"
PI_MODEL = "assistant"

MAX_CALLS_PER_QUESTION = int(os.getenv("AGENT_MAX_LLM_CALLS_PER_QUESTION", "8"))
# A BYOK setting nobody has used for this long is forgotten, taking the key with it.
BYOK_TTL_SECONDS = float(os.getenv("AGENT_BYOK_TTL", "3600"))

DEFAULT_HOUSE_MODELS = "google/gemma-4-31b-it:free,nvidia/nemotron-3-super-120b-a12b:free"


class HouseUnavailable(RuntimeError):
    """The free tier has no key configured."""


@dataclass
class Upstream:
    tier: str                    # "house" | "byok"
    provider: Provider
    base_url: str
    api_key: str = field(repr=False)
    models: list[str]

    @property
    def label(self) -> str:
        return self.provider.label


# --- house tier ---------------------------------------------------------------

def house_provider() -> Provider:
    provider_id = os.getenv("INFERENCE_HOUSE_PROVIDER", "openrouter")
    if provider_id not in PROVIDERS or PROVIDERS[provider_id].base_url is None:
        raise HouseUnavailable(f"INFERENCE_HOUSE_PROVIDER={provider_id!r} is not a known hosted provider")
    return PROVIDERS[provider_id]


def house_models() -> list[str]:
    raw = os.getenv("INFERENCE_HOUSE_MODELS", DEFAULT_HOUSE_MODELS)
    return [m.strip() for m in raw.split(",") if m.strip()]


def house_api_key() -> str | None:
    return get_secret("INFERENCE_HOUSE_API_KEY") or get_secret(house_provider().key_env or "")


def house_upstream() -> Upstream:
    provider = house_provider()
    key = house_api_key()
    if not key:
        raise HouseUnavailable(f"No API key for the free assistant (set {provider.key_env})")
    # Operator-controlled, so not subject to the BYOK URL checks. Lets a test
    # point the house tier at a local stub.
    base_url = (os.getenv("INFERENCE_HOUSE_BASE_URL") or provider.base_url).rstrip("/")
    return Upstream("house", provider, base_url, key, house_models())


def house_configured() -> bool:
    try:
        house_upstream()
        return True
    except HouseUnavailable:
        return False


# --- per-conversation state -----------------------------------------------------

@dataclass
class _Byok:
    client_id: str
    upstream: Upstream
    touched: float


_byok: dict[str, _Byok] = {}
_calls: dict[str, int] = {}
_lock = threading.Lock()


def resolve_byok(provider_id: str, api_key: str, model: str, base_url: str | None) -> Upstream:
    """Validate a user's settings into an Upstream. Raises ValueError with a user-facing reason."""
    provider = PROVIDERS.get(provider_id)
    if provider is None:
        raise ValueError(f"Unknown provider {provider_id!r}")
    if not api_key.strip():
        raise ValueError("Enter an API key")
    if not model.strip():
        raise ValueError("Choose a model")
    if provider.base_url is None:
        url = check_custom_base_url(base_url or "")
    else:
        url = provider.base_url
    return Upstream("byok", provider, url, api_key.strip(), [model.strip()])


def set_byok(conversation_id: str, client_id: str, upstream: Upstream) -> None:
    with _lock:
        _byok[conversation_id] = _Byok(client_id, upstream, time.time())


def clear_byok(conversation_id: str) -> None:
    with _lock:
        _byok.pop(conversation_id, None)


def _sweep() -> None:
    cutoff = time.time() - BYOK_TTL_SECONDS
    for cid in [c for c, b in _byok.items() if b.touched < cutoff]:
        del _byok[cid]


def upstream_for(conversation_id: str, client_id: str) -> Upstream:
    with _lock:
        _sweep()
        entry = _byok.get(conversation_id)
        if entry and entry.client_id == client_id:
            entry.touched = time.time()
            upstream = entry.upstream
        else:
            upstream = None
    if upstream is None:
        return house_upstream()
    if upstream.provider.base_url is None:
        # Re-checked at use: a hostname can re-resolve to a private address after it was saved.
        check_custom_base_url(upstream.base_url)
    return upstream


def begin_question(conversation_id: str) -> None:
    with _lock:
        _calls[conversation_id] = 0


def note_call(conversation_id: str) -> bool:
    """Count one model call against the current question. False once over the cap.

    Guards against a model stuck calling tools in a loop, which would burn the
    house budget or a user's own credits while they watch a spinner.
    """
    with _lock:
        _calls[conversation_id] = _calls.get(conversation_id, 0) + 1
        return _calls[conversation_id] <= MAX_CALLS_PER_QUESTION


# --- forwarding ---------------------------------------------------------------

# Swapped for httpx.MockTransport in tests.
_transport: httpx.AsyncBaseTransport | None = None


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=_transport,
        timeout=httpx.Timeout(connect=10.0, read=180.0, write=30.0, pool=10.0),
        # Never follow: a public host must not be able to bounce us to a private one.
        follow_redirects=False,
    )


def _error(status: int, message: str, headers: dict | None = None) -> JSONResponse:
    # OpenAI's error shape: pi surfaces error.message to the user verbatim.
    return JSONResponse({"error": {"message": message, "type": "proxy_error"}}, status_code=status, headers=headers)


def prepare_payload(body: dict, upstream: Upstream) -> dict:
    payload = dict(body)
    payload["model"] = upstream.models[0]
    if upstream.provider.supports_model_fallback and len(upstream.models) > 1:
        payload["models"] = list(upstream.models)
    else:
        payload.pop("models", None)
    field_name = upstream.provider.max_tokens_field
    for other in ("max_tokens", "max_completion_tokens"):
        if other != field_name and other in payload:
            payload[field_name] = payload.pop(other)
    return payload


def _headers(upstream: Upstream) -> dict:
    headers = {"Authorization": f"Bearer {upstream.api_key}", "Content-Type": "application/json"}
    headers.update(upstream.provider.extra_headers)
    app_url = os.getenv("PUBLIC_APP_URL")
    if app_url and upstream.provider.id == "openrouter":
        headers["HTTP-Referer"] = app_url
    return headers


def _explain(status: int, upstream: Upstream, raw: bytes) -> str:
    detail = ""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list) and parsed:
            parsed = parsed[0]
        detail = (parsed.get("error") or {}).get("message") or ""
    except Exception:
        detail = raw[:200].decode("utf-8", "replace")

    if upstream.tier == "byok":
        if status in (401, 403):
            return f"{upstream.label} rejected your API key ({status}). Check it in the assistant settings."
        if status == 404:
            return f"{upstream.label} doesn't know the model {upstream.models[0]!r}: {detail}".rstrip(": ")
        if status == 429:
            return f"{upstream.label} rate-limited your key. {detail}".strip()
        return f"{upstream.label} returned {status}: {detail}".rstrip(": ")

    if status == 429:
        return "The free assistant is busy right now. Try again in a minute."
    logger.warning("house upstream %s returned %s: %s", upstream.label, status, detail[:300])
    return "The free assistant hit a problem with its model provider. Try again shortly."


async def forward(body: dict, upstream: Upstream):
    payload = prepare_payload(body, upstream)
    url = f"{upstream.base_url}/chat/completions"
    client = _client()
    try:
        request = client.build_request("POST", url, json=payload, headers=_headers(upstream))
        response = await client.send(request, stream=bool(payload.get("stream")))
    except httpx.HTTPError as exc:
        await client.aclose()
        return _error(502, f"Could not reach {upstream.label}: {type(exc).__name__}")

    if response.status_code >= 400:
        raw = await response.aread()
        await response.aclose()
        await client.aclose()
        retry = {"Retry-After": response.headers["retry-after"]} if "retry-after" in response.headers else None
        return _error(response.status_code, _explain(response.status_code, upstream, raw), retry)

    if not payload.get("stream"):
        raw = await response.aread()
        await response.aclose()
        await client.aclose()
        return JSONResponse(json.loads(raw), status_code=response.status_code)

    async def relay():
        try:
            # Decoded bytes, not raw: providers may gzip, and the encoding header is not
            # relayed, so raw bytes would reach pi still compressed.
            async for chunk in response.aiter_bytes():
                yield chunk
        finally:
            await response.aclose()
            await client.aclose()

    return StreamingResponse(
        relay(),
        status_code=response.status_code,
        media_type=response.headers.get("content-type", "text/event-stream"),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def list_models(provider_id: str, api_key: str | None, base_url: str | None) -> list[str]:
    """Model ids a provider offers right now, for the settings UI.

    OpenRouter's list is narrowed to models that accept tools, since the agent
    cannot work without tool calling. Other providers don't report that, so their
    lists are returned whole.
    """
    provider = PROVIDERS.get(provider_id)
    if provider is None:
        raise ValueError(f"Unknown provider {provider_id!r}")
    url = check_custom_base_url(base_url or "") if provider.base_url is None else provider.base_url
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    async with _client() as client:
        response = await client.get(f"{url}/models", headers=headers)
    if response.status_code in (401, 403):
        raise ValueError(f"{provider.label} rejected the API key")
    response.raise_for_status()
    data = response.json().get("data") or []
    if provider.id == "openrouter":
        data = [m for m in data if "tools" in (m.get("supported_parameters") or [])]
    return sorted(str(m.get("id")) for m in data if m.get("id"))


def pi_models_json(proxy_base_url: str) -> dict:
    """The models.json pi reads: one provider pointing at this proxy, one model alias.

    compat is set to the lowest common denominator on purpose. The alias can land
    on any upstream, and plain `system` messages without `reasoning_effort` or
    `store` are accepted by every provider in the catalog.
    """
    return {
        "providers": {
            PI_PROVIDER: {
                "baseUrl": proxy_base_url.rstrip("/"),
                "api": "openai-completions",
                "apiKey": "$FOOTBALL_AI_PROXY_TOKEN",
                "compat": {
                    "supportsDeveloperRole": False,
                    "supportsReasoningEffort": False,
                    "supportsStore": False,
                    "maxTokensField": "max_tokens",
                },
                "models": [
                    {
                        "id": PI_MODEL,
                        "name": "THE SPOT assistant",
                        "contextWindow": int(os.getenv("AGENT_CONTEXT_WINDOW", "128000")),
                        "maxTokens": int(os.getenv("AGENT_MAX_OUTPUT_TOKENS", "2048")),
                    }
                ],
            }
        }
    }
