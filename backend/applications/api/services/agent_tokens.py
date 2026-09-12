"""Short-lived credentials that let one pi process call the inference proxy.

pi never holds the OpenRouter key. When the runtime starts a conversation it mints
a token naming that conversation and the browser that owns it, and pi sends it as
its "API key" to `/llm/v1`. The proxy verifies it, swaps in the real key upstream,
and knows whose usage to count.

The token is HMAC-signed rather than stored, so any backend instance can verify
it without shared state. That matters on Cloud Run, where requests land on
whichever instance is up. Set `AGENT_PROXY_SIGNING_KEY` (same value everywhere)
for multi-instance deploys. Without it, a random per-process key is used, which is
fine for one local server.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass

from ..config import logger
from ..secret_store import get_secret

# Outlives the runtime's idle TTL (30 min) with room to spare: an expired token
# mid-conversation would surface as a baffling auth error from the proxy.
TOKEN_TTL_SECONDS = int(os.getenv("AGENT_PROXY_TOKEN_TTL", str(12 * 3600)))
TOKEN_PREFIX = "spt_"

_ephemeral_key: bytes | None = None


def _signing_key() -> bytes:
    global _ephemeral_key
    configured = get_secret("AGENT_PROXY_SIGNING_KEY")
    if configured:
        return configured.encode("utf-8")
    if _ephemeral_key is None:
        _ephemeral_key = secrets.token_bytes(32)
        logger.warning(
            "AGENT_PROXY_SIGNING_KEY is not set; using a random per-process key. Fine for one "
            "local server; set it (identically on every instance) before scaling out."
        )
    return _ephemeral_key


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


@dataclass(frozen=True)
class ProxyGrant:
    conversation_id: str
    client_id: str
    owner: bool
    expires_at: int


def mint(conversation_id: str, client_id: str, owner: bool = False, ttl: int | None = None) -> str:
    payload = {
        "c": conversation_id,
        "u": client_id,
        "o": bool(owner),
        "exp": int(time.time()) + int(ttl if ttl is not None else TOKEN_TTL_SECONDS),
    }
    body = _b64(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = _b64(hmac.new(_signing_key(), body.encode("ascii"), hashlib.sha256).digest())
    return f"{TOKEN_PREFIX}{body}.{signature}"


def verify(token: str | None) -> ProxyGrant | None:
    """The grant a token carries, or None if it is malformed, forged or expired."""
    if not token or not token.startswith(TOKEN_PREFIX):
        return None
    try:
        body, signature = token[len(TOKEN_PREFIX):].split(".", 1)
        expected = _b64(hmac.new(_signing_key(), body.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            return None
        payload = json.loads(_unb64(body))
        if int(payload["exp"]) < time.time():
            return None
        return ProxyGrant(
            conversation_id=str(payload["c"]),
            client_id=str(payload["u"]),
            owner=bool(payload.get("o")),
            expires_at=int(payload["exp"]),
        )
    except Exception:
        return None
