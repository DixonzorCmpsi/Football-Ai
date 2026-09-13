"""The in-app agent: a tool surface for pi, a streaming chat endpoint, and its limits.

* `/agent/tools` and `/agent/tools/{name}` are what the vendored pi extension
  talks to. They expose the same functions `backend/mcp_server` publishes to
  Claude Code, so both agents read identical numbers from identical formatters.
  They require the session token pi was started with; they are not a public API.
* `/agent/chat` streams one answer to the browser as Server-Sent Events,
  translating pi's RPC events into what the UI renders: text deltas, which tool is
  running, the finished answer, errors, and the user's remaining quota.
* Inference goes through `/llm/v1` (routes/llm_proxy.py). The browser chooses the
  free house tier or its own key; this module applies the house limits and hands
  a BYOK choice to the proxy for that conversation.

The tool endpoints call back into this same API over loopback (that is how the
MCP tools are written). They are declared `def`, not `async def`, so FastAPI
runs them in its threadpool and a self-call cannot block the event loop.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import queue
import re
import threading
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, SecretStr

# Absolute: uvicorn runs from backend/ (`applications.server:app`), so `agent`
# and `mcp_server` are top-level packages there, the same way mcp_server imports.
from agent import tools as agent_tools
from agent.pi_runtime import RUNTIME_DIR, AgentBusy, AgentForbidden, AgentUnavailable, runtime

from ..config import logger
from ..rate_limit import limiter as ip_limiter
from ..secret_store import get_secret, secret_source
from ..services import llm_proxy as proxy
from ..services.agent_tokens import mint, verify
from ..services.inference_providers import PROVIDERS, UnsafeUpstream, catalog
from ..services.usage_limits import house_daily_budget, limiter as usage

router = APIRouter(prefix="/agent", tags=["agent"])


# ---------------------------------------------------------------------------
# How pi is started: pointed at our proxy, holding only a session token
# ---------------------------------------------------------------------------

# pi's config directory for server-run agents. Kept apart from ~/.pi so a
# developer's own logins and settings never leak into the product agent.
PI_AGENT_DIR = RUNTIME_DIR / ".pi-agent"


def _proxy_base_url() -> str:
    return os.getenv("FOOTBALL_AI_API", "http://127.0.0.1:8000").rstrip("/") + "/llm/v1"


def _pi_session_env(conversation_id: str, client_id: str | None, owner: bool) -> tuple[dict, list]:
    PI_AGENT_DIR.mkdir(parents=True, exist_ok=True)
    models_path = PI_AGENT_DIR / "models.json"
    rendered = json.dumps(proxy.pi_models_json(_proxy_base_url()), indent=2)
    if not models_path.exists() or models_path.read_text(encoding="utf-8") != rendered:
        models_path.write_text(rendered, encoding="utf-8")

    env = {
        "PI_CODING_AGENT_DIR": str(PI_AGENT_DIR),
        # Both the proxy and the tool endpoints accept this; the extension sends it too.
        "FOOTBALL_AI_PROXY_TOKEN": mint(conversation_id, client_id or "anonymous", owner),
    }
    args: list[str] = []
    # FOOTBALL_AI_AGENT_MODEL/PROVIDER remain an escape hatch for running pi
    # directly against a provider during development.
    if not os.getenv("FOOTBALL_AI_AGENT_MODEL") and not os.getenv("FOOTBALL_AI_AGENT_PROVIDER"):
        args += ["--model", f"{proxy.PI_PROVIDER}/{proxy.PI_MODEL}"]
    return env, args


runtime.use_session_env(_pi_session_env)


# ---------------------------------------------------------------------------
# Tool surface (consumed by the pi extension, not by the browser)
# ---------------------------------------------------------------------------

class ToolCall(BaseModel):
    arguments: dict[str, Any] = Field(default_factory=dict)


def _require_session_token(request: Request) -> None:
    header = request.headers.get("authorization") or ""
    token = header[7:].strip() if header.lower().startswith("bearer ") else None
    if verify(token) is None:
        raise HTTPException(status_code=401, detail="Agent session token required")


# Exempt from the per-IP limiter for the same reason as the proxy: the calls come
# from pi over loopback, so every user would share one bucket. The token gates them.
@router.get("/tools")
@ip_limiter.exempt
def list_agent_tools(request: Request):
    """Specs for every tool the in-app agent may call."""
    _require_session_token(request)
    try:
        return agent_tools.list_tools()
    except agent_tools.ToolsUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/tools/{name}")
@ip_limiter.exempt
def call_agent_tool(name: str, call: ToolCall, request: Request):
    """Run one tool and return its text.

    A failing tool returns 200 with `error` set rather than an HTTP error: the
    model needs to read what went wrong and try something else, and an exception
    crossing the extension boundary would abort the whole answer instead.
    """
    _require_session_token(request)
    try:
        return {"text": agent_tools.call_tool(name, call.arguments)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"No agent tool named '{name}'") from None
    except agent_tools.ToolsUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.warning("agent tool %s failed: %s", name, exc)
        return {"error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Who is asking
# ---------------------------------------------------------------------------

_CLIENT_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")


def client_identity(request: Request) -> tuple[str, str | None, bool]:
    """(browser id, IP, is owner) for a request. Raises 400 without a usable browser id."""
    client_id = request.headers.get("x-client-id", "")
    if not _CLIENT_ID.match(client_id):
        raise HTTPException(status_code=400, detail="Missing or malformed X-Client-Id")

    ip = request.client.host if request.client else None
    # Cloud Run and most load balancers put the real client first in
    # X-Forwarded-For. Only trusted when explicitly enabled, since a client can
    # send the header itself when nothing sits in front of the app.
    if os.getenv("TRUST_PROXY_HEADERS", "").lower() in ("1", "true", "yes"):
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            ip = forwarded.split(",")[0].strip() or ip

    configured = get_secret("AGENT_OWNER_TOKEN")
    presented = request.headers.get("x-owner-token", "")
    owner = bool(configured and presented and hmac.compare_digest(configured, presented))
    return client_id, ip, owner


# ---------------------------------------------------------------------------
# Screen context
# ---------------------------------------------------------------------------

class ScreenEntity(BaseModel):
    type: str = ""
    name: str = ""
    id: str | None = None
    detail: str | None = None


class ScreenContext(BaseModel):
    """What the user is looking at, as reported by the frontend."""

    view: str = ""
    title: str = ""
    week: int | None = None
    facts: list[str] = Field(default_factory=list)
    entities: list[ScreenEntity] = Field(default_factory=list)


# Enough to resolve "him" and "this game" without burning context on a page dump.
MAX_ENTITIES = 12


def describe_screen(screen: ScreenContext | None) -> str:
    """One bracketed line naming the page and what is on it."""
    if not screen:
        return ""
    parts: list[str] = []
    if screen.title:
        parts.append(screen.title)
    elif screen.view:
        parts.append(screen.view.replace("_", " ").title())
    if screen.week:
        parts.append(f"week {screen.week}")
    parts.extend(f for f in screen.facts if f)

    entities = [e for e in screen.entities if e.name][:MAX_ENTITIES]
    if entities:
        rendered = ", ".join(
            f"{e.name} ({e.detail})" if e.detail else e.name for e in entities
        )
        parts.append(f"showing {rendered}")

    return f"[On screen: {'; '.join(parts)}]" if parts else ""


def build_prompt(message: str, screen: ScreenContext | None) -> str:
    preamble = describe_screen(screen)
    return f"{preamble}\n\n{message}".strip() if preamble else message


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------

class ByokSettings(BaseModel):
    """A user's own model provider. SecretStr keeps the key out of reprs and logs."""

    provider: str
    api_key: SecretStr
    model: str = Field(min_length=1, max_length=200)
    base_url: str | None = Field(default=None, max_length=500)


class ChatRequest(BaseModel):
    conversation_id: str = Field(min_length=8, max_length=64)
    message: str = Field(max_length=4000)
    screen: ScreenContext | None = None
    byok: ByokSettings | None = None


def _sse(event: dict) -> str:
    return f"data: {json.dumps(event)}\n\n"


def _translate(event: dict, answer: list[str]) -> dict | None:
    """pi RPC event -> the browser's event types, or None to ignore."""
    kind = event.get("type")

    if kind == "message_update":
        delta = event.get("assistantMessageEvent") or {}
        if delta.get("type") == "text_delta" and delta.get("delta"):
            answer.append(delta["delta"])
            return {"type": "delta", "text": delta["delta"]}
        # Thinking deltas are not surfaced: the dock shows one small box, and
        # reasoning scrolling past would read as the answer.
        return None

    if kind == "tool_execution_start":
        return {"type": "tool", "name": event.get("toolName"), "state": "start"}

    if kind == "tool_execution_end":
        return {"type": "tool", "name": event.get("toolName"), "state": "end"}

    if kind == "agent_settled":
        return {"type": "done", "text": "".join(answer).strip()}

    if kind == "_error":
        return {"type": "error", "message": event.get("message")}

    return None


def _model_error(event: dict) -> str | None:
    """A failed model call pi reports inside a message rather than as an RPC error.

    Proxy refusals (quota, bad BYOK key, capacity) land here: pi ends the
    assistant message with stopReason "error" and the proxy's message text.
    """
    if event.get("type") not in ("message_end", "turn_end"):
        return None
    message = event.get("message") or {}
    if message.get("role") == "assistant" and message.get("stopReason") == "error":
        return clean_model_error(message.get("errorMessage") or "")
    return None


def clean_model_error(raw: str) -> str:
    """The human sentence inside pi's error text.

    pi reports a failed call as the provider's body, JSON-encoded, sometimes after a
    status code: `429 {"error":{"message":"Quota exceeded: ..."}}`. The browser
    should show the sentence, not the envelope.
    """
    text = (raw or "").strip()
    start = text.find("{")
    if start != -1:
        try:
            parsed = json.loads(text[start:])
            inner = None
            if isinstance(parsed, dict):
                # Either the full body {"error": {"message": ...}} or, as pi formats
                # some statuses (seen for a 401: `401: {"message": ..., "type": ...}`),
                # the error object on its own.
                error = parsed.get("error")
                inner = error.get("message") if isinstance(error, dict) else parsed.get("message")
            # Some SDKs stringify the error body twice; unwrap one more level.
            if isinstance(inner, str) and inner.lstrip().startswith("{"):
                return clean_model_error(inner)
            if inner:
                text = str(inner)
        except (ValueError, AttributeError):
            pass
    if text.startswith("Quota exceeded:"):
        text = text[len("Quota exceeded:"):].strip()
        text = text[:1].upper() + text[1:]
    return text or "The model call failed."


@router.post("/chat")
async def agent_chat(request: Request, body: ChatRequest):
    """Stream one answer as SSE.

    The runtime is a blocking iterator over a pipe, so it runs on a worker
    thread and hands events over a queue; the loop stays free to flush SSE
    frames as they arrive.
    """
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="Ask something.")
    client_id, ip, owner = client_identity(request)

    tier = "house"
    quota = None
    if body.byok is not None:
        try:
            upstream = proxy.resolve_byok(
                body.byok.provider, body.byok.api_key.get_secret_value(), body.byok.model, body.byok.base_url
            )
        except (ValueError, UnsafeUpstream) as exc:
            return JSONResponse({"detail": str(exc), "code": "byok_invalid"}, status_code=400)
        proxy.set_byok(body.conversation_id, client_id, upstream)
        tier = "byok"
    else:
        # Switching back to the free tier must stop using a key entered earlier.
        proxy.clear_byok(body.conversation_id)
        if not proxy.house_configured():
            return JSONResponse(
                {
                    "detail": "The free assistant isn't set up on this server yet. Add your own API key in the assistant settings.",
                    "code": "house_unconfigured",
                },
                status_code=503,
            )
        quota = await run_in_threadpool(usage().admit_question, client_id, ip, owner)
        if not quota.allowed:
            message = (
                "You've used today's free questions. They reset at midnight ET, or add your own API key in settings."
                if quota.blocked_by == "client"
                else "This network has used today's free questions. They reset at midnight ET, or add your own API key in settings."
            )
            return JSONResponse({"detail": message, "code": "quota_exhausted", "quota": quota.as_dict()}, status_code=429)

    prompt = build_prompt(body.message.strip(), body.screen)
    proxy.begin_question(body.conversation_id)
    bridge: queue.Queue = queue.Queue()
    _DONE = object()

    def pump() -> None:
        try:
            for event in runtime.prompt(body.conversation_id, prompt, client_id, owner):
                bridge.put(event)
        except AgentBusy:
            bridge.put({"type": "_error", "message": "Still working on the last question."})
        except AgentForbidden:
            bridge.put({"type": "_error", "message": "That conversation belongs to another session. Clear it and start again."})
        except AgentUnavailable as exc:
            bridge.put({"type": "_error", "message": str(exc)})
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("agent chat failed")
            bridge.put({"type": "_error", "message": f"{type(exc).__name__}: {exc}"})
        finally:
            bridge.put(_DONE)

    worker = threading.Thread(target=pump, name="agent-chat", daemon=True)
    worker.start()

    async def stream():
        answer: list[str] = []
        settled = False
        failed = False
        model_error: str | None = None
        try:
            if quota is not None:
                yield _sse({"type": "quota", "quota": quota.as_dict()})
            yield _sse({"type": "tier", "tier": tier})
            while True:
                event = await asyncio.to_thread(bridge.get)
                if event is _DONE:
                    break
                model_error = _model_error(event) or model_error
                out = _translate(event, answer)
                if out is None:
                    continue
                if out["type"] == "done" and not out["text"] and model_error:
                    out = {"type": "error", "message": model_error}
                if out["type"] in ("done", "error"):
                    settled = True
                    failed = out["type"] == "error"
                yield _sse(out)
            # A pipe that closes mid-answer still owes the UI a terminal event,
            # otherwise the dock spins forever.
            if not settled:
                text = "".join(answer).strip()
                failed = not text
                yield _sse(
                    {"type": "done", "text": text}
                    if text
                    else {"type": "error", "message": model_error or "The agent returned nothing."}
                )
        finally:
            # A question that produced no answer shouldn't cost the user one.
            if quota is not None and failed and not answer and not owner:
                await run_in_threadpool(usage().refund_question, client_id, ip)
            yield _sse({"type": "end"})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class ConversationRef(BaseModel):
    conversation_id: str


@router.post("/abort")
def agent_abort(ref: ConversationRef):
    return {"aborted": runtime.abort(ref.conversation_id)}


@router.post("/reset")
def agent_reset(ref: ConversationRef):
    """End a conversation and its process. Sent when the user clears the chat."""
    runtime.close(ref.conversation_id)
    proxy.clear_byok(ref.conversation_id)
    return {"closed": True}


# ---------------------------------------------------------------------------
# Limits and providers, for the dock
# ---------------------------------------------------------------------------

@router.get("/quota")
def agent_quota(request: Request):
    """This browser's free-tier allowance for today."""
    client_id, _ip, owner = client_identity(request)
    return {
        "house_configured": proxy.house_configured(),
        "quota": usage().quota(client_id, owner).as_dict(),
    }


@router.get("/providers")
def agent_providers():
    """Providers the settings UI can offer for bring-your-own-key."""
    return {"providers": catalog()}


class ModelListRequest(BaseModel):
    provider: str
    api_key: SecretStr | None = None
    base_url: str | None = Field(default=None, max_length=500)


@router.post("/byok/models")
@ip_limiter.limit("20/minute")
async def byok_models(request: Request, body: ModelListRequest):
    """Model ids a provider offers right now. Makes an outbound call, so it is rate limited."""
    try:
        models = await proxy.list_models(
            body.provider, body.api_key.get_secret_value() if body.api_key else None, body.base_url
        )
    except (ValueError, UnsafeUpstream) as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except httpx.HTTPError as exc:
        return JSONResponse({"detail": f"Could not list models: {type(exc).__name__}"}, status_code=502)
    return {"models": models}


_PING_TOOL = {
    "type": "function",
    "function": {
        "name": "ping",
        "description": "Connectivity check. Call it with no arguments.",
        "parameters": {"type": "object", "properties": {}},
    },
}


@router.post("/byok/test")
@ip_limiter.limit("10/minute")
async def byok_test(request: Request, body: ByokSettings):
    """One tiny completion with a tool attached, so a key, a model and tool support are checked together.

    A model that rejects `tools` fails here, in settings, instead of on the user's
    first real question.
    """
    try:
        upstream = proxy.resolve_byok(body.provider, body.api_key.get_secret_value(), body.model, body.base_url)
    except (ValueError, UnsafeUpstream) as exc:
        return JSONResponse({"ok": False, "detail": str(exc)}, status_code=400)

    started = asyncio.get_running_loop().time()
    response = await proxy.forward(
        {
            "messages": [{"role": "user", "content": "Reply with the single word OK."}],
            "tools": [_PING_TOOL],
            "max_tokens": 16,
            "stream": False,
        },
        upstream,
    )
    elapsed_ms = int((asyncio.get_running_loop().time() - started) * 1000)
    payload = json.loads(response.body)
    if response.status_code >= 400:
        return JSONResponse(
            {"ok": False, "detail": (payload.get("error") or {}).get("message"), "status": response.status_code},
            status_code=200,
        )
    return {"ok": True, "latency_ms": elapsed_ms, "model": payload.get("model") or body.model}


@router.get("/status")
def agent_status():
    """Whether the agent can run, and how it is configured. Never includes secret values."""
    status = runtime.status()
    try:
        status["tools"] = len(agent_tools.list_tools())
    except agent_tools.ToolsUnavailable as exc:
        status["tools"] = 0
        status["error"] = status.get("error") or str(exc)
    status["ready"] = bool(status["installed"] and status["tools"])

    try:
        provider = proxy.house_provider()
        key_name = provider.key_env or "INFERENCE_HOUSE_API_KEY"
        status["house"] = {
            "configured": proxy.house_configured(),
            "provider": provider.id,
            "models": proxy.house_models(),
            "key_source": secret_source("INFERENCE_HOUSE_API_KEY")
            if secret_source("INFERENCE_HOUSE_API_KEY") != "missing"
            else secret_source(key_name),
            "daily_request_budget": house_daily_budget(),
            "budget_remaining_today": usage().house_budget_remaining(),
        }
    except proxy.HouseUnavailable as exc:
        status["house"] = {"configured": False, "error": str(exc)}
    status["byok_providers"] = [p for p in PROVIDERS]
    return status
