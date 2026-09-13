"""The in-app agent: a tool surface for pi, and a streaming chat endpoint.

Two halves that only meet inside the model:

* `/agent/tools` and `/agent/tools/{name}` are what the vendored pi extension
  talks to. They expose the same functions `backend/mcp_server` publishes to
  Claude Code, so both agents read identical numbers from identical formatters.
* `/agent/chat` streams one answer back to the browser as Server-Sent Events,
  translating pi's RPC event stream into the four things the UI cares about:
  text deltas, which tool is running, the finished answer, and errors.

The tool endpoints call back into this same API over loopback (that is how the
MCP tools are written). They are declared `def`, not `async def`, so FastAPI
runs them in its threadpool and a self-call cannot block the event loop.
"""

from __future__ import annotations

import asyncio
import json
import queue
import threading
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

# Absolute: uvicorn runs from backend/ (`applications.server:app`), so `agent`
# and `mcp_server` are top-level packages there, the same way mcp_server imports.
from agent import tools as agent_tools
from agent.pi_runtime import AgentBusy, AgentUnavailable, runtime

from ..config import logger

router = APIRouter(prefix="/agent", tags=["agent"])


# ---------------------------------------------------------------------------
# Tool surface (consumed by the pi extension, not by the browser)
# ---------------------------------------------------------------------------

class ToolCall(BaseModel):
    arguments: dict[str, Any] = Field(default_factory=dict)


@router.get("/tools")
def list_agent_tools():
    """Specs for every tool the in-app agent may call."""
    try:
        return agent_tools.list_tools()
    except agent_tools.ToolsUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/tools/{name}")
def call_agent_tool(name: str, call: ToolCall):
    """Run one tool and return its text.

    A failing tool returns 200 with `error` set rather than an HTTP error: the
    model needs to read what went wrong and try something else, and an exception
    crossing the extension boundary would abort the whole answer instead.
    """
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

class ChatRequest(BaseModel):
    conversation_id: str
    message: str
    screen: ScreenContext | None = None


def _sse(event: dict) -> str:
    return f"data: {json.dumps(event)}\n\n"


def _translate(event: dict, answer: list[str]) -> dict | None:
    """pi RPC event -> the browser's four event types, or None to ignore."""
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


@router.post("/chat")
async def agent_chat(request: ChatRequest):
    """Stream one answer as SSE.

    The runtime is a blocking iterator over a pipe, so it runs on a worker
    thread and hands events over a queue; the loop stays free to flush SSE
    frames as they arrive.
    """
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Ask something.")

    prompt = build_prompt(request.message.strip(), request.screen)
    bridge: queue.Queue = queue.Queue()
    _DONE = object()

    def pump() -> None:
        try:
            for event in runtime.prompt(request.conversation_id, prompt):
                bridge.put(event)
        except AgentBusy:
            bridge.put({"type": "_error", "message": "Still working on the last question."})
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
        try:
            while True:
                event = await asyncio.to_thread(bridge.get)
                if event is _DONE:
                    break
                out = _translate(event, answer)
                if out is None:
                    continue
                if out["type"] in ("done", "error"):
                    settled = True
                yield _sse(out)
            # A pipe that closes mid-answer still owes the UI a terminal event,
            # otherwise the dock spins forever.
            if not settled:
                text = "".join(answer).strip()
                yield _sse(
                    {"type": "done", "text": text}
                    if text
                    else {"type": "error", "message": "The agent returned nothing."}
                )
        finally:
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
    return {"closed": True}


@router.get("/status")
def agent_status():
    """Whether the agent can run, and how many tools it has."""
    status = runtime.status()
    try:
        status["tools"] = len(agent_tools.list_tools())
    except agent_tools.ToolsUnavailable as exc:
        status["tools"] = 0
        status["error"] = status.get("error") or str(exc)
    status["ready"] = bool(status["installed"] and status["tools"])
    return status
