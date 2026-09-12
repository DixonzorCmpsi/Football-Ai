"""Supervises `pi --mode rpc` subprocesses for the in-app agent.

One pi process per conversation, spoken to over stdin/stdout in JSONL. RPC mode
was chosen over pi's Node SDK because the backend is Python; it also keeps the
agent in its own process, so a wedged run never takes the API down with it.

Three things here are deliberate rather than incidental:

* **The agent gets no shell.** `--no-builtin-tools` drops read/write/edit/bash
  and the extension supplies football tools only. This agent answers questions
  for whoever has the page open; it has no business touching the filesystem.
* **Threads, not asyncio subprocesses.** Windows pipe handling has already cost
  this codebase a day (see run_mcp_server.py). A blocking reader thread feeding
  a queue behaves the same on every platform.
* **Nothing is hardcoded.** The pi entry point is resolved from the vendored
  package's own `bin` field, so a version bump moves it without edits here.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator

logger = logging.getLogger(__name__)

RUNTIME_DIR = Path(__file__).resolve().parent / "runtime"
EXTENSION = RUNTIME_DIR / "football-tools.ts"
PI_PACKAGE = "@earendil-works/pi-coding-agent"

# A conversation nobody has touched for this long is reaped along with its
# process. Node holds tens of megabytes per session; leaking them is not an option.
SESSION_TTL_SECONDS = float(os.getenv("FOOTBALL_AI_AGENT_TTL", "1800"))
MAX_SESSIONS = int(os.getenv("FOOTBALL_AI_AGENT_MAX_SESSIONS", "8"))
# How long to wait for the next event before deciding the run is wedged.
EVENT_TIMEOUT_SECONDS = float(os.getenv("FOOTBALL_AI_AGENT_EVENT_TIMEOUT", "120"))

SYSTEM_PROMPT = """You are the analyst inside THE SPOT AI, an NFL projection app.

The person you are talking to is looking at a page in that app right now. Each
message is prefixed with what is on their screen; treat it as the subject of
"this", "him", "that game" and similar references unless they clearly mean
something else.

Answer from the tools. Every projection, line, injury and roster fact comes from
this app's own models and feeds, so call a tool rather than recalling anything
about the NFL from memory -- your training data is a season behind and players
have changed teams. If a tool returns nothing useful, say so plainly instead of
filling the gap.

Be brief. Two or three sentences of substance beats a structured report; the
first answer appears in a small box on their screen. Lead with the number or the
call they asked for, then the one reason that matters. No preamble, no restating
the question, no bullet lists unless comparing three or more things.
"""


class AgentUnavailable(RuntimeError):
    """The agent cannot run: pi missing, node missing, or no model credentials."""


class AgentBusy(RuntimeError):
    """That conversation is already mid-answer."""


class AgentForbidden(RuntimeError):
    """The conversation belongs to a different browser."""


# (conversation_id, client_id, owner) -> (extra env vars, extra CLI args) for a new
# pi process. The API installs one to hand pi its proxy token and model config;
# with none installed pi runs on its own defaults (used by the transport tests).
SessionEnvFactory = Callable[[str, "str | None", bool], "tuple[dict[str, str], list[str]]"]


def _pi_entry() -> Path:
    """Absolute path to pi's CLI JS, read from the installed package manifest.

    Running `node <cli.js>` rather than the `.bin/pi.cmd` shim keeps the command
    identical on Windows and POSIX and avoids the cmd wrapper entirely.
    """
    manifest = RUNTIME_DIR / "node_modules" / PI_PACKAGE.replace("/", os.sep) / "package.json"
    if not manifest.exists():
        raise AgentUnavailable(f"pi is not installed. Run `npm install` in {RUNTIME_DIR}.")
    bin_field = json.loads(manifest.read_text(encoding="utf-8")).get("bin") or {}
    relative = bin_field.get("pi") if isinstance(bin_field, dict) else bin_field
    if not relative:
        raise AgentUnavailable(f"{PI_PACKAGE} declares no `pi` bin entry")
    entry = manifest.parent / relative
    if not entry.exists():
        raise AgentUnavailable(f"pi entry point missing: {entry}")
    return entry


def _node_executable() -> str:
    return os.getenv("FOOTBALL_AI_NODE", "node")


@dataclass
class _Session:
    conversation_id: str
    process: subprocess.Popen
    events: queue.Queue = field(default_factory=queue.Queue)
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_used: float = field(default_factory=time.time)
    reader: threading.Thread | None = None
    stderr_tail: list[str] = field(default_factory=list)
    # The browser that started this conversation. Another browser presenting the
    # same conversation id is refused rather than handed this one's context.
    client_id: str | None = None

    def alive(self) -> bool:
        return self.process.poll() is None


class AgentRuntime:
    """Owns every live pi process. One instance per API process."""

    def __init__(self) -> None:
        self._sessions: dict[str, _Session] = {}
        self._guard = threading.Lock()
        self._session_env: SessionEnvFactory | None = None

    def use_session_env(self, factory: SessionEnvFactory | None) -> None:
        self._session_env = factory

    # -- lifecycle ---------------------------------------------------------

    def _spawn(self, conversation_id: str, client_id: str | None = None, owner: bool = False) -> _Session:
        entry = _pi_entry()
        args = [
            _node_executable(),
            str(entry),
            "--mode", "rpc",
            # Ephemeral: the browser owns conversation history, and writing
            # every fan's questions into ~/.pi is not our call to make.
            "--no-session",
            # Load exactly our extension, not whatever the developer happens to
            # have installed globally -- a user-facing agent should not inherit
            # someone's local tooling.
            "--no-extensions", "-e", str(EXTENSION),
            "--no-skills",
            "--no-prompt-templates",
            "--no-context-files",
            # No read/write/edit/bash. Football tools only.
            "--no-builtin-tools",
            "--system-prompt", SYSTEM_PROMPT,
        ]
        model = os.getenv("FOOTBALL_AI_AGENT_MODEL")
        if model:
            args += ["--model", model]
        provider = os.getenv("FOOTBALL_AI_AGENT_PROVIDER")
        if provider:
            args += ["--provider", provider]

        env = os.environ.copy()
        env.setdefault("FOOTBALL_AI_API", os.getenv("FOOTBALL_AI_API", "http://127.0.0.1:8000"))
        if self._session_env is not None:
            extra_env, extra_args = self._session_env(conversation_id, client_id, owner)
            env.update(extra_env)
            args += extra_args
        # Startup network calls (update check, telemetry) add latency to the
        # first prompt and tell pi.dev when this app is running. Neither helps.
        env.setdefault("PI_OFFLINE", "1")
        env.setdefault("PI_SKIP_VERSION_CHECK", "1")

        creation_flags = 0
        if sys.platform == "win32":
            # Keeps a console window from flashing up for every conversation.
            creation_flags = subprocess.CREATE_NO_WINDOW

        try:
            process = subprocess.Popen(
                args,
                cwd=str(RUNTIME_DIR),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creation_flags,
            )
        except FileNotFoundError as exc:
            raise AgentUnavailable(f"could not start node ({_node_executable()}): {exc}") from exc

        session = _Session(conversation_id=conversation_id, process=process, client_id=client_id)
        session.reader = threading.Thread(
            target=self._read_stdout,
            args=(session,),
            name=f"pi-rpc-{conversation_id[:8]}",
            daemon=True,
        )
        session.reader.start()
        threading.Thread(
            target=self._read_stderr,
            args=(session,),
            name=f"pi-err-{conversation_id[:8]}",
            daemon=True,
        ).start()
        logger.info("agent: started pi for conversation %s (pid %s)", conversation_id, process.pid)
        return session

    def _read_stdout(self, session: _Session) -> None:
        r"""Parse JSONL from pi.

        Records are delimited by LF only. Iterating a text-mode pipe splits on
        \r\n and \n but not on U+2028/U+2029, which is the trap pi's docs warn
        Node clients about, so this is protocol-safe; universal newlines strip
        the trailing \r.
        """
        stdout = session.process.stdout
        assert stdout is not None
        try:
            for line in stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    session.events.put(json.loads(line))
                except json.JSONDecodeError:
                    logger.debug("agent: non-JSON line from pi: %s", line[:200])
        except Exception as exc:  # pragma: no cover - pipe teardown
            logger.debug("agent: stdout reader ended: %s", exc)
        finally:
            session.events.put({"type": "_process_exit", "code": session.process.poll()})

    def _read_stderr(self, session: _Session) -> None:
        stderr = session.process.stderr
        assert stderr is not None
        try:
            for line in stderr:
                line = line.rstrip()
                if not line:
                    continue
                # Kept so a failed run can say *why* rather than "agent stopped".
                session.stderr_tail.append(line)
                del session.stderr_tail[:-20]
                logger.debug("agent[pi]: %s", line)
        except Exception:  # pragma: no cover - pipe teardown
            pass

    def _reap(self) -> None:
        now = time.time()
        for cid, session in list(self._sessions.items()):
            if not session.alive() or now - session.last_used > SESSION_TTL_SECONDS:
                self._close(cid)

    def _close(self, conversation_id: str) -> None:
        session = self._sessions.pop(conversation_id, None)
        if not session:
            return
        try:
            if session.process.stdin:
                session.process.stdin.close()
        except Exception:
            pass
        try:
            session.process.terminate()
            session.process.wait(timeout=5)
        except Exception:
            try:
                session.process.kill()
            except Exception:
                pass
        logger.info("agent: closed conversation %s", conversation_id)

    def close(self, conversation_id: str) -> None:
        with self._guard:
            self._close(conversation_id)

    def shutdown(self) -> None:
        with self._guard:
            for cid in list(self._sessions):
                self._close(cid)

    def _session_for(self, conversation_id: str, client_id: str | None = None, owner: bool = False) -> _Session:
        with self._guard:
            self._reap()
            session = self._sessions.get(conversation_id)
            if session and client_id is not None and session.client_id not in (None, client_id):
                raise AgentForbidden(conversation_id)
            if session and session.alive():
                session.last_used = time.time()
                return session
            if session:
                self._close(conversation_id)
            if len(self._sessions) >= MAX_SESSIONS:
                oldest = min(self._sessions, key=lambda c: self._sessions[c].last_used)
                self._close(oldest)
            session = self._spawn(conversation_id, client_id, owner)
            self._sessions[conversation_id] = session
            return session

    # -- prompting ---------------------------------------------------------

    def prompt(
        self, conversation_id: str, message: str, client_id: str | None = None, owner: bool = False
    ) -> Iterator[dict]:
        """Send a prompt and yield pi's events until the run settles.

        Yields raw pi events; shaping for the browser happens in the route so
        this stays a transport.

        A conversation holds its process between questions, so a pi that died
        after the last answer is only discovered on this send. Retried once, and
        only when nothing has been streamed yet: otherwise the user pays for a
        crash with a failed question they then have to retype.
        """
        for attempt in (0, 1):
            streamed = False
            for event in self._run(conversation_id, message, client_id, owner):
                if event.get("type") == "_dead_on_arrival":
                    if attempt == 0 and not streamed:
                        self.close(conversation_id)
                        break
                    yield {"type": "_error", "message": event["message"]}
                    return
                streamed = True
                yield event
            else:
                return

    def _run(
        self, conversation_id: str, message: str, client_id: str | None = None, owner: bool = False
    ) -> Iterator[dict]:
        session = self._session_for(conversation_id, client_id, owner)
        if not session.lock.acquire(blocking=False):
            raise AgentBusy(conversation_id)
        try:
            # Drain anything left from an aborted previous run so this run's
            # terminal event is unambiguous.
            while True:
                try:
                    session.events.get_nowait()
                except queue.Empty:
                    break

            request_id = str(uuid.uuid4())
            self._send(session, {"id": request_id, "type": "prompt", "message": message})

            failure = None
            retryable = False
            while True:
                try:
                    event = session.events.get(timeout=EVENT_TIMEOUT_SECONDS)
                except queue.Empty:
                    failure = "the agent stopped responding"
                    break

                kind = event.get("type")
                if kind == "_process_exit":
                    tail = "; ".join(session.stderr_tail[-5:]) or "no output"
                    failure = f"the agent process exited ({tail})"
                    retryable = True
                    break
                if kind == "response" and event.get("id") == request_id and not event.get("success"):
                    failure = str(event.get("error") or "the agent rejected the prompt")
                    break

                yield event

                # agent_settled, not agent_end: agent_end fires per low-level
                # run and can be followed by retries, compaction and queued
                # continuations. Settling is what "finished" means.
                if kind == "agent_settled":
                    break

            if failure:
                # _dead_on_arrival is the caller's cue that respawning may fix
                # this; every other failure is final.
                yield {
                    "type": "_dead_on_arrival" if retryable else "_error",
                    "message": failure,
                }
        finally:
            session.last_used = time.time()
            session.lock.release()

    def abort(self, conversation_id: str) -> bool:
        session = self._sessions.get(conversation_id)
        if not session or not session.alive():
            return False
        self._send(session, {"type": "abort"})
        return True

    def _send(self, session: _Session, command: dict) -> None:
        stdin = session.process.stdin
        if stdin is None or session.process.poll() is not None:
            raise AgentUnavailable("the agent process is not running")
        stdin.write(json.dumps(command) + "\n")
        stdin.flush()

    # -- introspection -----------------------------------------------------

    def status(self) -> dict:
        try:
            entry: str | None = str(_pi_entry())
            installed, error = True, None
        except AgentUnavailable as exc:
            entry, installed, error = None, False, str(exc)
        return {
            "installed": installed,
            "entry": entry,
            "error": error,
            "extension": str(EXTENSION) if EXTENSION.exists() else None,
            "live_conversations": len(self._sessions),
            "max_sessions": MAX_SESSIONS,
            "model": os.getenv("FOOTBALL_AI_AGENT_MODEL") or "pi default",
        }


runtime = AgentRuntime()
