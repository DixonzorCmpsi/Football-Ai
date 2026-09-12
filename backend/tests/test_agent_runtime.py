"""Transport tests for the pi supervisor, against a stub that speaks the protocol.

No model and no API key involved: what is under test is the part that has
actually broken before on this machine -- spawning a child, framing JSONL over
its pipes, and deciding when a run is finished or dead.

The stub is a Python script standing in for `pi --mode rpc`. It ignores pi's
flags, which is the point: the supervisor is exercised through Popen, the reader
thread and the queue exactly as in production.
"""

import json
import sys
import textwrap

import pytest

from agent import pi_runtime


STUB = textwrap.dedent(
    """
    import json, sys
    # Ignores argv (pi's flags) and answers the first prompt it is given.
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        command = json.loads(line)
        if command.get("type") != "prompt":
            continue
        out = sys.stdout
        emit = lambda obj: (out.write(json.dumps(obj) + "\\n"), out.flush())
        emit({"type": "response", "command": "prompt", "success": True, "id": command.get("id")})
        emit({"type": "agent_start"})
        emit({"type": "tool_execution_start", "toolName": "get_matchup", "toolCallId": "c1"})
        emit({"type": "tool_execution_end", "toolName": "get_matchup", "toolCallId": "c1"})
        for chunk in ("Start ", "Kyren ", "Williams."):
            emit({"type": "message_update",
                  "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": chunk}})
        emit({"type": "agent_end"})
        emit({"type": "agent_settled"})
        break
    """
)

DIES_IMMEDIATELY = textwrap.dedent(
    """
    import sys
    sys.stderr.write("no API key configured\\n")
    sys.exit(2)
    """
)


@pytest.fixture
def stubbed(tmp_path, monkeypatch):
    """Point the supervisor at a stub process instead of pi."""

    def install(source: str) -> pi_runtime.AgentRuntime:
        script = tmp_path / "stub_pi.py"
        script.write_text(source, encoding="utf-8")
        monkeypatch.setattr(pi_runtime, "_pi_entry", lambda: script)
        monkeypatch.setattr(pi_runtime, "_node_executable", lambda: sys.executable)
        return pi_runtime.AgentRuntime()

    runtimes: list[pi_runtime.AgentRuntime] = []

    def factory(source: str) -> pi_runtime.AgentRuntime:
        runtime = install(source)
        runtimes.append(runtime)
        return runtime

    yield factory
    for runtime in runtimes:
        runtime.shutdown()


def test_a_prompt_streams_events_and_stops_at_settled(stubbed):
    runtime = stubbed(STUB)
    events = list(runtime.prompt("conv-1", "who should I start?"))

    kinds = [e["type"] for e in events]
    assert kinds[-1] == "agent_settled", "the run must end on the terminal event"
    assert "_error" not in kinds
    assert "tool_execution_start" in kinds

    deltas = [
        e["assistantMessageEvent"]["delta"]
        for e in events
        if e["type"] == "message_update"
    ]
    assert "".join(deltas) == "Start Kyren Williams."


def test_the_prompt_reaches_the_child_intact(stubbed, tmp_path):
    """Framing is LF-delimited JSON; a mangled command would silently hang."""
    recorder = tmp_path / "seen.json"
    runtime = stubbed(
        textwrap.dedent(
            f"""
            import json, sys
            line = sys.stdin.readline()
            open({str(recorder)!r}, "w", encoding="utf-8").write(line)
            sys.stdout.write(json.dumps({{"type": "agent_settled"}}) + "\\n")
            sys.stdout.flush()
            """
        )
    )
    list(runtime.prompt("conv-framing", "[On screen: the SF at LA game page]\n\nwho?"))

    sent = json.loads(recorder.read_text(encoding="utf-8"))
    assert sent["type"] == "prompt"
    assert sent["message"].endswith("who?")
    assert "[On screen:" in sent["message"]


def test_a_dead_child_reports_why(stubbed):
    """"The agent stopped" is useless; the child's stderr says what went wrong."""
    runtime = stubbed(DIES_IMMEDIATELY)
    events = list(runtime.prompt("conv-dead", "hello"))

    assert events[-1]["type"] == "_error"
    assert "no API key configured" in events[-1]["message"]


def test_one_run_at_a_time_per_conversation(stubbed):
    runtime = stubbed(STUB)
    stream = runtime.prompt("conv-busy", "first")
    next(stream)  # start the run, holding the lock

    with pytest.raises(pi_runtime.AgentBusy):
        list(runtime.prompt("conv-busy", "second"))

    list(stream)  # drain, releasing the lock


def test_a_conversation_reuses_its_process(stubbed):
    """Node startup is the expensive part of a question; paying it once matters."""
    # Same stub without the trailing `break`, so it keeps serving prompts the
    # way a real pi process does. STUB is already dedented, hence four spaces.
    looping = STUB.replace("\n    break", "")
    assert looping != STUB, "the stub still exits after one prompt"
    runtime = stubbed(looping)
    list(runtime.prompt("conv-reuse", "first"))
    pid = runtime._sessions["conv-reuse"].process.pid
    list(runtime.prompt("conv-reuse", "second"))
    assert runtime._sessions["conv-reuse"].process.pid == pid


def test_a_crashed_child_is_replaced_rather_than_failing_the_question(stubbed):
    """A conversation holds its process, so a pi that died after the last answer
    is only discovered when the next question is sent. Retrying once turns that
    into a slow answer instead of an error the user has to retype past."""
    runtime = stubbed(STUB)  # this stub exits after a single prompt
    first = list(runtime.prompt("conv-crash", "first"))
    assert first[-1]["type"] == "agent_settled"

    second = list(runtime.prompt("conv-crash", "second"))
    assert second[-1]["type"] == "agent_settled", "the retry should have answered"
    assert not any(e["type"] == "_error" for e in second)


def test_a_child_that_never_starts_gives_up_after_one_retry(stubbed):
    """Retrying forever would hang the request; one attempt then the reason."""
    runtime = stubbed(DIES_IMMEDIATELY)
    events = list(runtime.prompt("conv-hopeless", "hello"))
    assert [e["type"] for e in events] == ["_error"]
    assert "no API key configured" in events[0]["message"]


def test_sessions_are_capped(stubbed, monkeypatch):
    """Each session is a Node process; unbounded conversations exhaust memory."""
    monkeypatch.setattr(pi_runtime, "MAX_SESSIONS", 2)
    runtime = stubbed(STUB)
    for i in range(4):
        runtime._session_for(f"conv-{i}")
    assert len(runtime._sessions) <= 2


def test_closing_a_conversation_stops_its_process(stubbed):
    runtime = stubbed(STUB)
    session = runtime._session_for("conv-close")
    process = session.process
    runtime.close("conv-close")
    assert "conv-close" not in runtime._sessions
    assert process.poll() is not None


def test_the_pi_entry_point_is_resolved_from_the_package():
    """No hardcoded paths: a version bump must not need an edit here."""
    try:
        entry = pi_runtime._pi_entry()
    except pi_runtime.AgentUnavailable:
        pytest.skip("pi is not installed in this checkout (npm install in agent/runtime)")
    assert entry.is_file()
    assert entry.suffix == ".js"


def test_status_reports_an_uninstalled_runtime_instead_of_raising(monkeypatch, tmp_path):
    monkeypatch.setattr(pi_runtime, "RUNTIME_DIR", tmp_path)
    status = pi_runtime.AgentRuntime().status()
    assert status["installed"] is False
    assert "npm install" in status["error"]
