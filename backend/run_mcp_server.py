#!/usr/bin/env python3
"""Launcher for the Football-Ai MCP server.

Two problems this solves, both of which otherwise force absolute paths into
every client's config:

1. MCP clients start a server with a bare command and whatever working directory
   they happen to be in, so `python -m mcp_server.server` only resolves when the
   client is launched from `backend/`. This file puts its own directory on
   sys.path first, so it works from anywhere.

2. The server needs the project virtualenv (mcp, httpx). Rather than making
   every config hardcode `.../backend/.venv/Scripts/python.exe`, this re-execs
   itself with the venv interpreter sitting next to it when started by some
   other Python. So `python backend/run_mcp_server.py` is enough, and the path
   is relative to the repo rather than to one machine.

Configuration is via environment: FOOTBALL_AI_API, FOOTBALL_AI_TIMEOUT,
FOOTBALL_AI_CACHE.
"""

import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
_REEXEC_FLAG = "FOOTBALL_AI_MCP_REEXEC"


def _venv_python() -> str | None:
    """The project venv interpreter, if one exists beside this file."""
    for relative in (("Scripts", "python.exe"), ("bin", "python3"), ("bin", "python")):
        candidate = os.path.join(HERE, ".venv", *relative)
        if os.path.isfile(candidate):
            return candidate
    return None


def _ensure_venv() -> None:
    """Hand off to the project venv when started by a different interpreter.

    A subprocess that INHERITS this process's stdin/stdout, rather than
    os.exec*. On Windows os.exec* is emulated by spawning a new process and
    terminating the current one, which severs the pipes the MCP client is
    holding -- the handshake then hangs forever with no error. Staying alive as
    a thin parent keeps those handles valid on every platform.

    The guard env var stops a spawn loop if the venv interpreter still cannot
    import the dependencies: better a real ImportError than infinite processes.
    """
    if os.environ.get(_REEXEC_FLAG):
        return
    target = _venv_python()
    if not target:
        return
    if os.path.normcase(os.path.abspath(target)) == os.path.normcase(os.path.abspath(sys.executable)):
        return

    env = dict(os.environ, **{_REEXEC_FLAG: "1"})
    # stdin/stdout/stderr are left as None so the child inherits ours directly.
    completed = subprocess.run([target, os.path.abspath(__file__), *sys.argv[1:]], env=env)
    sys.exit(completed.returncode)


_ensure_venv()

if HERE not in sys.path:
    sys.path.insert(0, HERE)

try:
    from mcp_server.server import main
except ImportError as exc:  # pragma: no cover - surfaced to the MCP client
    sys.stderr.write(
        f"Football-Ai MCP server could not start: {exc}\n"
        f"Expected the project virtualenv at {os.path.join(HERE, '.venv')}. "
        "Create it and install backend/requirements.txt, or run this file with "
        "an interpreter that already has 'mcp' and 'httpx'.\n"
    )
    raise

if __name__ == "__main__":
    main()
