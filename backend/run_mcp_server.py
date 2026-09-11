#!/usr/bin/env python3
"""Launcher for the Football-Ai MCP server.

MCP clients start a server with a bare command and whatever working directory
they happen to be in, so `python -m mcp_server.server` only resolves when the
client is launched from `backend/`. This entry point puts its own directory on
sys.path first, so it works from anywhere:

    python /abs/path/to/backend/run_mcp_server.py

Configuration is via environment: FOOTBALL_AI_API, FOOTBALL_AI_TIMEOUT.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mcp_server.server import main  # noqa: E402

if __name__ == "__main__":
    main()
