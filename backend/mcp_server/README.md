# Football-Ai MCP server

Exposes the app's projections, matchup context, betting lines and Sleeper
analysis as [MCP](https://modelcontextprotocol.io) tools, so an agent can answer
*"should I start Purdy this week?"* against the same model the UI uses.

It talks to the running FastAPI backend over HTTP rather than importing it. One
server can therefore point at a local dev instance or a deployed one, and the
agent process never holds the models, the DB connection, or a 2 GB footprint.

## Running

The backend must be up first (default `http://localhost:8000`).

```bash
python backend/run_mcp_server.py                     # stdio transport, any cwd
FOOTBALL_AI_API=http://otherhost:8000 python backend/run_mcp_server.py
```

`run_mcp_server.py` exists because MCP clients start a server with a bare command
and whatever working directory they happen to be in. `python -m mcp_server.server`
only resolves from inside `backend/`; the launcher puts its own directory on
`sys.path`, so an absolute path works from anywhere.

| env var | default | meaning |
|---|---|---|
| `FOOTBALL_AI_API` | `http://localhost:8000` | backend base URL |
| `FOOTBALL_AI_TIMEOUT` | `30` | per-request timeout, seconds |

## Registering with Claude Code

```bash
claude mcp add football-ai --scope project -- /abs/path/backend/.venv/Scripts/python.exe /abs/path/backend/run_mcp_server.py
```

That writes `.mcp.json` at the repo root. Both paths are absolute and therefore
machine-specific — adjust them if the checkout lives elsewhere.

For Claude Desktop, the equivalent `mcpServers` entry in
`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "football-ai": {
      "command": "C:/dev/Football-Ai/backend/.venv/Scripts/python.exe",
      "args": ["C:/dev/Football-Ai/backend/run_mcp_server.py"],
      "env": { "FOOTBALL_AI_API": "http://localhost:8000" }
    }
  }
}
```

## Tools

**Discovery**

| tool | purpose |
|---|---|
| `get_status` | current week, models loaded, data freshness, row counts |
| `search_players` | partial-name search returning ids |
| `get_schedule` | a week's games, with scores where final |

**Players**

| tool | purpose |
|---|---|
| `get_player_projection` | projection, floor, lines, props, injury |
| `get_player_history` | recent weekly game log |
| `get_player_storylines` | latest news |
| `compare_players` | side-by-side, for start/sit calls |

**Matchups**

| tool | purpose |
|---|---|
| `get_matchup` | line, weather, top projected players, injuries |
| `get_matchup_insights` | model-derived narrative for one game |
| `get_parlays` | suggested legs, optionally per game |

**Sleeper**

| tool | purpose |
|---|---|
| `sleeper_find_leagues` | a user's leagues (username only, no password) |
| `sleeper_list_teams` | teams + the `roster_id` the next call needs |
| `sleeper_analyze_roster` | projected lineup vs the saved one |
| `sleeper_waiver_targets` | free agents ranked by our projection |

**Escape hatch**

`get_raw(path, params_json)` calls any backend GET endpoint and returns JSON,
truncated to keep context usable. For data the shaped tools don't cover
(`/team/SF/offense`, `/tier_list`, …).

## Design notes

**Tools return prose, not JSON.** The REST responses are built for a UI: one
matchup is ~31 player cards with a dozen prop rows and a headshot URL each.
Handing that over verbatim burns context on data a model can't use and buries
the few numbers that matter. Shaping lives in `formatting.py`, which is pure and
unit-tested; `get_raw` is there when the full payload is genuinely wanted.

**Names, not ids.** Agents have "Brock Purdy", not `00-0037834`. Every
player-facing tool resolves either. An ambiguous name comes back as a list to
pick from rather than a silent wrong guess.

**`week=0` means the current week**, so the common case needs no lookup first.

**Failures say what to do.** An unreachable backend names the URL it tried. An
unknown team abbreviation says to check the abbreviation and the week — the
matchup endpoint answers `200` with empty rosters for teams that don't play each
other, so an empty board is treated as "not found" rather than reported as a game.

## Two bugs this work surfaced

Running the tools against the live backend (rather than just reading the code)
turned up two defects in the app itself:

- **`snap_percentage` read `1%` for every full-time starter.** The conversion in
  `prediction.py` was `if snap_pct < 1.0 and snap_pct > 0`, which excludes
  exactly `1.0` — a player who took every snap. Now inclusive.
- **`/player/history` uses `points` / `passing_yds`**, not the weekly-stats
  spelling, so the first version of the game-log formatter printed `- pts` for
  every row.
