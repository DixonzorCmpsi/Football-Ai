# The in-app agent

The floating "Ask the Spot" box in the frontend is a [pi](https://github.com/earendil-works/pi)
coding agent, vendored here, driven over RPC by the FastAPI backend, and given
exactly one set of tools: this app's own football data.

```
browser (AgentDock)
   │  POST /agent/chat            SSE: delta / tool / done / error
   ▼
FastAPI  routes/agent.py
   │  stdin/stdout JSONL          pi --mode rpc
   ▼
pi (node)  ── loads runtime/football-tools.ts
   │  POST /agent/tools/{name}
   ▼
FastAPI  agent/tools.py ── the same functions backend/mcp_server publishes
```

## Why it looks like this

**pi has no MCP client, by design.** So the in-app agent cannot speak to
`backend/mcp_server` over stdio the way Claude Code does. Rather than keep a
second copy of sixteen tools and their formatters in TypeScript, the extension
asks the backend what tools exist and proxies each call back to it. The tool
bodies are the same Python functions in both paths, so the agent in the browser
and the agent in the terminal quote identical numbers.

`agent/tools.py` derives each tool's JSON Schema from the function's own
signature and docstring. Adding a tool is: write it in `mcp_server/server.py`,
add its name to `EXPOSED_TOOLS`. Nothing in the extension changes.

**The agent has no shell.** It is spawned with `--no-builtin-tools`, so
`read`, `write`, `edit` and `bash` are absent; football tools are all it has.
`get_raw` and `refresh` are also withheld — an arbitrary-path tool and a
cache-flushing tool are not things a fan should be able to drive from a chat box.

**One process per conversation**, reaped after 30 minutes idle, capped at 8.
Each is a Node process; a crashed one is respawned and the question retried once,
so a crash costs a slow answer rather than a failed one.

## Setup

```bash
cd backend/agent/runtime
npm install          # pinned; package-lock.json is committed
```

pi needs model credentials. Either set an API key in the backend's environment:

```bash
ANTHROPIC_API_KEY=sk-ant-...
```

or authenticate a subscription once, which writes to `~/.pi/agent` and is picked
up automatically:

```bash
cd backend/agent/runtime
node node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js
/login
```

`GET /agent/status` reports whether the runtime is installed and how many tools
it found. Without credentials the agent starts, loads its tools, and fails at
the first model call with the provider's own message, which is surfaced in the
chat box rather than swallowed.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `FOOTBALL_AI_API` | `http://127.0.0.1:8000` | Where the extension fetches tools (127.0.0.1, not `localhost`: see `mcp_server/server.py`) |
| `FOOTBALL_AI_AGENT_MODEL` | pi's default | e.g. `claude-sonnet-5` |
| `FOOTBALL_AI_AGENT_PROVIDER` | pi's default | e.g. `anthropic` |
| `FOOTBALL_AI_AGENT_TTL` | `1800` | Seconds a conversation's process survives idle |
| `FOOTBALL_AI_AGENT_MAX_SESSIONS` | `8` | Concurrent conversations |
| `FOOTBALL_AI_AGENT_EVENT_TIMEOUT` | `120` | Seconds to wait for the next event before calling a run wedged |
| `FOOTBALL_AI_NODE` | `node` | Node executable |
| `FOOTBALL_AI_TOOL_TIMEOUT_MS` | `60000` | Per-tool HTTP timeout inside the extension |

## Upgrading pi

Bump the version in `runtime/package.json`, `npm install`, run
`backend/tests/test_agent_runtime.py`. Nothing resolves pi by path — the CLI
entry point is read from the installed package's own `bin` field — so a version
bump needs no code change. Check `docs/rpc.md` in the new package for protocol
changes; the events this code depends on are `message_update`,
`tool_execution_start`/`_end` and `agent_settled`.

## Tests

| File | Covers |
|---|---|
| `tests/test_agent_tools.py` | Schema derivation, the withheld tools, importability without the MCP SDK |
| `tests/test_agent_chat.py` | Screen descriptors, prompt assembly, RPC event → UI event translation |
| `tests/test_agent_runtime.py` | Spawning, JSONL framing, crash recovery, session capping — against a stub that speaks the protocol, so no model and no key needed |
