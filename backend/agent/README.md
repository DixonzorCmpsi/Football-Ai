# The in-app agent

The floating "Ask the Spot" box in the frontend is a [pi](https://github.com/earendil-works/pi)
coding agent, vendored here, driven over RPC by the FastAPI backend, and given
exactly one set of tools: this app's own football data. It runs on free models
through a key-holding proxy by default, or on a user's own provider key.

```
browser (AgentDock)                     X-Client-Id, optional BYOK settings
   │  POST /agent/chat                  SSE: quota / delta / tool / done / error
   ▼
FastAPI  routes/agent.py ── daily limits (services/usage_limits.py)
   │  stdin/stdout JSONL                pi --mode rpc, holding only a session token
   ▼
pi (node) ── loads runtime/football-tools.ts
   │                        │
   │ POST /agent/tools/{n}  │ POST /llm/v1/chat/completions   (Bearer <session token>)
   ▼                        ▼
agent/tools.py          routes/llm_proxy.py ── swaps in the real key and model
(same functions as          │
 backend/mcp_server)        ├─ house: our key (secret store) → free models, shared daily budget
                            └─ byok:  the user's key → OpenRouter, Ollama Cloud, OpenAI, Groq,
                                      Gemini, or a custom OpenAI-compatible gateway (LiteLLM, vLLM)
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

## Inference

pi never holds a provider key. Each pi process is started with a generated
`models.json` (in `runtime/.pi-agent/`, git-ignored) that points one model alias,
`spot/assistant`, at this backend's `/llm/v1`, plus a signed per-conversation
token as its "API key". The proxy verifies the token and picks the real upstream:

- **House tier (default).** Our key, from the secret store, on the free models in
  `INFERENCE_HOUSE_MODELS`. OpenRouter falls through that list when one model is
  down or rate limited. Users need no key.
- **Bring your own key.** The user picks a provider, pastes a key and chooses a
  model in the dock's settings. The key lives in their browser (sessionStorage
  unless they tick "remember") and is sent with each question. The server keeps it
  in memory for that conversation only, and never writes or logs it.

Model ids are not hardcoded anywhere a user sees them: the settings UI lists each
provider's live `/models` (OpenRouter filtered to tool-capable models, since the
agent needs tool calling), and **Test** makes one tiny request with a tool
attached, so a bad key or unusable model fails in settings, not on a question.

### Limits

| Limit | Default | Applies to |
|---|---|---|
| `AGENT_DAILY_QUESTIONS` | 25 | per browser id, house tier |
| `AGENT_DAILY_QUESTIONS_PER_IP` | 75 | per IP, house tier (a browser id is reset by clearing site data) |
| `INFERENCE_HOUSE_DAILY_REQUEST_BUDGET` | 45 | upstream model calls per day, all users combined |
| `AGENT_MAX_LLM_CALLS_PER_QUESTION` | 8 | per question, both tiers (stops tool-call loops) |

Days reset at midnight US Eastern. Counters live in Postgres (`agent_usage`, created
on first use) so every instance shares them. A question that fails before
answering is refunded. BYOK questions don't count against the house limits.

**The house budget exists because of OpenRouter's free-tier cap:** free models are
limited to **50 requests/day account-wide** (and 20/minute) unless the account has
bought at least **$10 of credits, which raises it to 1000/day**. One question is
usually 2-4 model calls. Buy the credits, then raise
`INFERENCE_HOUSE_DAILY_REQUEST_BUDGET` to about 950.

**Owner access.** Set `AGENT_OWNER_TOKEN`, then open the app once with
`?owner=<token>`. The browser keeps it and removes it from the address bar. The
owner skips the per-browser and per-IP limits, but not the upstream budget, which
the provider enforces anyway.

**Refusals don't retry.** pi auto-retries errors whose text looks transient
(`429`, `503`, "rate limit"...) unless the text also says something terminal like
"quota exceeded". The proxy's deliberate refusals carry that phrase, or use 403, so
the user sees the reason at once instead of after ~15s of backoff. A genuine
upstream 429 stays retryable.

### Custom gateway URLs

A BYOK `custom` base URL is the one place a user can make the server call a host of
their choosing. It must be https, must not embed credentials, and every address
it resolves to must be public, which rules out `169.254.169.254` (GCP metadata,
which issues service-account tokens), loopback and VPC ranges. The URL is
re-checked on every call, and redirects are never followed. For a gateway on your
own machine during development, set `INFERENCE_ALLOW_PRIVATE_BYOK_URLS=true`;
never set it in a deployed environment.

## Setup

```bash
cd backend/agent/runtime
npm install          # pinned; package-lock.json is committed
```

Locally, secrets go in `backend/.env` (git-ignored, loaded by `config.py`):

```bash
OPENROUTER_API_KEY=sk-or-...         # turns on the free tier
AGENT_OWNER_TOKEN=...                # your unlimited access
AGENT_PROXY_SIGNING_KEY=...          # any long random string
```

`GET /agent/status` shows what is configured (never the values): whether pi is
installed, the tool count, the house provider and models, **where the key was
found** (`env`, `file`, `secret-manager` or `missing`) and today's remaining budget.

## Deploying on GCP (Cloud Run)

Nothing in the code changes between local and Cloud Run. `secret_store.get_secret`
tries, in order: an environment variable, a mounted file named by `<NAME>_FILE`,
then Secret Manager directly when `GCP_PROJECT_ID` is set.

```bash
# once
for s in openrouter-api-key agent-owner-token agent-proxy-signing-key; do
  gcloud secrets create $s --replication-policy=automatic
done
printf '%s' "sk-or-..." | gcloud secrets versions add openrouter-api-key --data-file=-
# (same for the other two; the signing key must be one value shared by all instances)

gcloud secrets add-iam-policy-binding openrouter-api-key \
  --member=serviceAccount:<run-service-account> --role=roles/secretmanager.secretAccessor

# deploy: map secrets to env vars, which needs no code at all
gcloud run deploy football-ai-api ... \
  --set-secrets OPENROUTER_API_KEY=openrouter-api-key:latest,AGENT_OWNER_TOKEN=agent-owner-token:latest,AGENT_PROXY_SIGNING_KEY=agent-proxy-signing-key:latest \
  --session-affinity \
  --set-env-vars FOOTBALL_AI_API=http://127.0.0.1:8080,TRUST_PROXY_HEADERS=true,PUBLIC_APP_URL=https://<your-domain>
```

Notes for that environment:

- `FOOTBALL_AI_API` must use the container's own port (Cloud Run's `PORT`, 8080 by
  default): pi reaches both the tools and the proxy over loopback inside the
  same container.
- `TRUST_PROXY_HEADERS=true` makes the per-IP limit use the client's address from
  `X-Forwarded-For`. Without it every user shares Google's front-end address.
- pi processes are per instance, and so are BYOK keys held in memory. A question
  and its model calls always stay on one instance, because pi calls back over
  loopback. Deploy with `--session-affinity` so a follow-up question usually lands on
  the same instance; on another instance the conversation simply starts fresh.
- `DB_CONNECTION_STRING` (Cloud SQL) is what makes the limits shared. Without it
  each instance counts separately.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `INFERENCE_HOUSE_PROVIDER` | `openrouter` | Any hosted provider id from `services/inference_providers.py` |
| `INFERENCE_HOUSE_MODELS` | two free models | Comma-separated; the first is primary, the rest are fallbacks |
| `INFERENCE_HOUSE_API_KEY` | (unset) | Overrides the provider's own key name (e.g. `OPENROUTER_API_KEY`) |
| `INFERENCE_HOUSE_BASE_URL` | provider default | For pointing the house tier at a local stub in tests |
| `PUBLIC_APP_URL` | (unset) | Sent to OpenRouter as `HTTP-Referer` for attribution |
| `AGENT_BYOK_TTL` | `3600` | Seconds an unused BYOK key is kept in memory |
| `AGENT_PROXY_TOKEN_TTL` | `43200` | Session token lifetime |
| `AGENT_CONTEXT_WINDOW` / `AGENT_MAX_OUTPUT_TOKENS` | `128000` / `2048` | What pi assumes about the model |
| `SECRET_MANAGER_PREFIX` | (empty) | Prefix for Secret Manager ids, e.g. `football-ai-` |
| `SECRET_CACHE_SECONDS` | `300` | How long a fetched secret is reused before re-reading |
| `FOOTBALL_AI_API` | `http://127.0.0.1:8000` | Where pi reaches the tools and the proxy (127.0.0.1, not `localhost`: see `mcp_server/server.py`) |
| `FOOTBALL_AI_AGENT_MODEL` / `_PROVIDER` | (unset) | Bypass the proxy and run pi against a provider directly. Development only |
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
| `tests/test_inference_proxy.py` | Model/key swapping, BYOK isolation, non-retryable refusals, quota and owner bypass over HTTP, error unwrapping |
| `tests/test_usage_limits.py` | Per-browser, per-IP and budget limits, refunds, day rollover, and a concurrent race against real Postgres |
| `tests/test_inference_security.py` | Secret resolution (env, file, Secret Manager), token forgery and expiry, SSRF refusals |
