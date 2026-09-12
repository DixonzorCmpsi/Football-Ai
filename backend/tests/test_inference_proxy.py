"""The inference proxy, against a fake upstream that records what it was sent.

What must hold: pi's alias model becomes the real one, the real key is added
upstream and never comes from pi, BYOK calls use the user's key and never the
house's, limits refuse in a way pi won't retry, and streams pass through intact.
"""

import json
import re

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from applications.api import secret_store
from applications.api.rate_limit import limiter as ip_limiter
from applications.api.routes import agent as agent_routes
from applications.api.routes import llm_proxy as proxy_routes
from applications.api.services import agent_tokens
from applications.api.services import llm_proxy as proxy
from applications.api.services import usage_limits

HOUSE_KEY = "sk-or-house-key-000"
USER_KEY = "sk-user-own-key-111"


class FakeUpstream:
    def __init__(self):
        self.calls = []
        self.status = 200
        self.stream_chunks = [
            b'data: {"choices":[{"delta":{"content":"Purdy "}}]}\n\n',
            b'data: {"choices":[{"delta":{"content":"22.5"}}]}\n\n',
            b"data: [DONE]\n\n",
        ]
        self.error_body = b'{"error":{"message":"nope"}}'

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        self.calls.append({"url": str(request.url), "headers": dict(request.headers), "body": body})
        if self.status != 200:
            return httpx.Response(self.status, content=self.error_body, headers={"retry-after": "7"})
        if request.url.path.endswith("/models"):
            return httpx.Response(200, json={"data": [
                {"id": "tool-model", "supported_parameters": ["tools"]},
                {"id": "no-tools-model", "supported_parameters": []},
            ]})
        if body.get("stream"):
            return httpx.Response(200, content=b"".join(self.stream_chunks), headers={"content-type": "text/event-stream"})
        return httpx.Response(200, json={"model": body["model"], "choices": [{"message": {"content": "OK"}}]})


@pytest.fixture
def upstream(monkeypatch):
    fake = FakeUpstream()
    monkeypatch.setattr(proxy, "_transport", httpx.MockTransport(fake.handler))
    return fake


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("AGENT_PROXY_SIGNING_KEY", "s" * 40)
    monkeypatch.setenv("OPENROUTER_API_KEY", HOUSE_KEY)
    monkeypatch.setenv("INFERENCE_HOUSE_PROVIDER", "openrouter")
    monkeypatch.setenv("INFERENCE_HOUSE_MODELS", "free/model-a:free,free/model-b:free")
    monkeypatch.setenv("AGENT_DAILY_QUESTIONS", "2")
    monkeypatch.setenv("INFERENCE_HOUSE_DAILY_REQUEST_BUDGET", "50")
    monkeypatch.delenv("INFERENCE_HOUSE_API_KEY", raising=False)
    monkeypatch.delenv("INFERENCE_HOUSE_BASE_URL", raising=False)
    monkeypatch.delenv("AGENT_OWNER_TOKEN", raising=False)
    monkeypatch.delenv("INFERENCE_ALLOW_PRIVATE_BYOK_URLS", raising=False)
    secret_store.clear_cache()
    monkeypatch.setattr(usage_limits, "_limiter", usage_limits.UsageLimiter(uri=None))
    yield
    secret_store.clear_cache()


@pytest.fixture
def client(env, upstream):
    app = FastAPI()
    app.state.limiter = ip_limiter
    app.include_router(proxy_routes.router)
    app.include_router(agent_routes.router)
    return TestClient(app)


def _pi_call(client, token, **extra):
    body = {"model": "assistant", "stream": True, "max_tokens": 512, "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return client.post("/llm/v1/chat/completions", json=body, headers={"authorization": f"Bearer {token}"})


def _token(conversation="conv-aaaaaaaa", client_id="browser-aaaaaaaa", owner=False):
    return agent_tokens.mint(conversation, client_id, owner)


# --- house tier -----------------------------------------------------------------------

def test_house_call_swaps_in_real_model_and_key(client, upstream):
    proxy.begin_question("conv-aaaaaaaa")
    response = _pi_call(client, _token())
    assert response.status_code == 200
    assert response.text.count("data:") == 3, "stream relayed intact"

    call = upstream.calls[-1]
    assert call["url"] == "https://openrouter.ai/api/v1/chat/completions"
    assert call["headers"]["authorization"] == f"Bearer {HOUSE_KEY}"
    assert call["body"]["model"] == "free/model-a:free"
    # OpenRouter falls through the list when the first free model is down or limited.
    assert call["body"]["models"] == ["free/model-a:free", "free/model-b:free"]
    assert call["headers"]["x-title"] == "THE SPOT AI"


def test_pi_never_sees_or_supplies_the_real_key(client, upstream):
    """pi's Authorization is the session token; it must not be forwarded upstream."""
    token = _token()
    proxy.begin_question("conv-aaaaaaaa")
    _pi_call(client, token)
    assert token not in json.dumps(upstream.calls[-1])


def test_requests_without_a_valid_token_are_refused(client, upstream):
    for header in ({}, {"authorization": "Bearer sk-or-guess"}, {"authorization": "Bearer spt_forged.sig"}):
        response = client.post("/llm/v1/chat/completions", json={"model": "x"}, headers=header)
        assert response.status_code == 401
    assert upstream.calls == [], "nothing reached the provider"


def test_house_budget_exhaustion_is_terminal_for_pi(client, upstream, monkeypatch):
    """The refusal must match pi's NON-retryable pattern, or pi backs off and retries a final 'no'."""
    monkeypatch.setenv("INFERENCE_HOUSE_DAILY_REQUEST_BUDGET", "1")
    proxy.begin_question("conv-aaaaaaaa")
    assert _pi_call(client, _token()).status_code == 200
    refused = _pi_call(client, _token())
    assert refused.status_code == 429
    message = refused.json()["error"]["message"]
    assert re.search(r"quota exceeded", message, re.I), message
    assert len(upstream.calls) == 1


def test_runaway_tool_loops_are_stopped(client, upstream, monkeypatch):
    monkeypatch.setattr(proxy, "MAX_CALLS_PER_QUESTION", 3)
    proxy.begin_question("conv-loop0000")
    token = _token("conv-loop0000")
    statuses = [_pi_call(client, token).status_code for _ in range(5)]
    assert statuses == [200, 200, 200, 429, 429]
    # A new question starts a fresh count.
    proxy.begin_question("conv-loop0000")
    assert _pi_call(client, token).status_code == 200


def test_unconfigured_house_is_refused_without_retry(client, upstream, monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY")
    secret_store.clear_cache()
    proxy.begin_question("conv-aaaaaaaa")
    response = _pi_call(client, _token())
    # 403 matches none of pi's retryable patterns (429/5xx/rate limit/...).
    assert response.status_code == 403
    assert upstream.calls == []


def test_upstream_rate_limit_stays_retryable_and_keeps_retry_after(client, upstream):
    """A genuine provider 429 is transient; pi retrying it is correct."""
    upstream.status = 429
    proxy.begin_question("conv-aaaaaaaa")
    response = _pi_call(client, _token())
    assert response.status_code == 429
    assert response.headers.get("retry-after") == "7"
    assert "quota exceeded" not in response.json()["error"]["message"].lower()


# --- bring your own key -------------------------------------------------------------

def test_byok_uses_the_users_key_provider_and_model(client, upstream):
    user = proxy.resolve_byok("ollama-cloud", USER_KEY, "gpt-oss:120b", None)
    proxy.set_byok("conv-byok0000", "browser-byok0000", user)
    proxy.begin_question("conv-byok0000")

    response = _pi_call(client, _token("conv-byok0000", "browser-byok0000"))
    assert response.status_code == 200
    call = upstream.calls[-1]
    assert call["url"] == "https://ollama.com/v1/chat/completions"
    assert call["headers"]["authorization"] == f"Bearer {USER_KEY}"
    assert call["body"]["model"] == "gpt-oss:120b"
    assert "models" not in call["body"], "fallback lists are OpenRouter-only"
    assert HOUSE_KEY not in json.dumps(call)


def test_byok_does_not_spend_the_house_budget(client, upstream, monkeypatch):
    monkeypatch.setenv("INFERENCE_HOUSE_DAILY_REQUEST_BUDGET", "0")
    proxy.set_byok("conv-byok1111", "browser-byok1111", proxy.resolve_byok("groq", USER_KEY, "some-model", None))
    proxy.begin_question("conv-byok1111")
    assert _pi_call(client, _token("conv-byok1111", "browser-byok1111")).status_code == 200


def test_another_browser_cannot_ride_someone_elses_key(client, upstream):
    """A token for a different browser falls back to the house tier, not to the stored user key."""
    proxy.set_byok("conv-shared00", "browser-owner000", proxy.resolve_byok("openai", USER_KEY, "gpt-x", None))
    proxy.begin_question("conv-shared00")
    _pi_call(client, _token("conv-shared00", "browser-intruder"))
    assert upstream.calls[-1]["headers"]["authorization"] == f"Bearer {HOUSE_KEY}"


def test_openai_gets_its_max_tokens_field(client, upstream):
    proxy.set_byok("conv-openai00", "browser-openai00", proxy.resolve_byok("openai", USER_KEY, "gpt-x", None))
    proxy.begin_question("conv-openai00")
    _pi_call(client, _token("conv-openai00", "browser-openai00"))
    body = upstream.calls[-1]["body"]
    assert body["max_completion_tokens"] == 512 and "max_tokens" not in body


def test_a_rejected_user_key_says_so_plainly(client, upstream):
    upstream.status = 401
    proxy.set_byok("conv-badkey00", "browser-badkey00", proxy.resolve_byok("openrouter", USER_KEY, "m", None))
    proxy.begin_question("conv-badkey00")
    response = _pi_call(client, _token("conv-badkey00", "browser-badkey00"))
    assert response.status_code == 401
    assert "rejected your API key" in response.json()["error"]["message"]
    assert USER_KEY not in response.text


def test_custom_urls_pointing_inward_are_refused(env):
    with pytest.raises(Exception):
        proxy.resolve_byok("custom", USER_KEY, "m", "https://169.254.169.254/v1")


def test_byok_key_is_masked_in_reprs():
    upstream = proxy.resolve_byok("groq", USER_KEY, "m", None)
    assert USER_KEY not in repr(upstream)
    settings = agent_routes.ByokSettings(provider="groq", api_key=USER_KEY, model="m")
    assert USER_KEY not in repr(settings)


# --- /agent endpoints: quota and settings ----------------------------------------------

def _browser_headers(client_id="browser-quota000", **extra):
    return {"x-client-id": client_id, **extra}


def test_chat_requires_a_browser_id(client):
    response = client.post("/agent/chat", json={"conversation_id": "conv-00000000", "message": "hi"})
    assert response.status_code == 400


def test_chat_refuses_once_the_daily_quota_is_spent(client, monkeypatch):
    """Checked before pi starts: an exhausted user never spawns a process."""
    spawned = []
    monkeypatch.setattr(agent_routes.runtime, "prompt", lambda *a, **k: spawned.append(a) or iter([{"type": "agent_settled"}]))
    headers = _browser_headers()
    for _ in range(2):
        assert client.post("/agent/chat", json={"conversation_id": "conv-quota000", "message": "hi"}, headers=headers).status_code == 200
    refused = client.post("/agent/chat", json={"conversation_id": "conv-quota000", "message": "hi"}, headers=headers)
    assert refused.status_code == 429
    assert refused.json()["code"] == "quota_exhausted"
    assert refused.json()["quota"]["remaining"] == 0
    assert len(spawned) == 2


def test_the_owner_token_bypasses_the_quota(client, monkeypatch):
    monkeypatch.setenv("AGENT_OWNER_TOKEN", "owner-secret-token")
    secret_store.clear_cache()
    monkeypatch.setattr(agent_routes.runtime, "prompt", lambda *a, **k: iter([{"type": "agent_settled"}]))
    headers = _browser_headers("browser-owner000", **{"x-owner-token": "owner-secret-token"})
    statuses = [client.post("/agent/chat", json={"conversation_id": "conv-owner000", "message": "hi"}, headers=headers).status_code for _ in range(5)]
    assert statuses == [200] * 5
    # A wrong token is just an ordinary browser.
    wrong = _browser_headers("browser-owner111", **{"x-owner-token": "guess"})
    for _ in range(2):
        client.post("/agent/chat", json={"conversation_id": "conv-owner111", "message": "hi"}, headers=wrong)
    assert client.post("/agent/chat", json={"conversation_id": "conv-owner111", "message": "hi"}, headers=wrong).status_code == 429


def test_a_question_that_fails_before_answering_is_refunded(client, monkeypatch):
    monkeypatch.setattr(agent_routes.runtime, "prompt", lambda *a, **k: iter([{"type": "_error", "message": "boom"}]))
    headers = _browser_headers("browser-refund00")
    client.post("/agent/chat", json={"conversation_id": "conv-refund00", "message": "hi"}, headers=headers)
    assert client.get("/agent/quota", headers=headers).json()["quota"]["used"] == 0


def test_byok_chat_skips_the_house_quota(client, monkeypatch):
    monkeypatch.setattr(agent_routes.runtime, "prompt", lambda *a, **k: iter([{"type": "agent_settled"}]))
    headers = _browser_headers("browser-byokq000")
    body = {"conversation_id": "conv-byokq000", "message": "hi", "byok": {"provider": "groq", "api_key": USER_KEY, "model": "m"}}
    assert [client.post("/agent/chat", json=body, headers=headers).status_code for _ in range(4)] == [200] * 4
    assert client.get("/agent/quota", headers=headers).json()["quota"]["used"] == 0


def test_switching_back_to_free_forgets_the_user_key(client, monkeypatch):
    monkeypatch.setattr(agent_routes.runtime, "prompt", lambda *a, **k: iter([{"type": "agent_settled"}]))
    headers = _browser_headers("browser-switch00")
    client.post("/agent/chat", json={"conversation_id": "conv-switch00", "message": "hi",
                                     "byok": {"provider": "groq", "api_key": USER_KEY, "model": "m"}}, headers=headers)
    client.post("/agent/chat", json={"conversation_id": "conv-switch00", "message": "hi"}, headers=headers)
    assert proxy.upstream_for("conv-switch00", "browser-switch00").tier == "house"


def test_model_errors_reach_the_browser_as_a_sentence(client, monkeypatch):
    raw = '429 {"error":{"message":"Quota exceeded: the free assistant has used today\'s capacity.","type":"proxy_error"}}'
    events = [
        {"type": "message_end", "message": {"role": "assistant", "stopReason": "error", "errorMessage": raw}},
        {"type": "agent_settled"},
    ]
    monkeypatch.setattr(agent_routes.runtime, "prompt", lambda *a, **k: iter(events))
    response = client.post("/agent/chat", json={"conversation_id": "conv-errmsg00", "message": "hi"}, headers=_browser_headers("browser-errmsg00"))
    frames = [json.loads(line[5:]) for line in response.text.splitlines() if line.startswith("data:")]
    error = next(f for f in frames if f["type"] == "error")
    assert error["message"] == "The free assistant has used today's capacity."


def test_tool_endpoints_require_the_session_token(client):
    assert client.get("/agent/tools").status_code == 401
    assert client.post("/agent/tools/get_status", json={"arguments": {}}).status_code == 401


def test_model_listing_filters_openrouter_to_tool_capable(client, upstream):
    response = client.post("/agent/byok/models", json={"provider": "openrouter"})
    assert response.json()["models"] == ["tool-model"]


def test_status_reports_configuration_without_secret_values(client):
    text = client.get("/agent/status").text
    assert HOUSE_KEY not in text
    house = json.loads(text)["house"]
    assert house["configured"] is True and house["key_source"] == "env"


@pytest.mark.parametrize("raw, expected", [
    # Full body after a status code.
    ("429 " + json.dumps({"error": {"message": "Quota exceeded: the free assistant has used today's capacity.", "type": "proxy_error"}}),
     "The free assistant has used today's capacity."),
    # The bare error object, exactly as the real UI showed it for a rejected BYOK key.
    ('401: {"message":"OpenRouter rejected your API key (401). Check it in the assistant settings.","type":"proxy_error"}',
     "OpenRouter rejected your API key (401). Check it in the assistant settings."),
    # Plain text passes through.
    ("Connection reset", "Connection reset"),
    ("", "The model call failed."),
])
def test_clean_model_error_unwraps_every_shape_pi_produces(raw, expected):
    assert agent_routes.clean_model_error(raw) == expected
