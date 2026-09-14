"""web_search and fetch_page: the conversation's key, the budget, and the SSRF guards."""

from __future__ import annotations

import json
import socket

import httpx
import pytest

from agent import tools as agent_tools
from agent import web_tools as wt
from applications.api.services import llm_proxy as proxy
from applications.api.services.inference_providers import PROVIDERS


def _resolver(table: dict[str, str]):
    def resolve(host, port, type=socket.SOCK_STREAM):  # noqa: A002 - mirrors getaddrinfo
        if host not in table:
            raise socket.gaierror("unknown host")
        return [(socket.AF_INET, type, 6, "", (table[host], port))]
    return resolve


PUBLIC = _resolver({"news.example.com": "93.184.216.34", "evil.example.com": "93.184.216.35", "inside.example.com": "10.0.0.5"})


def _upstream(provider="openrouter", tier="byok", key="sk-user"):
    p = PROVIDERS[provider]
    return proxy.Upstream(tier, p, p.base_url, key, ["anthropic/claude-sonnet-5"])


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    wt._used.clear()
    yield
    monkeypatch.setattr(wt, "_transport", None)


class TestSearch:
    def test_runs_on_the_conversations_openrouter_key_and_lists_citations(self, monkeypatch):
        seen = {}

        def handler(request: httpx.Request):
            seen["url"] = str(request.url)
            seen["auth"] = request.headers["authorization"]
            seen["body"] = json.loads(request.content)
            return httpx.Response(200, json={"choices": [{"message": {
                "content": "Mitchell was limited Wednesday.",
                "annotations": [
                    {"type": "url_citation", "url_citation": {"url": "https://news.example.com/a", "title": "Jets injury report", "content": "Adonai Mitchell (hamstring) limited"}},
                    {"type": "url_citation", "url_citation": {"url": "https://news.example.com/a", "title": "duplicate"}},
                ],
            }}]})

        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(handler))
        req = wt.web_search("Adonai Mitchell injury", max_results=3)
        out = wt.run(req, "conv-search01", _upstream())

        assert seen["url"] == "https://openrouter.ai/api/v1/chat/completions"
        assert seen["auth"] == "Bearer sk-user"
        assert seen["body"]["plugins"] == [{"id": "web", "max_results": 3}]
        assert "Jets injury report" in out and "https://news.example.com/a" in out
        assert out.count("https://news.example.com/a") == 1, "duplicate citations collapse"
        assert wt.UNTRUSTED_START in out and "limited Wednesday" in out

    def test_other_providers_say_unavailable_without_calling_anything(self, monkeypatch):
        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(lambda r: pytest.fail("must not call out")))
        out = wt.run(wt.web_search("news"), "conv-search02", _upstream("groq"))
        assert "isn't available" in out and "Groq" in out

    def test_house_tier_gets_a_smaller_budget_per_question(self, monkeypatch):
        calls = []
        monkeypatch.setattr(wt, "search", lambda q, n, u: calls.append(q) or "ok")
        monkeypatch.setitem(wt.SEARCHES_PER_QUESTION, "house", 2)
        house = _upstream(tier="house")
        results = [wt.run(wt.web_search(f"q{i}"), "conv-search03", house) for i in range(3)]
        assert results[:2] == ["ok", "ok"] and "limit reached" in results[2]
        wt.begin_question("conv-search03")
        assert wt.run(wt.web_search("again"), "conv-search03", house) == "ok"

    def test_out_of_credits_is_explained(self, monkeypatch):
        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(
            lambda r: httpx.Response(402, json={"error": {"message": "Insufficient credits"}})))
        out = wt.run(wt.web_search("news"), "conv-search04", _upstream())
        assert "402" in out and "credits" in out


class TestFetch:
    def test_reads_the_article_not_the_chrome(self, monkeypatch):
        html = """<html><head><title>Jets notes</title><script>var x=1</script></head>
        <body><nav>Home Teams Scores</nav><article><h1>Mitchell limited</h1>
        <p>Adonai Mitchell was limited with a hamstring issue.</p></article><footer>(c) site</footer></body></html>"""
        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(
            lambda r: httpx.Response(200, text=html, headers={"content-type": "text/html; charset=utf-8"})))
        out = wt.fetch("https://news.example.com/jets", resolve=PUBLIC)
        assert "Title: Jets notes" in out and "## Mitchell limited" in out and "hamstring" in out
        assert "var x" not in out and "Home Teams" not in out and "(c) site" not in out
        assert out.index(wt.UNTRUSTED_START) < out.index("hamstring") < out.index(wt.UNTRUSTED_END)

    @pytest.mark.parametrize("url, reason", [
        ("http://169.254.169.254/computeMetadata/v1/", "private or reserved"),
        ("http://127.0.0.1/agent/tools", "private or reserved"),
        ("http://127.0.0.1:8000/agent/tools", "Port 8000"),
        ("https://inside.example.com/", "private or reserved"),
        ("file:///etc/passwd", "http(s)"),
        ("https://user:pw@news.example.com/", "credentials"),
        ("https://news.example.com:5432/", "Port 5432"),
    ])
    def test_refuses_addresses_inside_our_network(self, monkeypatch, url, reason):
        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(lambda r: pytest.fail(f"requested {r.url}")))
        resolve = _resolver({"169.254.169.254": "169.254.169.254", "127.0.0.1": "127.0.0.1", "news.example.com": "93.184.216.34", "inside.example.com": "10.0.0.5"})
        out = wt.fetch(url, resolve=resolve)
        assert out.startswith("Can't read that address") and reason in out

    def test_a_redirect_into_the_network_is_refused(self, monkeypatch):
        requested = []

        def handler(request):
            requested.append(str(request.url))
            return httpx.Response(302, headers={"location": "https://inside.example.com/admin"})

        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(handler))
        out = wt.fetch("https://evil.example.com/", resolve=PUBLIC)
        assert requested == ["https://evil.example.com/"]
        assert "private or reserved" in out

    def test_binary_and_errors_are_reported(self, monkeypatch):
        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(
            lambda r: httpx.Response(200, content=b"%PDF", headers={"content-type": "application/pdf"})))
        assert "not a text page" in wt.fetch("https://news.example.com/x.pdf", resolve=PUBLIC)
        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(lambda r: httpx.Response(404)))
        assert "answered 404" in wt.fetch("https://news.example.com/gone", resolve=PUBLIC)

    def test_long_pages_are_cut(self, monkeypatch):
        monkeypatch.setattr(wt, "_transport", httpx.MockTransport(
            lambda r: httpx.Response(200, text="x" * 50_000, headers={"content-type": "text/plain"})))
        out = wt.fetch("https://news.example.com/long", resolve=PUBLIC)
        assert f"[cut at {wt.MAX_PAGE_CHARS} characters]" in out and len(out) < wt.MAX_PAGE_CHARS + 500


class TestOverHttp:
    def test_listed_for_the_agent(self):
        names = [spec["name"] for spec in agent_tools.list_tools()]
        assert names[-2:] == ["web_search", "fetch_page"]

    def test_search_uses_the_tokens_conversation_key(self, client, monkeypatch):
        from applications.api.services import agent_tokens

        cid, client_id = "conv-websrch1", "browser-websrch1"
        proxy.set_byok(cid, client_id, _upstream(key="sk-this-user"))
        seen = {}

        def fake_search(query, n, upstream):
            seen["key"] = upstream.api_key
            return f"results for {query}"

        monkeypatch.setattr(wt, "search", fake_search)
        response = client.post(
            "/agent/tools/web_search",
            json={"arguments": {"query": "jets injuries"}},
            headers={"authorization": f"Bearer {agent_tokens.mint(cid, client_id)}", "x-client-id": "t" * 8},
        )
        proxy.clear_byok(cid)
        assert response.status_code == 200, response.text
        assert response.json()["text"] == "results for jets injuries"
        assert seen["key"] == "sk-this-user"

    def test_fetch_needs_the_session_token(self, client):
        response = client.post("/agent/tools/fetch_page", json={"arguments": {"url": "https://example.com"}})
        assert response.status_code == 401
