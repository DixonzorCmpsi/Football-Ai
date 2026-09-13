"""Which model backends the agent can use, and whether a user-supplied URL is safe to call.

Model ids are deliberately not hardcoded. Provider catalogs turn over every few
weeks (OpenRouter's free tool-capable models were mostly weeks old when this was
written), so the UI asks each provider's live /models endpoint instead.

Every provider here speaks OpenAI Chat Completions, which is what lets one proxy
serve both the house tier (our key, free models) and bring-your-own-key. Base
URLs were checked against the live services on 2026-09-12: each chat endpoint
answered 401/400 for a missing or invalid key, not 404.

`custom` covers self-hosted gateways (LiteLLM, vLLM, a llama.cpp server). Because
it takes a URL from the browser, it is the one place a user can point our server
at an arbitrary host, so `check_custom_base_url` refuses anything that isn't
https to a public address. Otherwise anyone could make the backend fetch
`http://169.254.169.254/` (GCP instance metadata, which hands out
service-account tokens) or services on our private network.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from dataclasses import dataclass, field
from urllib.parse import urlparse


@dataclass(frozen=True)
class Provider:
    id: str
    label: str
    base_url: str | None          # None: the user supplies it (custom)
    key_env: str | None           # secret name when this provider serves the house tier
    key_help_url: str
    # Whether GET {base_url}/models answers without a key (true for OpenRouter and
    # Ollama Cloud), so the settings UI can offer real model ids before a key is entered.
    public_model_list: bool = False
    extra_headers: dict = field(default_factory=dict)
    # Newer OpenAI models reject max_tokens and require max_completion_tokens.
    max_tokens_field: str = "max_tokens"
    # OpenRouter accepts a `models` list and falls through it when one is down or rate limited.
    supports_model_fallback: bool = False
    # A local Ollama needs no key.
    requires_key: bool = True
    # Runs on the same machine as this backend; only offered where that makes sense.
    local: bool = False


PROVIDERS: dict[str, Provider] = {
    p.id: p
    for p in (
        Provider(
            id="openrouter",
            label="OpenRouter",
            base_url="https://openrouter.ai/api/v1",
            key_env="OPENROUTER_API_KEY",
            key_help_url="https://openrouter.ai/settings/keys",
            public_model_list=True,
            # Attribution header OpenRouter documents for app rankings. HTTP-Referer
            # is added by the proxy only when PUBLIC_APP_URL is configured.
            extra_headers={"X-Title": "THE SPOT AI"},
            supports_model_fallback=True,
        ),
        Provider(
            id="ollama-cloud",
            label="Ollama Cloud",
            base_url="https://ollama.com/v1",
            key_env="OLLAMA_API_KEY",
            key_help_url="https://ollama.com/settings/keys",
            public_model_list=True,
        ),
        Provider(
            id="ollama-local",
            label="Ollama (this computer)",
            # Operator-configured, never taken from the browser, so pointing it at
            # loopback is not the SSRF hole a user-supplied URL would be.
            base_url=os.getenv("OLLAMA_LOCAL_URL", "http://127.0.0.1:11434/v1").rstrip("/"),
            key_env=None,
            key_help_url="https://ollama.com/download",
            public_model_list=True,
            requires_key=False,
            local=True,
        ),
        Provider(
            id="openai",
            label="OpenAI",
            base_url="https://api.openai.com/v1",
            key_env="OPENAI_API_KEY",
            key_help_url="https://platform.openai.com/api-keys",
            max_tokens_field="max_completion_tokens",
        ),
        Provider(
            id="groq",
            label="Groq",
            base_url="https://api.groq.com/openai/v1",
            key_env="GROQ_API_KEY",
            key_help_url="https://console.groq.com/keys",
        ),
        Provider(
            id="gemini",
            label="Google Gemini",
            base_url="https://generativelanguage.googleapis.com/v1beta/openai",
            key_env="GEMINI_API_KEY",
            key_help_url="https://aistudio.google.com/apikey",
        ),
        Provider(
            id="custom",
            label="Custom (LiteLLM, vLLM, ...)",
            base_url=None,
            key_env=None,
            key_help_url="https://docs.litellm.ai/docs/simple_proxy",
        ),
    )
}


class UnsafeUpstream(ValueError):
    """A user-supplied base URL the server must not call."""


def _allow_private_urls() -> bool:
    # Local development only: lets BYOK point at an Ollama or LiteLLM running on
    # this machine. Never set in a deployed environment.
    return os.getenv("INFERENCE_ALLOW_PRIVATE_BYOK_URLS", "").lower() in ("1", "true", "yes")


def _address_is_public(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    if ip.version == 6 and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return not (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
        or ip.is_multicast or ip.is_unspecified
    )


def check_custom_base_url(url: str, resolve=socket.getaddrinfo) -> str:
    """Return the normalized URL, or raise UnsafeUpstream with a reason the user can act on.

    Called on every request, not only when the settings are saved, so a hostname
    that later re-resolves to a private address is refused at the time of use.
    Redirects are also disabled on the upstream client, so a public host can't
    bounce the request inward.
    """
    parsed = urlparse((url or "").strip())
    if parsed.scheme not in ("https", "http") or not parsed.hostname:
        raise UnsafeUpstream("Base URL must look like https://host/v1")
    if parsed.username or parsed.password:
        raise UnsafeUpstream("Put credentials in the API key field, not in the URL")

    if _allow_private_urls():
        return url.strip().rstrip("/")

    if parsed.scheme != "https":
        raise UnsafeUpstream("Base URL must use https")
    try:
        infos = resolve(parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM)
    except socket.gaierror:
        raise UnsafeUpstream(f"Could not resolve {parsed.hostname}") from None
    addresses = {info[4][0] for info in infos}
    if not addresses or not all(_address_is_public(a.split("%")[0]) for a in addresses):
        raise UnsafeUpstream(f"{parsed.hostname} resolves to a private or reserved address")
    return url.strip().rstrip("/")


def local_providers_enabled() -> bool:
    """Whether "this computer" providers make sense for this server.

    On a laptop, the backend and Ollama share a machine. On Cloud Run there is no
    Ollama next to the container, and offering it would only produce errors.
    INFERENCE_LOCAL_OLLAMA=true/false overrides the guess.
    """
    setting = os.getenv("INFERENCE_LOCAL_OLLAMA", "auto").lower()
    if setting in ("1", "true", "yes"):
        return True
    if setting in ("0", "false", "no"):
        return False
    return not os.getenv("K_SERVICE")  # set by Cloud Run


def provider_available(provider: Provider) -> bool:
    return not provider.local or local_providers_enabled()


def catalog() -> list[dict]:
    """What the settings UI offers. No secrets, and no house configuration."""
    return [
        {
            "id": p.id,
            "label": p.label,
            "base_url": p.base_url,
            "requires_base_url": p.base_url is None,
            "requires_key": p.requires_key,
            "local": p.local,
            "key_help_url": p.key_help_url,
            "public_model_list": p.public_model_list,
        }
        for p in PROVIDERS.values()
        if provider_available(p)
    ]
