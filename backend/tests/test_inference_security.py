"""Secrets, session tokens and user-supplied URLs: the parts of inference that must not leak or be abused."""

import socket
import time

import pytest

from applications.api import secret_store
from applications.api.services import agent_tokens
from applications.api.services.inference_providers import UnsafeUpstream, check_custom_base_url


@pytest.fixture(autouse=True)
def _clean_secrets(monkeypatch):
    for name in ("DEMO_KEY", "DEMO_KEY_FILE", "GCP_PROJECT_ID", "GOOGLE_CLOUD_PROJECT", "AGENT_PROXY_SIGNING_KEY"):
        monkeypatch.delenv(name, raising=False)
    secret_store.clear_cache()
    yield
    secret_store.clear_cache()


# --- secret store ----------------------------------------------------------------

def test_env_var_wins(monkeypatch):
    monkeypatch.setenv("DEMO_KEY", " sk-from-env \n")
    assert secret_store.get_secret("DEMO_KEY") == "sk-from-env"
    assert secret_store.secret_source("DEMO_KEY") == "env"


def test_mounted_file_is_read(monkeypatch, tmp_path):
    """Cloud Run secret volumes, Docker and Kubernetes secrets all arrive as files."""
    path = tmp_path / "demo"
    path.write_text("sk-from-file\n", encoding="utf-8")
    monkeypatch.setenv("DEMO_KEY_FILE", str(path))
    assert secret_store.get_secret("DEMO_KEY") == "sk-from-file"
    assert secret_store.secret_source("DEMO_KEY") == "file"


def test_secret_manager_is_used_when_gcp_is_configured(monkeypatch):
    seen = {}

    class FakePayload:
        data = b"sk-from-secret-manager"

    class FakeClient:
        def access_secret_version(self, name):
            seen["name"] = name
            return type("Resp", (), {"payload": FakePayload()})()

    import sys
    import types

    google = types.ModuleType("google")
    cloud = types.ModuleType("google.cloud")
    secretmanager = types.ModuleType("google.cloud.secretmanager")
    secretmanager.SecretManagerServiceClient = FakeClient
    cloud.secretmanager = secretmanager
    google.cloud = cloud
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", secretmanager)
    monkeypatch.setattr(secret_store, "_sm_client", None)

    monkeypatch.setenv("GCP_PROJECT_ID", "spot-prod")
    monkeypatch.setenv("SECRET_MANAGER_PREFIX", "football-ai-")
    assert secret_store.get_secret("DEMO_KEY") == "sk-from-secret-manager"
    # Env-var names map to kebab-case secret ids.
    assert seen["name"] == "projects/spot-prod/secrets/football-ai-demo-key/versions/latest"


def test_missing_secret_is_none_and_never_raises():
    assert secret_store.get_secret("DEMO_KEY") is None
    assert secret_store.secret_source("DEMO_KEY") == "missing"


def test_secret_values_are_not_logged(monkeypatch, caplog, tmp_path):
    monkeypatch.setenv("DEMO_KEY_FILE", str(tmp_path / "does-not-exist"))
    with caplog.at_level("DEBUG"):
        secret_store.get_secret("DEMO_KEY")
    monkeypatch.setenv("DEMO_KEY", "sk-super-secret-value")
    secret_store.clear_cache()
    with caplog.at_level("DEBUG"):
        secret_store.get_secret("DEMO_KEY")
    assert "sk-super-secret-value" not in caplog.text


# --- session tokens ---------------------------------------------------------------

def test_token_round_trip(monkeypatch):
    monkeypatch.setenv("AGENT_PROXY_SIGNING_KEY", "k" * 40)
    token = agent_tokens.mint("conv-12345678", "client-abcdefgh", owner=True)
    grant = agent_tokens.verify(token)
    assert grant.conversation_id == "conv-12345678"
    assert grant.client_id == "client-abcdefgh"
    assert grant.owner is True


@pytest.mark.parametrize("mutate", [
    lambda t: t[:-2] + ("AA" if not t.endswith("AA") else "BB"),   # forged signature
    lambda t: t.replace("spt_", "spt_x"),                          # altered body
    lambda t: "sk-" + t,                                            # wrong prefix
    lambda t: "",
])
def test_tampered_tokens_are_refused(monkeypatch, mutate):
    monkeypatch.setenv("AGENT_PROXY_SIGNING_KEY", "k" * 40)
    assert agent_tokens.verify(mutate(agent_tokens.mint("conv-12345678", "client-abcdefgh"))) is None


def test_a_token_cannot_claim_owner(monkeypatch):
    """The owner flag is inside the signature; flipping it breaks the token."""
    import base64
    import json

    monkeypatch.setenv("AGENT_PROXY_SIGNING_KEY", "k" * 40)
    token = agent_tokens.mint("conv-12345678", "client-abcdefgh", owner=False)
    body, sig = token[len("spt_"):].split(".")
    payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    payload["o"] = True
    forged = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).rstrip(b"=").decode()
    assert agent_tokens.verify(f"spt_{forged}.{sig}") is None


def test_expired_tokens_are_refused(monkeypatch):
    monkeypatch.setenv("AGENT_PROXY_SIGNING_KEY", "k" * 40)
    token = agent_tokens.mint("conv-12345678", "client-abcdefgh", ttl=-1)
    assert agent_tokens.verify(token) is None


def test_tokens_from_another_key_are_refused(monkeypatch):
    """Instances sharing AGENT_PROXY_SIGNING_KEY accept each other's tokens; others don't."""
    monkeypatch.setenv("AGENT_PROXY_SIGNING_KEY", "a" * 40)
    token = agent_tokens.mint("conv-12345678", "client-abcdefgh")
    secret_store.clear_cache()
    monkeypatch.setenv("AGENT_PROXY_SIGNING_KEY", "b" * 40)
    assert agent_tokens.verify(token) is None


# --- custom BYOK base URLs (SSRF) -------------------------------------------------

def _resolves_to(*addresses):
    def fake(host, port, type=None):
        return [(socket.AF_INET6 if ":" in a else socket.AF_INET, socket.SOCK_STREAM, 6, "", (a, port)) for a in addresses]
    return fake


@pytest.fixture
def no_private_override(monkeypatch):
    monkeypatch.delenv("INFERENCE_ALLOW_PRIVATE_BYOK_URLS", raising=False)


def test_public_https_gateway_is_accepted(no_private_override):
    url = check_custom_base_url("https://litellm.example.com/v1/", resolve=_resolves_to("93.184.216.34"))
    assert url == "https://litellm.example.com/v1"


@pytest.mark.parametrize("address", [
    "169.254.169.254",   # GCP/AWS instance metadata: hands out service-account tokens
    "127.0.0.1",
    "10.8.0.4",          # VPC-private, e.g. Cloud SQL private IP
    "192.168.1.20",
    "::1",
    "fd00::1",
    "::ffff:10.0.0.1",   # IPv4-mapped private
])
def test_private_and_metadata_addresses_are_refused(no_private_override, address):
    with pytest.raises(UnsafeUpstream):
        check_custom_base_url("https://innocent.example.com/v1", resolve=_resolves_to(address))


def test_one_private_record_among_public_ones_is_refused(no_private_override):
    """DNS answering with both is a classic way to slip an internal address past a check."""
    with pytest.raises(UnsafeUpstream):
        check_custom_base_url("https://mixed.example.com/v1", resolve=_resolves_to("93.184.216.34", "10.0.0.5"))


@pytest.mark.parametrize("url", [
    "http://litellm.example.com/v1",       # plaintext would expose the user's key
    "ftp://example.com/v1",
    "https://user:pass@example.com/v1",    # credentials belong in the key field
    "not a url",
])
def test_malformed_or_insecure_urls_are_refused(no_private_override, url):
    with pytest.raises(UnsafeUpstream):
        check_custom_base_url(url, resolve=_resolves_to("93.184.216.34"))


def test_local_gateways_allowed_only_with_explicit_dev_flag(monkeypatch):
    monkeypatch.setenv("INFERENCE_ALLOW_PRIVATE_BYOK_URLS", "true")
    assert check_custom_base_url("http://127.0.0.1:4000/v1") == "http://127.0.0.1:4000/v1"
