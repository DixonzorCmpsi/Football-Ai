"""Where the backend gets secrets, locally and on GCP, with no code change between them.

Resolution order for `get_secret("OPENROUTER_API_KEY")`:

1. **Environment variable** `OPENROUTER_API_KEY`.
   Locally this comes from `backend/.env` (loaded by config.py, git-ignored).
   On Cloud Run, map it straight from Secret Manager, which needs no code:
       gcloud run deploy ... --set-secrets OPENROUTER_API_KEY=openrouter-api-key:latest
2. **A mounted file** named by `OPENROUTER_API_KEY_FILE`.
   Cloud Run secret volumes, Kubernetes and Docker secrets all mount files.
3. **Secret Manager, fetched directly**, when `GCP_PROJECT_ID` (or
   `GOOGLE_CLOUD_PROJECT`) is set. The secret id is the name in kebab case
   (`openrouter-api-key`), optionally prefixed by `SECRET_MANAGER_PREFIX`.
   Authentication is the runtime's service account (Application Default
   Credentials); no key file ships with the app.

Values are cached for `SECRET_CACHE_SECONDS` (default 300), so a rotated secret
is picked up without a restart. Values are never logged.
"""

from __future__ import annotations

import os
import threading
import time

from .config import logger

_CACHE_SECONDS = float(os.getenv("SECRET_CACHE_SECONDS", "300"))
_cache: dict[str, tuple[float, str | None]] = {}
_lock = threading.Lock()
_sm_client = None


def _secret_id(name: str) -> str:
    return os.getenv("SECRET_MANAGER_PREFIX", "") + name.lower().replace("_", "-")


def _from_secret_manager(name: str) -> str | None:
    global _sm_client
    project = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        return None
    try:
        # Imported only when GCP is configured, so local runs need neither the
        # library nor credentials.
        from google.cloud import secretmanager
    except ImportError:
        logger.error("GCP_PROJECT_ID is set but google-cloud-secret-manager is not installed")
        return None
    try:
        if _sm_client is None:
            _sm_client = secretmanager.SecretManagerServiceClient()
        path = f"projects/{project}/secrets/{_secret_id(name)}/versions/latest"
        payload = _sm_client.access_secret_version(name=path).payload.data
        return payload.decode("utf-8").strip() or None
    except Exception as exc:
        # The message names the secret, never its value.
        logger.warning("Secret Manager lookup failed for %s: %s", _secret_id(name), type(exc).__name__)
        return None


def _resolve(name: str) -> str | None:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()

    path = os.getenv(f"{name}_FILE")
    if path:
        try:
            with open(path, encoding="utf-8") as handle:
                value = handle.read().strip()
            if value:
                return value
        except OSError as exc:
            logger.warning("Secret file for %s unreadable: %s", name, type(exc).__name__)

    return _from_secret_manager(name)


def get_secret(name: str) -> str | None:
    """The secret's value, or None if it is not configured anywhere."""
    now = time.monotonic()
    with _lock:
        cached = _cache.get(name)
        if cached and now - cached[0] < _CACHE_SECONDS:
            return cached[1]
    value = _resolve(name)
    with _lock:
        _cache[name] = (now, value)
    return value


def secret_source(name: str) -> str:
    """Where a secret would come from: "env", "file", "secret-manager" or "missing".

    For /health and logs. Tells an operator which path is live without exposing the value.
    """
    if (os.getenv(name) or "").strip():
        return "env"
    if os.getenv(f"{name}_FILE"):
        return "file"
    if (os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")) and get_secret(name):
        return "secret-manager"
    return "missing"


def clear_cache() -> None:
    with _lock:
        _cache.clear()
