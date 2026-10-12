#!/usr/bin/env bash
# Switches the site from the password to "Sign in with Google", letting in ONE email address.
#
#   deploy/gcp/set-google-login.sh            # asks for the email, client ID and client secret
#   deploy/gcp/set-google-login.sh --disable  # back to the password and the bare IP
#
# Run it in your own terminal. It asks for the Google client ID and secret and sends them
# straight to the server over the IAP tunnel (stored in the root-only /opt/football-ai/.env.prod);
# they never pass through chat, the repo, or the build.
#
# Before running it, in the Google Cloud console for the project (APIs & Services):
#   1. OAuth consent screen: User type External, any app name, your email as support contact,
#      add your email as a Test user. Leave it in "Testing": then Google itself only lets your
#      test users sign in, on top of the allow-list here.
#   2. Credentials -> Create credentials -> OAuth client ID -> Web application, with
#      Authorized redirect URI:   https://<host>/oauth2/callback
#      where <host> is printed below, for example 34-24-8-123.sslip.io.
#
# The host is a free sslip.io name that points at the VM's IP; it is a real DNS name, which is
# what Google requires for the redirect, and it lets Caddy get a real HTTPS certificate.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${HERE}/_common.sh"

IP="$(vm_ip)"
HOST="${HOST_NAME:-$(echo "$IP" | tr . -).sslip.io}"

if [[ "${1:-}" == "--disable" ]]; then
  say "Back to the password login on https://${IP}"
  vm_script <<REMOTE
set -euo pipefail
f=${REMOTE_ROOT}/.env.prod
set_key() { grep -v "^\$1=" "\$f" > "\$f.tmp" || true; printf '%s=%s\n' "\$1" "\$2" >> "\$f.tmp"; mv "\$f.tmp" "\$f"; }
set_key CADDYFILE ./Caddyfile
set_key COMPOSE_PROFILES ''
set_key SITE_ADDRESS https://${IP}
set_key DEFAULT_SNI ${IP}
set_key TLS_MODE internal
set_key PUBLIC_APP_URL https://${IP}
cd ${REMOTE_ROOT}/app && ${COMPOSE} --profile oauth stop oauth2-proxy || true
${COMPOSE} up -d --no-build --remove-orphans web
REMOTE
  echo "Done: https://${IP} asks for the password again."
  exit 0
fi

if [[ "${1:-}" == "--rehost" ]]; then
  say "Moving the Google sign-in to https://${HOST} (keeps the saved client ID, secret and email)"
  echo "The OAuth client's Authorized redirect URI must include: https://${HOST}/oauth2/callback"
  vm_script <<REMOTE
set -euo pipefail
f=${REMOTE_ROOT}/.env.prod
grep -q '^GOOGLE_CLIENT_ID=' "\$f" || { echo "No Google login configured yet: run without --rehost." >&2; exit 1; }
set_key() { grep -v "^\$1=" "\$f" > "\$f.tmp" || true; printf '%s=%s
' "\$1" "\$2" >> "\$f.tmp"; mv "\$f.tmp" "\$f"; }
set_key SITE_ADDRESS ${HOST}
set_key DEFAULT_SNI ${HOST}
set_key PUBLIC_APP_URL https://${HOST}
cd ${REMOTE_ROOT}/app && ${COMPOSE} up -d --no-build --remove-orphans --force-recreate web oauth2-proxy
REMOTE
  CHECK_ONLY=1
else
say "Google sign-in for https://${HOST}"
echo "In the Google console, the OAuth client's Authorized redirect URI must be exactly:"
echo "    https://${HOST}/oauth2/callback"
echo

read -r -p "The ONE email address allowed in: " EMAIL
read -r -p "OAuth client ID (ends in .apps.googleusercontent.com): " CLIENT_ID
read -r -s -p "OAuth client secret (hidden): " CLIENT_SECRET
echo

if [[ ! "$EMAIL" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]]; then echo "That does not look like an email address." >&2; exit 1; fi
if [[ "$CLIENT_ID" != *.apps.googleusercontent.com ]]; then echo "The client ID should end in .apps.googleusercontent.com." >&2; exit 1; fi
if [[ -z "$CLIENT_SECRET" ]]; then echo "The client secret is empty." >&2; exit 1; fi
COOKIE_SECRET="$(openssl rand -base64 32 | tr -d '\n' | tr '+/' '-_')"

say "Configuring the server"
vm_script <<REMOTE
set -euo pipefail
f=${REMOTE_ROOT}/.env.prod
set_key() { grep -v "^\$1=" "\$f" > "\$f.tmp" || true; printf '%s=%s\n' "\$1" "\$2" >> "\$f.tmp"; mv "\$f.tmp" "\$f"; }
set_key GOOGLE_CLIENT_ID '${CLIENT_ID}'
set_key GOOGLE_CLIENT_SECRET '${CLIENT_SECRET}'
set_key OAUTH_COOKIE_SECRET '${COOKIE_SECRET}'
set_key ALLOWED_EMAIL '${EMAIL}'
set_key CADDYFILE ./Caddyfile.oauth
set_key COMPOSE_PROFILES oauth
set_key SITE_ADDRESS ${HOST}
set_key DEFAULT_SNI ${HOST}
set_key PUBLIC_APP_URL https://${HOST}
cd ${REMOTE_ROOT}/app && ${COMPOSE} up -d --no-build --remove-orphans web oauth2-proxy
REMOTE

fi

say "Checking: an unauthenticated visit must be sent to Google"
for _ in $(seq 1 12); do
  code="$(curl -sL -m 30 -o /dev/null -w '%{http_code} %{url_effective}' "https://${HOST}/" || true)"
  echo "  ${code}"
  [[ "$code" == *accounts.google.com* ]] && break
  sleep 10
done
echo
echo "Open https://${HOST} and sign in as the allowed email."
