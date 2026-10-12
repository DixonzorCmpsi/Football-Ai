#!/usr/bin/env bash
# Switches the site from the bare IP to a domain, with a real Let's Encrypt certificate.
# Point the domain's A record at the VM's address first (provision.sh prints it), then:
#
#   deploy/gcp/set-domain.sh detalk.example you@example.com
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${HERE}/_common.sh"

DOMAIN="${1:?usage: set-domain.sh <domain> <acme-contact-email>}"
EMAIL="${2:?usage: set-domain.sh <domain> <acme-contact-email>}"
IP="$(vm_ip)"

say "Checking ${DOMAIN} resolves to ${IP}"
RESOLVED="$(getent hosts "$DOMAIN" 2>/dev/null | awk '{print $1}' | head -1 || true)"
[[ -z "$RESOLVED" ]] && RESOLVED="$(nslookup "$DOMAIN" 8.8.8.8 2>/dev/null | awk '/^Address: /{a=$2} END{print a}')"
if [[ "$RESOLVED" != "$IP" ]]; then
  echo "${DOMAIN} resolves to '${RESOLVED:-nothing}', not ${IP}. Fix the DNS record first: Let's Encrypt needs it to issue the certificate." >&2
  exit 1
fi

say "Pointing the server at ${DOMAIN}"
vm_script <<REMOTE
set -euo pipefail
f=${REMOTE_ROOT}/.env.prod
sed -i "s|^SITE_ADDRESS=.*|SITE_ADDRESS=${DOMAIN}|; s|^DEFAULT_SNI=.*|DEFAULT_SNI=${DOMAIN}|; s|^TLS_MODE=.*|TLS_MODE=${EMAIL}|; s|^PUBLIC_APP_URL=.*|PUBLIC_APP_URL=https://${DOMAIN}|" \$f
cd ${REMOTE_ROOT}/app && ${COMPOSE} up -d --no-build web
REMOTE
echo "Open https://${DOMAIN} (the certificate is issued on the first request; give it a minute)."
