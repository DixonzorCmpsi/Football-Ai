#!/usr/bin/env bash
# Builds the committed code on Google Cloud Build, then has the VM pull and run it. Run from
# anywhere in the repo.
#
#   deploy/gcp/deploy.sh
#
# Deploys what is committed on the current branch (git archive), not your working tree, so
# what runs on the server is always something you can name. The VM only pulls images: it is
# too small to build them. The first run also creates the server's .env.prod: a random
# database password and the login, whose password is written once to ~/.football-ai-login on
# this machine (the server keeps only its hash).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${HERE}/_common.sh"
cd "$(git -C "$HERE" rev-parse --show-toplevel)"

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "Uncommitted changes to tracked files: commit them first, so the server runs a known version." >&2
  exit 1
fi
REV="$(git rev-parse --short HEAD)"
IP="$(vm_ip)"

say "Building ${REV} on Cloud Build"
SRC="$(mktemp -d -t football-ai-src-XXXXXX)"
git archive HEAD | tar -x -C "$SRC"
( cd "$SRC" && gcloud builds submit . --project "$PROJECT" --config deploy/cloudbuild.yaml \
    --substitutions "_REGISTRY=${REGISTRY},_TAG=${REV}" )
rm -rf "$SRC"

say "Uploading the stack definition to ${VM} (${IP})"
ARCHIVE="$(mktemp -t football-ai-XXXXXX.tar.gz)"
git archive --format=tar.gz -o "$ARCHIVE" HEAD deploy
vm_scp "$ARCHIVE" /tmp/football-ai.tar.gz
rm -f "$ARCHIVE"

say "Unpacking and preparing .env.prod"
LOGIN_FILE="${HOME}/.football-ai-login"
# `sudo bash -s` reads this script from stdin. It prints the new login only on the first run.
OUT="$(vm_ssh "sudo bash -s" <<REMOTE
set -euo pipefail
rm -rf ${REMOTE_ROOT}/app.new && mkdir -p ${REMOTE_ROOT}/app.new
tar -xzf /tmp/football-ai.tar.gz -C ${REMOTE_ROOT}/app.new
rm -f /tmp/football-ai.tar.gz
rm -rf ${REMOTE_ROOT}/app.old && { [ -d ${REMOTE_ROOT}/app ] && mv ${REMOTE_ROOT}/app ${REMOTE_ROOT}/app.old || true; }
mv ${REMOTE_ROOT}/app.new ${REMOTE_ROOT}/app
rm -rf ${REMOTE_ROOT}/app.old
echo "${REV}" > ${REMOTE_ROOT}/app/REVISION
if [ ! -f ${REMOTE_ROOT}/.env.prod ]; then
  pg="\$(openssl rand -hex 24)"
  pw="\$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20)"
  hash="\$(docker run --rm caddy:2-alpine caddy hash-password --plaintext "\$pw")"
  umask 077
  cat > ${REMOTE_ROOT}/.env.prod <<ENV
POSTGRES_PASSWORD=\$pg
GCP_PROJECT_ID=${PROJECT}
REGISTRY=${REGISTRY}
TAG=latest
SITE_ADDRESS=https://${IP}
TLS_MODE=internal
PUBLIC_APP_URL=https://${IP}
BASIC_AUTH_USER=admin
BASIC_AUTH_HASH='\$hash'
RUN_ETL_ON_STARTUP=false
ENV
  echo "NEW_LOGIN user=admin password=\$pw"
fi
REMOTE
)"
if grep -q '^NEW_LOGIN' <<<"$OUT"; then
  umask 077
  grep '^NEW_LOGIN' <<<"$OUT" | sed 's/^NEW_LOGIN //' > "$LOGIN_FILE"
  echo "First deploy: the login was saved to ${LOGIN_FILE}"
fi

say "Pulling the images and starting the stack"
vm_ssh "cd ${REMOTE_ROOT}/app && sudo env REGISTRY=${REGISTRY} TAG=${REV} ${COMPOSE} pull && sudo env REGISTRY=${REGISTRY} TAG=${REV} ${COMPOSE} up -d --no-build"

say "Waiting for the API to report ready"
for _ in $(seq 1 60); do
  state="$(vm_ssh "sudo docker inspect -f '{{.State.Health.Status}}' football-ai-backend-1 2>/dev/null || echo starting" | tr -d '\r')"
  echo "backend: ${state}"
  [[ "$state" == "healthy" ]] && break
  sleep 15
done

say "Deployed ${REV}"
echo "Open https://${IP}   (self-signed certificate: the browser warns once; the login is in ${LOGIN_FILE})"
