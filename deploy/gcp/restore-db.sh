#!/usr/bin/env bash
# Copies a local database into the server's, so the site starts with the data you have now
# instead of rebuilding it from scratch. Overwrites the server's database.
#
#   deploy/gcp/restore-db.sh [local-db-container]      (default: football-ai-db-1)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${HERE}/_common.sh"

LOCAL_DB="${1:-football-ai-db-1}"
DUMP="$(mktemp -t football-ai-dump-XXXXXX.sql.gz)"

say "Dumping ${LOCAL_DB}"
docker exec "$LOCAL_DB" pg_dump -U admin -d football_ai --no-owner --clean --if-exists | gzip > "$DUMP"
ls -lh "$DUMP"

say "Uploading"
vm_scp "$DUMP" /tmp/restore.sql.gz
rm -f "$DUMP"

say "Restoring on the server (the API is stopped while this runs)"
vm_ssh "sudo bash -s" <<REMOTE
set -euo pipefail
cd ${REMOTE_ROOT}/app
${COMPOSE} stop backend web
${COMPOSE} up -d --no-build db
until docker exec football-ai-db-1 pg_isready -U admin -d football_ai >/dev/null 2>&1; do sleep 2; done
gunzip -c /tmp/restore.sql.gz | docker exec -i football-ai-db-1 psql -U admin -d football_ai -v ON_ERROR_STOP=1 -q
rm -f /tmp/restore.sql.gz
${COMPOSE} up -d --no-build
REMOTE
echo "Restored. The API takes a minute to load the data; deploy.sh shows when it is healthy."
