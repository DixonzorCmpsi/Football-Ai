#!/usr/bin/env bash
# Runs on the VM at every boot (as root), from instance metadata. Installs Docker the first
# time and keeps the nightly database backup scheduled. Idempotent.
set -euo pipefail
exec > >(tee -a /var/log/football-ai-startup.log) 2>&1
echo "startup $(date -u +%FT%TZ)"

if ! command -v docker >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
fi

# 1 GB of RAM is enough for the app at rest (about 0.5 GB) but not for its busiest moments
# (the nightly data refresh); a 2 GB swap file turns an out-of-memory kill into a slow minute.
if ! swapon --show | grep -q '/swapfile'; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# Lets `docker pull` fetch from Artifact Registry using the VM's service account.
REGISTRY_HOST="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/football-ai-registry-host || true)"
if [ -n "$REGISTRY_HOST" ]; then
  HOME=/root gcloud auth configure-docker "$REGISTRY_HOST" --quiet
fi

mkdir -p /opt/football-ai/app /opt/football-ai/backups
chmod 750 /opt/football-ai

# Nightly 03:30 UTC: dump the database into the backup bucket (kept 30 days by a bucket rule).
BUCKET="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/football-ai-bucket || true)"
cat > /opt/football-ai/backup.sh <<EOF
#!/usr/bin/env bash
set -euo pipefail
stamp="\$(date -u +%Y%m%dT%H%M%SZ)"
file="/opt/football-ai/backups/football_ai_\${stamp}.sql.gz"
docker exec football-ai-db-1 pg_dump -U admin -d football_ai --no-owner --clean --if-exists | gzip > "\$file"
gcloud storage cp "\$file" "gs://${BUCKET}/db/" --quiet
# Keep only the last 3 dumps on the disk itself.
ls -1t /opt/football-ai/backups/*.sql.gz | tail -n +4 | xargs -r rm --
EOF
chmod 750 /opt/football-ai/backup.sh
echo "30 3 * * * root /opt/football-ai/backup.sh >> /var/log/football-ai-backup.log 2>&1" > /etc/cron.d/football-ai-backup
echo "startup done"
