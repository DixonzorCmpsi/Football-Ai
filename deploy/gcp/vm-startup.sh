#!/usr/bin/env bash
# Runs on the VM at every boot (as root), from instance metadata. Installs Docker the first
# time, keeps the nightly database backup scheduled and the self-healing watchdog installed. Idempotent.
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
# --- Self-healing -----------------------------------------------------------------------------
# The 1 GB VM has hung twice (Oct 13 and Oct 16-19): the network stack dies (the guest agent loses
# the metadata server, systemd-networkd cannot renew its address) and nothing brings it back, so
# the site stayed down for days until someone reset the VM. This watchdog notices within minutes and
# reboots. Two probes, every minute: Google's metadata server (the first thing to vanish) and the
# local web container. Eight failed minutes in a row reboot the VM, at most once every 30 minutes
# (no reboot loops).
cat > /usr/local/bin/football-watchdog.sh <<'EOS'
#!/usr/bin/env bash
FAILS=/run/football-watchdog.fails
LAST=/var/lib/football-watchdog.last
ok=1
curl -s -m 5 -o /dev/null -H 'Metadata-Flavor: Google' http://169.254.169.254/computeMetadata/v1/instance/id || ok=0
host="$(grep -m1 '^SITE_ADDRESS=' /opt/football-ai/.env.prod 2>/dev/null | cut -d= -f2- | sed 's#^https\?://##')"
if [ -n "$host" ]; then
  code="$(curl -sk -m 10 -o /dev/null -w '%{http_code}' --resolve "${host}:443:127.0.0.1" "https://${host}/" || true)"
  [ "$code" = "000" ] && ok=0
fi
if [ "$ok" = 1 ]; then rm -f "$FAILS"; exit 0; fi
n=$(( $(cat "$FAILS" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$FAILS"
logger -t football-watchdog "health probe failed ($n/8)"
if [ "$n" -ge 8 ]; then
  if [ -f "$LAST" ] && [ -z "$(find "$LAST" -mmin +30)" ]; then
    logger -t football-watchdog "would reboot, but one happened in the last 30 minutes"; exit 0
  fi
  touch "$LAST"; logger -t football-watchdog "rebooting"; systemctl reboot --force
fi
EOS
chmod 755 /usr/local/bin/football-watchdog.sh
cat > /etc/systemd/system/football-watchdog.service <<'EOS'
[Unit]
Description=Reboot the VM when it stops answering
[Service]
Type=oneshot
ExecStart=/usr/local/bin/football-watchdog.sh
EOS
cat > /etc/systemd/system/football-watchdog.timer <<'EOS'
[Unit]
Description=Run the watchdog every minute
[Timer]
OnBootSec=10min
OnUnitActiveSec=1min
[Install]
WantedBy=timers.target
EOS
systemctl daemon-reload
systemctl enable --now football-watchdog.timer

# Keep hot pages in RAM longer, and stop a crash loop from filling the journal (it reached 300 MB).
echo 'vm.swappiness=20' > /etc/sysctl.d/90-football-ai.conf
sysctl -q -p /etc/sysctl.d/90-football-ai.conf
mkdir -p /etc/systemd/journald.conf.d
printf '[Journal]\nSystemMaxUse=60M\n' > /etc/systemd/journald.conf.d/football-ai.conf
systemctl restart systemd-journald
journalctl --vacuum-size=60M >/dev/null 2>&1 || true

echo "startup done"
