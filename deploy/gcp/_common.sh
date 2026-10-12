# Shared by the deploy scripts. Source it; don't run it.
PROJECT="${PROJECT:-football-ai-478621}"
ZONE="${ZONE:-us-east1-b}"
REGION="${REGION:-us-east1}"
VM="${VM:-football-ai}"
IP_NAME="${IP_NAME:-football-ai-ip}"
BUCKET="${BUCKET:-${PROJECT}-backups}"
REPO="${REPO:-football-ai}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}"
REMOTE_ROOT=/opt/football-ai
COMPOSE="docker compose -f deploy/docker-compose.prod.yml --env-file ${REMOTE_ROOT}/.env.prod"

say() { printf '\n== %s\n' "$*"; }

vm_ip() { gcloud compute addresses describe "$IP_NAME" --region "$REGION" --project "$PROJECT" --format='value(address)'; }

# Run a command on the VM over Google's IAP tunnel (the VM has no open SSH port).
vm_ssh() { gcloud compute ssh "$VM" --zone "$ZONE" --project "$PROJECT" --tunnel-through-iap --quiet --command "$1"; }
vm_scp() { gcloud compute scp "$1" "${VM}:$2" --zone "$ZONE" --project "$PROJECT" --tunnel-through-iap --quiet; }

# Run a script on the VM as root. The script comes in on stdin (usually a here-doc). It is
# uploaded and run as a file rather than piped into `ssh`, because `gcloud compute ssh` on
# Windows does not forward piped input reliably.
vm_script() {
  local f
  f="$(mktemp -t football-ai-remote-XXXXXX)"
  cat > "$f"
  vm_scp "$f" /tmp/football-ai-remote.sh
  rm -f "$f"
  vm_ssh 'sudo bash /tmp/football-ai-remote.sh; rc=$?; sudo rm -f /tmp/football-ai-remote.sh; exit $rc'
}
