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
