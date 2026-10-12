#!/usr/bin/env bash
# Creates the Google Cloud pieces Football-Ai runs on. Safe to re-run: every step checks
# whether the thing already exists. It changes billing and creates resources (one VM, one
# disk, one static IP, one bucket, one image repository), so read it before running it.
#
# The defaults are chosen to sit inside Google's Always Free tier: ONE e2-micro per billing
# account in us-west1, us-central1 or us-east1, with up to 30 GB of standard disk. What is
# not free is the VM's public IPv4 address (about $3.65/month) and a few cents of storage,
# so the running cost is about $4/month. Images are built on Cloud Build and pulled by the
# VM, because 1 GB of memory cannot build them.
#
#   deploy/gcp/provision.sh
#
# Override anything with an environment variable. If the app outgrows the micro:
#   MACHINE_TYPE=e2-small  (2 GB, about $13/month)   or   e2-standard-2  (8 GB, about $50/month)
set -euo pipefail

PROJECT="${PROJECT:-football-ai-478621}"
BILLING_ACCOUNT="${BILLING_ACCOUNT:-017321-C7A48D-1D33CF}"   # "My Billing Account 1"
REGION="${REGION:-us-east1}"
ZONE="${ZONE:-us-east1-b}"
VM="${VM:-football-ai}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-micro}"                      # 2 shared vCPU, 1 GB: Always Free
DISK_GB="${DISK_GB:-30}"                                     # the free allowance
DISK_TYPE="${DISK_TYPE:-pd-standard}"                        # standard disk is the free kind
REPO="${REPO:-football-ai}"
REGISTRY_HOST="${REGION}-docker.pkg.dev"
SA_NAME="${SA_NAME:-football-ai-vm}"
SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
IP_NAME="${IP_NAME:-football-ai-ip}"
BUCKET="${BUCKET:-${PROJECT}-backups}"
TAG="football-ai"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

say() { printf '\n== %s\n' "$*"; }

say "Billing: linking ${PROJECT} to ${BILLING_ACCOUNT}"
if [[ "$(gcloud billing projects describe "$PROJECT" --format='value(billingEnabled)')" != "True" ]]; then
  gcloud billing projects link "$PROJECT" --billing-account="$BILLING_ACCOUNT"
else
  echo "already billed"
fi

say "Enabling APIs"
gcloud services enable compute.googleapis.com secretmanager.googleapis.com \
  iap.googleapis.com storage.googleapis.com logging.googleapis.com monitoring.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com \
  --project "$PROJECT"

say "Service account ${SA} (the VM's identity; it gets only what it needs)"
if ! gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT" --display-name "Football-Ai VM"
fi
for role in roles/secretmanager.secretAccessor roles/logging.logWriter roles/monitoring.metricWriter; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:${SA}" --role "$role" \
    --condition=None --quiet >/dev/null
done

say "Image repository ${REGISTRY_HOST}/${PROJECT}/${REPO}"
if ! gcloud artifacts repositories describe "$REPO" --location "$REGION" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud artifacts repositories create "$REPO" --repository-format docker --location "$REGION" \
    --project "$PROJECT" --description "Football-Ai production images"
fi
gcloud artifacts repositories add-iam-policy-binding "$REPO" --location "$REGION" --project "$PROJECT" \
  --member "serviceAccount:${SA}" --role roles/artifactregistry.reader >/dev/null

say "Backup bucket gs://${BUCKET}"
if ! gcloud storage buckets describe "gs://${BUCKET}" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" --project "$PROJECT" --location "$REGION" \
    --uniform-bucket-level-access --public-access-prevention
  # Database dumps older than 30 days are deleted automatically.
  printf '{"rule":[{"action":{"type":"Delete"},"condition":{"age":30}}]}' > /tmp/fa-lifecycle.json
  gcloud storage buckets update "gs://${BUCKET}" --lifecycle-file=/tmp/fa-lifecycle.json
fi
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member "serviceAccount:${SA}" --role roles/storage.objectAdmin >/dev/null

say "Static IP ${IP_NAME}"
if ! gcloud compute addresses describe "$IP_NAME" --region "$REGION" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute addresses create "$IP_NAME" --region "$REGION" --project "$PROJECT"
fi
IP="$(gcloud compute addresses describe "$IP_NAME" --region "$REGION" --project "$PROJECT" --format='value(address)')"
echo "address: ${IP}"

say "Firewall: web from anywhere, SSH only through Google's IAP tunnel"
if ! gcloud compute firewall-rules describe football-ai-web --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules create football-ai-web --project "$PROJECT" --network default \
    --direction INGRESS --allow tcp:80,tcp:443 --target-tags "$TAG" --source-ranges 0.0.0.0/0
fi
if ! gcloud compute firewall-rules describe football-ai-iap-ssh --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules create football-ai-iap-ssh --project "$PROJECT" --network default \
    --direction INGRESS --allow tcp:22 --target-tags "$TAG" --source-ranges 35.235.240.0/20
fi

say "VM ${VM} (${MACHINE_TYPE}, ${DISK_GB} GB)"
if ! gcloud compute instances describe "$VM" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute instances create "$VM" --project "$PROJECT" --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" \
    --image-family debian-12 --image-project debian-cloud \
    --boot-disk-size "${DISK_GB}GB" --boot-disk-type "$DISK_TYPE" \
    --address "$IP_NAME" --tags "$TAG" \
    --service-account "$SA" --scopes cloud-platform \
    --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
    --metadata "football-ai-bucket=${BUCKET},football-ai-registry-host=${REGISTRY_HOST},enable-oslogin=TRUE" \
    --metadata-from-file startup-script="${HERE}/vm-startup.sh"
else
  echo "already exists"
fi

say "Done"
echo "VM address: ${IP}"
echo "Next: deploy/gcp/deploy.sh   (first run also creates the server's .env.prod and login)"
