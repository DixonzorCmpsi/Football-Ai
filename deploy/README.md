# Hosting Football-Ai on Google Cloud

One small VM running three containers: **Postgres**, the **API**, and **Caddy** (HTTPS, the login, the app's static files, and the `/api` proxy). Only Caddy listens on the internet; the database and the API are reachable from the other containers and nowhere else.

```
browser ──https──▶ Caddy ─┬─ /            static app
                          └─ /api/*  ──▶ backend:8000 ──▶ db:5432
```

## What it costs

Sized to sit inside Google's **Always Free** tier (one `e2-micro` per billing account in `us-east1`, up to 30 GB standard disk):

| Item | Per month |
|---|---|
| VM `e2-micro` (1 GB), 30 GB standard disk | $0 (free tier) |
| Public IPv4 address (not covered by the free tier) | ~$3.65 |
| Backups bucket, Artifact Registry, Cloud Build, Secret Manager | cents |
| **Total** | **about $4** |

The app uses about 0.5 GB at rest, so it fits in 1 GB with a 2 GB swap file; it is slower during the nightly data refresh. If it ever feels cramped, `MACHINE_TYPE=e2-small` (2 GB, ~$13/month) is a one-variable change. The assistant's model usage (OpenRouter or your own keys) is billed by the model provider, not by Google.

## First time

Prerequisites: `gcloud` signed in as the project owner, and the repo's changes committed.

```bash
deploy/gcp/provision.sh      # billing link, APIs, service account, bucket, image repo, static IP, firewall, VM
deploy/gcp/deploy.sh         # builds on Cloud Build, pulls on the VM, starts everything, saves the login
deploy/gcp/restore-db.sh     # optional: copy your local database up, so the site starts with today's data
```

`deploy.sh` prints the URL (`https://<ip>`) and writes the login to `~/.football-ai-login` on your computer. The certificate is self-signed until there is a domain, so the browser warns once.

## Access control

- **Now (IP only):** HTTP basic auth at Caddy. Nothing, including `/api`, is served without it.
- **GCP itself:** only the project owner (your Google account) can administer it. The VM has its own service account limited to Secret Manager read, image pull, logging and its backup bucket; it does not use the default compute account (which has project-wide Editor). SSH is only through Google's IAP tunnel, authenticated as your Google account (OS Login); port 22 is not open to the internet.
- **Sign in with Google, one email only:** `deploy/gcp/set-google-login.sh`. It puts `oauth2-proxy` in front of everything (the page, the files and `/api`), so nothing is served until the browser is signed in as the one allowed Google account. It uses a free `<ip-with-dashes>.sslip.io` name that points at the VM: Google rejects a bare IP as a redirect address but accepts a real DNS name, and Caddy gets a normal Let's Encrypt certificate for it (no browser warning). With the console's OAuth consent screen left in *Testing* and only your account listed as a test user, Google refuses every other account too. `set-google-login.sh --disable` returns to the password.

## Moving to a domain

1. Point the domain's **A record** at the VM address (`provision.sh` prints it).
2. `deploy/gcp/set-domain.sh your.domain you@example.com`. It refuses to run until the name resolves to the VM, then switches Caddy to a real Let's Encrypt certificate.

## Everyday

| Task | Command |
|---|---|
| Ship a new version | commit, then `deploy/gcp/deploy.sh` |
| Look at logs | `gcloud compute ssh football-ai --tunnel-through-iap --command "sudo docker logs -f football-ai-backend-1"` |
| Roll back | `git checkout <old commit>` then `deploy/gcp/deploy.sh` (images are tagged by commit) |
| Backups | nightly 03:30 UTC dump to `gs://<project>-backups/db/`, kept 30 days |
| Restore a backup | download the `.sql.gz`, then the `psql` line in `restore-db.sh` |

## Secrets

Nothing sensitive is in the repo or the images. The database password and login hash are generated on the server (`/opt/football-ai/.env.prod`, root-only). The assistant's model key goes in Secret Manager and the backend reads it through the VM's service account (`applications/api/secret_store.py`):

```bash
printf '%s' 'sk-...' | gcloud secrets create openrouter-api-key --data-file=- --project football-ai-478621
```

## What is not done for you

- **Single VM, single disk:** a VM or zone outage means downtime until it is recreated; the nightly dump is the safety net.
- **Daily data refresh** runs inside the API at 06:00 server time, as it does locally. A failed run is visible in the backend log, not in an alert.
- **Bovada props** depend on a scraper that has been unreliable; game lines come from the NFL schedule feed.
