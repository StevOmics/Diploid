# MediaBridge — Project Context

## Project Overview

**MediaBridge** is a self-hosted media cataloging and backup utility. It scans local media folders (movies today), catalogs files in Postgres, syncs watch status from Jellyfin, and backs files up to a local target and/or Google Cloud Storage with optional client-side AES-256-GCM encryption.

### Core Purpose
- Catalog media files (currently video files: mp4, m4v, mkv, avi, mov, wmv) across one or more local "storage locations" (libraries)
- Track per-file content fingerprints (size + partial BLAKE2b) independent of path
- Sync watched/play-count data from a Jellyfin server for one configured user
- Back up files to a local backup target and/or a GCS bucket, with optional encryption, splitting of oversized files, and clumping of many small files into one archive
- Restore and verify (re-hash and compare) backups
- Web UI (FastAPI + Jinja templates, no SPA framework) for catalog browsing, settings, and per-library backup status

---

## Architecture

### Docker Services (`docker-compose.yml`)

| Service | Purpose |
|---|---|
| `db` | Postgres 16 |
| `web` | FastAPI app — UI, auth, catalog, settings (port 8765→8000) |
| `rabbitmq` | Celery broker (management UI on 15672) |
| `celery` | Celery worker on the `default` queue — lightweight/background tasks |
| `celery-beat` | Schedules `retry_failed_transfers` every 5 min onto `default` |
| `worker` | Celery worker on the `copy` queue — actual file copy/backup/restore/verify (has read-write mount of media + backup paths) |
| `flower` | Celery monitoring UI (port 5555) |
| `terminator` | Internal-only FastAPI service that can restart the `celery`/`worker` containers via the Docker socket, gated by `TERMINATOR_API_KEY`; not reachable from the host |

`docker-compose.override.yml` remaps the Linux host bind mounts (`/mnt/homedrive/mediafiles`, `/mnt/backup/mediabridge`) to `./data/*` for local macOS dev — Compose merges it automatically.

### Why `celery` and `worker` are split
Both build from `./worker`'s image (same Celery app/tasks/DB models), but consume different queues: `worker` handles `copy`/`backup_clump`/`verify_copy_job` (potentially slow file transfers), while `celery` handles `default` (ping, retry sweep) so lightweight background work never queues behind a multi-GB file copy.

### Code layout
```
web/app/       FastAPI app: main.py (routes), models.py (SQLAlchemy), catalog.py (scan + Jellyfin sync),
               auth.py, jellyfin.py, gcs.py, fingerprint.py, nfo.py, tasks_client.py (enqueue helpers),
               manage.py (CLI: create-admin), config.py, db.py, templates/
worker/app/    Celery worker: tasks.py (copy/backup/restore/verify/clump), celery_app.py, encryption.py
               (AES-256-GCM), gcs.py, fingerprint.py, models.py (duplicated schema, same DB), config.py
flower/app/    Minimal Celery app config so Flower can inspect the broker
terminator/app/main.py   Docker-socket-gated restart API, allowlisted to celery/worker services
```
Note: `web` and `worker` each define their own `models.py`/`config.py`/`db.py`/`fingerprint.py`/`gcs.py` — they share a database but are not a shared Python package. Changes to the schema or these helpers need to be mirrored in both.

---

## Data Model (`web/app/models.py`)

- **StorageLocation** — a library or backup target. `location_type` is `"local"` or `"gcs"`; at most one local location and independently at most one gcs "location" can have `is_backup_target=True`.
- **MediaFile** — one cataloged file: path, size, fingerprint, NFO-derived metadata (title/year/imdb/tmdb/rating), Jellyfin watch data.
- **CopyJob** — append-only log of one backup/restore/verify operation on one file to one destination. Tracks status/progress/retry state.
- **BackupRecord** — current backup state per (file, destination) pair, upserted on every successful backup. Independent from CopyJob's history log.
- **BackupArchive** — one physical stored blob: a plain single-file copy, one part of a split file, or a shared clump body.
- **BackupRecordArchive** — join table (many-to-many) linking a BackupRecord to the BackupArchive(s) that reconstruct it, with byte offsets — handles the plain / split / clump cases uniformly.
- **BackupEncryptionConfig**, **TransferConfig**, **CloudStorageConfig**, **JellyfinConfig** — singleton settings rows (at most one row each), stored plaintext (password, service account JSON, API key) so scheduled/background transfers can run unattended.

## Backup pipeline (worker/app/tasks.py)
1. **Plan** (`web/app/main.py:_plan_backup`) — decides per-file whether to clump (small files under threshold), split (oversized files), or copy as-is, before creating any CopyJob rows.
2. **Execute** — CopyJobs are created and enqueued (`copy_media_file`, or `backup_clump` for a batch).
3. Encryption (if enabled) happens per-archive via streaming AES-256-GCM (`worker/app/encryption.py`) — each file/part/clump gets its own key derived from a master key + a stable ID, chunked so multi-GB files don't blow up memory.
4. Failed backup/restore jobs retry immediately (short backoff, `TransferConfig.retry_count`) then park as `failed` with `next_retry_at`; `celery-beat` sweeps those back to `pending` every 5 min. `backup_clump` is **not** auto-retried — a failure fails the whole batch and needs a manual re-backup.
5. **Verify** re-derives the backup's fingerprint (decrypting/downloading as needed) and compares to `MediaFile.fingerprint`.

Cloud uploads are throttled to a fraction (`CLOUD_UPLOAD_THROTTLE_FRACTION` / `UPLOAD_THROTTLE_FRACTION`, currently 0.5) of the last measured speed-test bandwidth — this constant must stay in sync between `web/app/main.py` (ETA estimates) and `worker/app/gcs.py` (actual throttling).

---

## Setup & Development

### Quick start
```bash
cp .env.example .env   # then fill in secrets (see below)
docker compose up -d
./setup.sh --admin     # creates/updates the admin user, prompts for a password
```
- Web UI: http://localhost:8765 (login page at `/login`)
- Flower: http://localhost:5555
- RabbitMQ management: http://localhost:15672

### `.env` (see `.env.example`)
`POSTGRES_USER/PASSWORD/DB`, `DATABASE_URL`, `MOVIES_ROOT`, `SECRET_KEY` (session signing), `RABBITMQ_DEFAULT_USER/PASS`, `TERMINATOR_API_KEY`. Generate secrets with `python3 -c "import secrets; print(secrets.token_hex(32))"`.

### Tests
Both `web/` and `worker/` have `tests/` (pytest) and a `requirements-dev.txt`. No CI config is present in the repo yet.

---

## Current Status

Per git log, the app has one substantial commit ("Build MediaBridge: catalog, auth, Jellyfin sync, and backup/restore workflow") plus two unlabeled "updated" commits — this is early-stage, single-developer, actively-in-development code. `docs/` exists but is currently empty.

### Notable design choices worth knowing before changing things
- Secrets (Jellyfin API key, backup password, GCS service account JSON) are stored **plaintext** in Postgres by design, so unattended/scheduled transfers work without a running keyring. This trades off DB-compromise exposure for operational simplicity — worth flagging if this ever moves off a trusted single-host deployment.
- The backup target is deliberately excluded from library scanning (`catalog.py:scan_library`) so backup copies never get cataloged as duplicate source files.
- "Library" in the UI/filtering sense means `StorageLocation`, not `MediaFile.jellyfin_library` (only populated after a Jellyfin sync) — see the comment in `main.py:list_movies`.
- No automated migrations tool is set up — `Base.metadata.create_all` runs on FastAPI startup, so schema changes are additive-only in practice unless a migration is added by hand.

### Known gaps / not yet built
- Only movies/video files are handled end-to-end; the README's "managing files" (beyond movies) and mp3/audio cataloging aren't implemented.
- `backup_clump` has no automatic retry (see above).
- No CI pipeline, no `ARCHITECTURE.md`, no `docs/*.md` content yet.

---

## Open Source / Licensing

- Licensed **AGPL-3.0** (see `LICENSE`), chosen deliberately over a permissive license: Steve is the sole copyright holder today, which keeps the door open to dual-license a commercial version later (open-core model, à la Grafana/MinIO/Sentry) without needing anyone else's consent.
- **Before accepting outside contributions**, this needs a CLA (or equivalent contributor-license terms) — without one, external contributors retain copyright on their own patches and Steve can't legally relicense those pieces commercially. `CONTRIBUTING.md` now covers this with an inline contributor-license clause (PRs are submitted under a grant that lets the project owner relicense them too).
- A sensitive-data audit of the full git history (2026-09-14) found nothing to scrub: `.env` was never committed, no hardcoded secrets exist anywhere in tracked source (everything routes through env vars / DB config), and no key/credential files ever entered history. Decision made to squash history into a single "Initial public release" commit for the public fork rather than reusing the private dev history verbatim (purely for a cleaner external-facing log, not because of any leaked data).

### Branch model: private dev repo + public mirror

This directory is the private `MediaBridge` repo (`origin`, private) *and* tracks the public `Diploid` repo (`public-origin`) in the same working tree, via two branches:

- **`main`** — day-to-day development, pushed to `origin` (private). Can contain WIP and anything not ready for public release. This is also where `internal/` lives.
- **`public-release`** — mirrors what's actually live on Diploid. Only moves forward by merging reviewed work in from `main`; pushing this branch to `public-origin` is the actual "release" action. Its own `.gitignore` also excludes `/internal/` as a second layer of defense.
- **`internal/`** — private, pre-release work (and eventually any commercial/enterprise-only code) that must never reach Diploid. Only exists on `main`. See `internal/README.md`.

To publish: run `scripts/publish-to-diploid.sh`. It merges `main` into `public-release`, hard-strips `internal/` from the result regardless of what `main` did to it, commits, and pushes to `public-origin`. Because both branches share history (merged 2026-09-14 via `--allow-unrelated-histories`, since `public-release` started life as a squashed orphan commit), this is now a normal incremental merge each time — no more from-scratch squashing.

If Diploid ever takes outside PRs, merge them into `public-release`, then merge `public-release` back into `main` to keep the two in sync (safe in that direction, since public content is by definition not sensitive).

---

**Project Owner**: Steve Ayers
