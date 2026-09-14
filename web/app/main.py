import json
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx
from fastapi import Depends, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from app.auth import verify_password
from app.catalog import _map_jellyfin_path, scan_library, sync_watch_data
from app.config import VIDEO_EXTENSIONS, settings as app_settings
from app.db import Base, SessionLocal, engine, get_db
from app import gcs, jellyfin
from app import models  # noqa: F401  (registers tables with Base.metadata)
from app.models import (
    BackupEncryptionConfig,
    CloudStorageConfig,
    CopyJob,
    JellyfinConfig,
    MediaFile,
    StorageLocation,
    TransferConfig,
    User,
)
from app.schemas import MediaFileOut
from app.tasks_client import enqueue_clump_backup, enqueue_copy_job, enqueue_verify_job

TERMINATOR_URL = "http://terminator:8000"

# Must match worker/app/gcs.py's UPLOAD_THROTTLE_FRACTION - real cloud uploads
# are capped to this fraction of measured capacity so a backup doesn't
# saturate the connection; ETA estimates here use the same effective rate.
CLOUD_UPLOAD_THROTTLE_FRACTION = 0.5

app = FastAPI(title="MediaBridge")
app.add_middleware(SessionMiddleware, secret_key=app_settings.secret_key)
templates = Jinja2Templates(directory="app/templates")


class NotAuthenticated(Exception):
    pass


@app.exception_handler(NotAuthenticated)
def not_authenticated_handler(request: Request, exc: NotAuthenticated) -> RedirectResponse:
    return RedirectResponse(url="/login", status_code=303)


def require_login(request: Request) -> str:
    username = request.session.get("username")
    if not username:
        raise NotAuthenticated()
    return username


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        if not db.query(StorageLocation).count():
            db.add(StorageLocation(name="Movies", path=app_settings.movies_root, location_type="local"))
            db.commit()

        jellyfin_config = db.query(JellyfinConfig).first()
    finally:
        db.close()

    if jellyfin_config:
        # Warms jellyfin's cached user/library lookups in the background so the
        # first Settings page load after a restart doesn't block on them too.
        for target in (jellyfin.list_users_cached, jellyfin.list_libraries_cached):
            threading.Thread(
                target=target,
                args=(jellyfin_config.server_url, jellyfin_config.api_key),
                daemon=True,
            ).start()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/health/db")
def health_db(db: Session = Depends(get_db)) -> dict:
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str | None = None):
    return templates.TemplateResponse(request, "login.html", {"error": error})


@app.post("/login")
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter_by(username=username).one_or_none()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            request, "login.html", {"error": "Invalid username or password"}, status_code=401
        )
    request.session["username"] = user.username
    return RedirectResponse(url="/", status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.post("/scan", dependencies=[Depends(require_login)])
def scan(db: Session = Depends(get_db)) -> dict:
    return scan_library(db)


@app.get("/movies", response_model=list[MediaFileOut], dependencies=[Depends(require_login)])
def list_movies(
    genre: str | None = None, library: str | None = None, q: str | None = None, db: Session = Depends(get_db)
):
    query = db.query(MediaFile)
    if genre:
        query = query.filter(MediaFile.genre == genre)
    if library:
        # "Library" is the storage location a file lives in (Movies, Shows, ...) - not
        # MediaFile.jellyfin_library, which is only ever set after a Jellyfin watch-status
        # sync and would leave newly-added storage locations unfilterable until then.
        query = query.join(StorageLocation, MediaFile.storage_location_id == StorageLocation.id).filter(
            StorageLocation.name == library
        )
    if q:
        query = query.filter(MediaFile.filename.ilike(f"%{q}%"))
    return query.order_by(MediaFile.genre, MediaFile.filename).all()


@app.get("/", response_class=HTMLResponse)
def catalog_page(
    request: Request,
    genre: str | None = None,
    library: str | None = None,
    q: str | None = None,
    error: str | None = None,
    message: str | None = None,
    select_all: bool = False,
    db: Session = Depends(get_db),
    username: str = Depends(require_login),
):
    movies = list_movies(genre=genre, library=library, q=q, db=db)
    genres = [row[0] for row in db.query(MediaFile.genre).distinct().order_by(MediaFile.genre) if row[0]]
    storage_locations = db.query(StorageLocation).order_by(StorageLocation.name).all()
    # Every configured local storage location (except the backup target) is a library,
    # whether or not anything's been scanned/synced into it yet.
    libraries = [
        location.name for location in storage_locations if not location.is_backup_target and location.location_type == "local"
    ]
    storage_location_by_id = {location.id: location for location in storage_locations}

    backup_location = _get_backup_location(db)
    cloud_backup_location = _get_cloud_backup_location(db)
    backup_jobs: dict[int, CopyJob] = {}
    cloud_backup_jobs: dict[int, CopyJob] = {}
    # Latest completed-or-in-progress "backup" job per (file, destination) pair, so a
    # file's catalog row can stack every location it's been copied to underneath its
    # original location - not just the one designated backup target (a file can also
    # reach other locations via the generic /movies/{id}/copy route).
    file_locations: dict[int, list[CopyJob]] = {}
    seen_destinations: set[tuple[int, int]] = set()
    jobs = db.query(CopyJob).filter_by(job_type="backup").order_by(CopyJob.created_at.desc())
    for job in jobs:
        if backup_location and job.destination_storage_location_id == backup_location.id:
            backup_jobs.setdefault(job.media_file_id, job)
        if cloud_backup_location and job.destination_storage_location_id == cloud_backup_location.id:
            cloud_backup_jobs.setdefault(job.media_file_id, job)

        dest_key = (job.media_file_id, job.destination_storage_location_id)
        if dest_key in seen_destinations:
            continue
        seen_destinations.add(dest_key)
        file_locations.setdefault(job.media_file_id, []).append(job)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "movies": movies,
            "genres": genres,
            "libraries": libraries,
            "storage_locations": storage_locations,
            "storage_location_by_id": storage_location_by_id,
            "backup_location": backup_location,
            "backup_jobs": backup_jobs,
            "cloud_backup_location": cloud_backup_location,
            "cloud_backup_jobs": cloud_backup_jobs,
            "file_locations": file_locations,
            "genre": genre,
            "library": library,
            "q": q,
            "error": error,
            "message": message,
            "select_all": select_all,
            "active": "catalog",
            "username": username,
        },
    )


# Filesystem housekeeping folders that show up on real (especially
# NTFS-formatted external/network) drives but are never a media library.
_IGNORED_FOLDER_NAMES = {"system volume information", "$recycle.bin"}


def _discover_media_folders(existing_paths: set[str]) -> list[dict]:
    """Immediate subfolders of the media root (e.g. /mediafiles/test_lib) that
    aren't already a configured storage location - lets a folder dropped
    straight onto the mount (no Jellyfin library involved) still show up as an
    addable candidate in Settings, the same way Jellyfin libraries do."""
    root = Path(app_settings.media_root)
    if not root.is_dir():
        return []
    return [
        {"name": entry.name, "path": str(entry), "already_added": str(entry) in existing_paths}
        for entry in sorted(root.iterdir(), key=lambda p: p.name.lower())
        if entry.is_dir() and not entry.name.startswith(".") and entry.name.lower() not in _IGNORED_FOLDER_NAMES
    ]


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f} sec"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f} min"
    hours = minutes / 60
    if hours < 48:
        return f"{hours:.1f} hr"
    return f"{hours / 24:.1f} days"


@app.get("/settings", response_class=HTMLResponse)
def settings_page(
    request: Request,
    error: str | None = None,
    flash_status: str | None = None,
    flash_message: str | None = None,
    db: Session = Depends(get_db),
    username: str = Depends(require_login),
):
    movie_count = db.query(func.count(MediaFile.id)).scalar()
    last_scanned_at = db.query(func.max(MediaFile.scanned_at)).scalar()
    db_parts = urlsplit(app_settings.database_url.replace("postgresql+psycopg", "postgresql"))
    database_host = f"{db_parts.hostname}:{db_parts.port}{db_parts.path}"

    # The backup target isn't a source location to scan, so it's kept out of the
    # regular Storage locations table below - mixing it into the scan list made
    # it easy to mistake for just another media folder. Which location (if any)
    # is the backup target, and per-library backup status/actions, now live on
    # the dedicated Libraries page.
    locations = (
        db.query(StorageLocation)
        .filter_by(is_backup_target=False, location_type="local")
        .order_by(StorageLocation.name)
        .all()
    )
    storage_locations = [{"location": location, "exists": Path(location.path).is_dir()} for location in locations]

    backup_location = db.query(StorageLocation).filter_by(is_backup_target=True, location_type="local").first()

    existing_paths = {loc.path for loc in locations}
    if backup_location:
        existing_paths.add(backup_location.path)
    discovered_folders = _discover_media_folders(existing_paths)

    jellyfin_config = db.query(JellyfinConfig).first()
    jellyfin_users = []
    jellyfin_libraries = []
    if jellyfin_config:
        try:
            jellyfin_users = jellyfin.list_users_cached(jellyfin_config.server_url, jellyfin_config.api_key)
        except httpx.HTTPError:
            pass

        try:
            for lib in jellyfin.list_libraries_cached(jellyfin_config.server_url, jellyfin_config.api_key):
                mapped_paths = [
                    mapped for loc in lib["locations"] if (mapped := _map_jellyfin_path(jellyfin_config, loc))
                ]
                # Fall back to Jellyfin's raw path if it doesn't share the configured
                # prefix - still useful as a starting point, the user can edit it after adding.
                suggested_path = mapped_paths[0] if mapped_paths else (lib["locations"][0] if lib["locations"] else "")
                jellyfin_libraries.append(
                    {
                        "name": lib["name"],
                        "suggested_path": suggested_path,
                        "already_added": suggested_path in existing_paths,
                    }
                )
        except httpx.HTTPError:
            pass

    watched_count = db.query(func.count(MediaFile.id)).filter(MediaFile.watched.is_(True)).scalar()
    backup_encryption_config = db.query(BackupEncryptionConfig).first()
    transfer_config = db.query(TransferConfig).first()

    cloud_storage_config = db.query(CloudStorageConfig).first()
    cloud_storage_buckets = (
        json.loads(cloud_storage_config.available_buckets)
        if cloud_storage_config and cloud_storage_config.available_buckets
        else []
    )

    # Rough backup-time estimate from the last measured upload speed - assumes
    # the whole library needs transferring, since nothing's actually gone to
    # cloud storage yet (no way to tell what's already "backed up" there).
    # Real uploads are throttled to CLOUD_UPLOAD_THROTTLE_FRACTION of measured
    # capacity (worker/app/gcs.py enforces this) so the estimate uses that same
    # effective rate rather than the full measured speed - keep the two in sync.
    cloud_library_total_bytes = 0
    cloud_backup_eta = None
    cloud_effective_upload_mbps = None
    if cloud_storage_config and cloud_storage_config.upload_mbps:
        cloud_effective_upload_mbps = cloud_storage_config.upload_mbps * CLOUD_UPLOAD_THROTTLE_FRACTION
        cloud_library_total_bytes = int(db.query(func.sum(MediaFile.size_bytes)).scalar() or 0)
        if cloud_library_total_bytes:
            seconds = (cloud_library_total_bytes * 8 / 1_000_000) / cloud_effective_upload_mbps
            cloud_backup_eta = _format_duration(seconds)

    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "active": "settings",
            "username": username,
            "error": error,
            "storage_locations": storage_locations,
            "discovered_folders": discovered_folders,
            "media_root": app_settings.media_root,
            "video_extensions": ", ".join(sorted(VIDEO_EXTENSIONS)),
            "database_host": database_host,
            "movie_count": movie_count,
            "last_scanned_at": last_scanned_at,
            "jellyfin_config": jellyfin_config,
            "jellyfin_users": jellyfin_users,
            "jellyfin_libraries": jellyfin_libraries,
            "watched_count": watched_count,
            "backup_encryption_config": backup_encryption_config,
            "transfer_config": transfer_config,
            "cloud_storage_config": cloud_storage_config,
            "cloud_storage_buckets": cloud_storage_buckets,
            "cloud_library_total_bytes": cloud_library_total_bytes,
            "cloud_backup_eta": cloud_backup_eta,
            "cloud_effective_upload_mbps": cloud_effective_upload_mbps,
            "flash_status": flash_status,
            "flash_message": flash_message,
        },
    )


@app.get("/libraries", response_class=HTMLResponse)
def libraries_page(
    request: Request,
    error: str | None = None,
    flash_status: str | None = None,
    flash_message: str | None = None,
    db: Session = Depends(get_db),
    username: str = Depends(require_login),
):
    all_local_locations = db.query(StorageLocation).filter_by(location_type="local").order_by(StorageLocation.name).all()
    backup_location = _get_backup_location(db)
    cloud_backup_location = _get_cloud_backup_location(db)
    cloud_storage_config = db.query(CloudStorageConfig).first()

    # Libraries are every local location except whichever one is currently the
    # local backup target - it's a destination, not a source to back up. The
    # same list doubles as the candidate pool for picking a new local target.
    libraries = [loc for loc in all_local_locations if not loc.is_backup_target]

    # Per-library backup status (local / cloud), so each row's buttons show
    # progress instead of firing blind.
    library_totals: dict[int, int] = {}
    for source_location_id, count in db.query(MediaFile.storage_location_id, func.count(MediaFile.id)).group_by(
        MediaFile.storage_location_id
    ):
        library_totals[source_location_id] = count

    media_file_library: dict[int, int] = dict(db.query(MediaFile.id, MediaFile.storage_location_id).all())
    library_local_backed: dict[int, int] = {}
    library_cloud_backed: dict[int, int] = {}
    seen_backup_dest: set[tuple[int, int]] = set()
    backup_done_jobs = db.query(CopyJob).filter_by(job_type="backup", status="done").order_by(CopyJob.created_at.desc())
    for job in backup_done_jobs:
        dest_key = (job.media_file_id, job.destination_storage_location_id)
        if dest_key in seen_backup_dest:
            continue
        seen_backup_dest.add(dest_key)

        source_location_id = media_file_library.get(job.media_file_id)
        if source_location_id is None:
            continue
        if backup_location and job.destination_storage_location_id == backup_location.id:
            library_local_backed[source_location_id] = library_local_backed.get(source_location_id, 0) + 1
        if cloud_backup_location and job.destination_storage_location_id == cloud_backup_location.id:
            library_cloud_backed[source_location_id] = library_cloud_backed.get(source_location_id, 0) + 1

    library_rows = [
        {
            "location": location,
            "total": library_totals.get(location.id, 0),
            "local_backed_up": library_local_backed.get(location.id, 0),
            "cloud_backed_up": library_cloud_backed.get(location.id, 0),
        }
        for location in libraries
    ]

    return templates.TemplateResponse(
        request,
        "libraries.html",
        {
            "active": "libraries",
            "username": username,
            "error": error,
            "flash_status": flash_status,
            "flash_message": flash_message,
            "backup_location": backup_location,
            "backup_location_exists": Path(backup_location.path).is_dir() if backup_location else False,
            "local_target_candidates": libraries,
            "cloud_backup_location": cloud_backup_location,
            "cloud_storage_config": cloud_storage_config,
            "library_rows": library_rows,
        },
    )


@app.post("/settings/storage-locations", dependencies=[Depends(require_login)])
def create_storage_location(name: str = Form(...), path: str = Form(...), db: Session = Depends(get_db)):
    name = name.strip()
    path = path.strip()
    if not name or not path:
        return RedirectResponse(url="/settings?error=Name+and+path+are+required", status_code=303)

    db.add(StorageLocation(name=name, path=path, location_type="local"))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse(url="/settings?error=That+path+is+already+configured", status_code=303)
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/storage-locations/{location_id}/update", dependencies=[Depends(require_login)])
def update_storage_location(
    location_id: int, name: str = Form(...), path: str = Form(...), db: Session = Depends(get_db)
):
    name = name.strip()
    path = path.strip()
    if not name or not path:
        return RedirectResponse(url="/settings?error=Name+and+path+are+required", status_code=303)

    location = db.get(StorageLocation, location_id)
    if location:
        location.name = name
        location.path = path
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            return RedirectResponse(url="/settings?error=That+path+is+already+configured", status_code=303)
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/storage-locations/{location_id}/set-backup", dependencies=[Depends(require_login)])
def set_backup_location(location_id: int, db: Session = Depends(get_db)):
    location = db.get(StorageLocation, location_id)
    if location:
        # At most one *local* backup target is active at a time. The cloud
        # archive is independent and can be active at the same time - see
        # set_cloud_backup_target below.
        db.query(StorageLocation).filter(
            StorageLocation.id != location_id, StorageLocation.location_type == "local"
        ).update({"is_backup_target": False})
        location.is_backup_target = True
        db.commit()
    return RedirectResponse(url="/libraries", status_code=303)


@app.post("/settings/storage-locations/{location_id}/unset-backup", dependencies=[Depends(require_login)])
def unset_backup_location(location_id: int, db: Session = Depends(get_db)):
    location = db.get(StorageLocation, location_id)
    if location:
        location.is_backup_target = False
        db.commit()
    return RedirectResponse(url="/libraries", status_code=303)


@app.post("/settings/storage-locations/{location_id}/delete", dependencies=[Depends(require_login)])
def delete_storage_location(location_id: int, db: Session = Depends(get_db)):
    location = db.get(StorageLocation, location_id)
    if location:
        db.query(MediaFile).filter_by(storage_location_id=location.id).delete()
        db.delete(location)
        db.commit()
    return RedirectResponse(url="/settings", status_code=303)


def _backup_whole_library(db: Session, library_id: int, target: StorageLocation) -> tuple[int, int]:
    """Plans and queues a backup of every file currently cataloged in one
    library (source StorageLocation) to the given destination - the same
    plan-then-execute pipeline the catalog's bulk-backup buttons use, just
    scoped to "everything in this library" instead of an explicit selection."""
    media_file_ids = [row[0] for row in db.query(MediaFile.id).filter_by(storage_location_id=library_id).all()]
    transfer_config = db.query(TransferConfig).first()
    plan, skipped = _plan_backup(db, media_file_ids, target, transfer_config)
    queued = _execute_backup_plan(db, target, plan)
    return queued, skipped


@app.post("/settings/storage-locations/{location_id}/backup-local", dependencies=[Depends(require_login)])
def backup_library_to_local(location_id: int, db: Session = Depends(get_db)):
    library = db.get(StorageLocation, location_id)
    backup_location = _get_backup_location(db)
    if not library:
        return RedirectResponse(url="/libraries?error=Library+not+found", status_code=303)
    if not backup_location:
        return RedirectResponse(url="/libraries?error=No+local+backup+location+configured", status_code=303)

    queued, skipped = _backup_whole_library(db, location_id, backup_location)
    message = f"Queued {queued} local backup job(s) for {library.name}" + (f", skipped {skipped}" if skipped else "")
    return RedirectResponse(url=f"/libraries?flash_status=ok&flash_message={quote(message)}", status_code=303)


@app.post("/settings/storage-locations/{location_id}/backup-cloud", dependencies=[Depends(require_login)])
def backup_library_to_cloud(location_id: int, db: Session = Depends(get_db)):
    library = db.get(StorageLocation, location_id)
    cloud_backup_location = _get_cloud_backup_location(db)
    if not library:
        return RedirectResponse(url="/libraries?error=Library+not+found", status_code=303)
    if not cloud_backup_location:
        return RedirectResponse(url="/libraries?error=No+cloud+archive+configured", status_code=303)

    queued, skipped = _backup_whole_library(db, location_id, cloud_backup_location)
    message = f"Queued {queued} cloud backup job(s) for {library.name}" + (f", skipped {skipped}" if skipped else "")
    return RedirectResponse(url=f"/libraries?flash_status=ok&flash_message={quote(message)}", status_code=303)


@app.post("/settings/backup-encryption", dependencies=[Depends(require_login)])
def save_backup_encryption(enabled: bool = Form(False), password: str = Form(""), db: Session = Depends(get_db)):
    config = db.query(BackupEncryptionConfig).first()
    if not config:
        config = BackupEncryptionConfig()
        db.add(config)

    password = password.strip()
    if password:
        # Changing the password re-salts too - any *existing* encrypted backups
        # were made with the old password and won't decrypt with the new one.
        config.password = password
        config.kdf_salt = secrets.token_hex(16)
    elif enabled and not config.password:
        return RedirectResponse(url="/settings?error=Set+a+password+before+enabling+encryption", status_code=303)

    config.enabled = enabled
    db.commit()
    return RedirectResponse(url="/settings?flash_status=ok&flash_message=Backup+encryption+settings+saved", status_code=303)


@app.post("/settings/transfer", dependencies=[Depends(require_login)])
def save_transfer_config(
    max_speed_mbps: int = Form(...),
    max_size_gb: float = Form(...),
    split_over_percent: int = Form(...),
    min_size_mb: float = Form(...),
    clump_under_percent: int = Form(...),
    clump_split_enabled: bool = Form(False),
    db: Session = Depends(get_db),
):
    if max_speed_mbps <= 0 or max_size_gb <= 0 or min_size_mb <= 0:
        return RedirectResponse(url="/settings?error=Speed+and+size+values+must+be+positive", status_code=303)
    if not (0 <= split_over_percent <= 500) or not (0 <= clump_under_percent <= 100):
        return RedirectResponse(url="/settings?error=Split%2Fclump+percentages+are+out+of+range", status_code=303)
    if min_size_mb >= max_size_gb * 1024:
        return RedirectResponse(url="/settings?error=Min+size+must+be+smaller+than+max+size", status_code=303)

    config = db.query(TransferConfig).first()
    if not config:
        config = TransferConfig()
        db.add(config)

    config.max_speed_mbps = max_speed_mbps
    config.max_size_gb = max_size_gb
    config.split_over_percent = split_over_percent
    config.min_size_mb = min_size_mb
    config.clump_under_percent = clump_under_percent
    config.clump_split_enabled = clump_split_enabled
    db.commit()
    return RedirectResponse(url="/settings?flash_status=ok&flash_message=Transfer+settings+saved", status_code=303)


@app.post("/settings/transfer-retry", dependencies=[Depends(require_login)])
def save_transfer_retry_config(
    retry_count: int = Form(...),
    retry_interval_minutes: int = Form(...),
    db: Session = Depends(get_db),
):
    if retry_count < 0:
        return RedirectResponse(url="/settings?error=Retry+count+cannot+be+negative", status_code=303)
    if retry_interval_minutes <= 0:
        return RedirectResponse(url="/settings?error=Retry+interval+must+be+positive", status_code=303)

    config = db.query(TransferConfig).first()
    if not config:
        config = TransferConfig()
        db.add(config)

    config.retry_count = retry_count
    config.retry_interval_minutes = retry_interval_minutes
    db.commit()
    return RedirectResponse(url="/settings?flash_status=ok&flash_message=Retry+settings+saved", status_code=303)


def _get_cloud_storage_config(db: Session) -> CloudStorageConfig | None:
    return db.query(CloudStorageConfig).first()


@app.post("/settings/cloud-storage/credentials", dependencies=[Depends(require_login)])
async def save_cloud_storage_credentials(key_file: UploadFile = File(...), db: Session = Depends(get_db)):
    raw = (await key_file.read()).decode("utf-8", errors="replace")
    try:
        data = gcs.parse_and_validate_key(raw)
    except ValueError as exc:
        return RedirectResponse(url=f"/settings?error={quote(str(exc))}", status_code=303)

    config = _get_cloud_storage_config(db)
    if not config:
        config = CloudStorageConfig()
        db.add(config)

    # A new key invalidates any bucket list/selection fetched with the old one.
    config.service_account_json = raw
    config.service_account_email = data["client_email"]
    config.project_id = data["project_id"]
    config.available_buckets = None
    config.bucket_name = None
    config.connected = False
    config.last_error = None
    db.commit()
    return RedirectResponse(url="/settings?flash_status=ok&flash_message=Service+account+key+saved", status_code=303)


@app.post("/settings/cloud-storage/list-buckets", dependencies=[Depends(require_login)])
def list_cloud_storage_buckets(db: Session = Depends(get_db)):
    config = _get_cloud_storage_config(db)
    if not config or not config.service_account_json:
        return RedirectResponse(url="/settings?error=Upload+a+service+account+key+first", status_code=303)

    try:
        buckets = gcs.list_buckets(config.service_account_json, config.project_id)
    except Exception as exc:
        config.last_error = str(exc)[:1000]
        db.commit()
        return RedirectResponse(url=f"/settings?error={quote('Could not list buckets: ' + str(exc)[:200])}", status_code=303)

    config.available_buckets = json.dumps(buckets)
    config.last_error = None
    db.commit()
    message = f"Found {len(buckets)} bucket(s)" if buckets else "No buckets found in this project"
    return RedirectResponse(url=f"/settings?flash_status=ok&flash_message={quote(message)}", status_code=303)


@app.post("/settings/cloud-storage/bucket", dependencies=[Depends(require_login)])
def select_cloud_storage_bucket(bucket_name: str = Form(...), db: Session = Depends(get_db)):
    config = _get_cloud_storage_config(db)
    if not config or not config.service_account_json:
        return RedirectResponse(url="/settings?error=Connect+a+service+account+first", status_code=303)
    bucket_name = bucket_name.strip()
    if not bucket_name:
        return RedirectResponse(url="/settings?error=Bucket+name+cannot+be+blank", status_code=303)

    try:
        gcs.verify_bucket_access(config.service_account_json, bucket_name, config.project_id)
    except Exception as exc:
        config.connected = False
        config.last_error = str(exc)[:1000]
        db.commit()
        return RedirectResponse(url=f"/settings?error={quote(str(exc)[:200])}", status_code=303)

    config.bucket_name = bucket_name
    config.connected = True
    config.last_error = None
    db.commit()
    return RedirectResponse(url=f"/settings?flash_status=ok&flash_message=Connected+to+bucket+{quote(bucket_name)}", status_code=303)


@app.post("/settings/cloud-storage/test-speed", dependencies=[Depends(require_login)])
def test_cloud_storage_speed(db: Session = Depends(get_db)):
    config = _get_cloud_storage_config(db)
    if not config or not config.service_account_json or not config.bucket_name:
        return RedirectResponse(url="/settings?error=Connect+to+a+bucket+first", status_code=303)

    try:
        result = gcs.test_connectivity_and_speed(config.service_account_json, config.bucket_name, config.project_id)
    except Exception as exc:
        config.last_error = str(exc)[:1000]
        db.commit()
        return RedirectResponse(url=f"/settings?error={quote('Connectivity test failed: ' + str(exc)[:200])}", status_code=303)

    config.upload_mbps = result["upload_mbps"]
    config.download_mbps = result["download_mbps"]
    config.last_speed_test_at = datetime.now(timezone.utc)
    config.last_error = None
    db.commit()
    message = f"Connectivity OK - {result['upload_mbps']:.1f} Mbps up / {result['download_mbps']:.1f} Mbps down"
    return RedirectResponse(url=f"/settings?flash_status=ok&flash_message={quote(message)}", status_code=303)


@app.post("/settings/cloud-storage/disconnect", dependencies=[Depends(require_login)])
def disconnect_cloud_storage(db: Session = Depends(get_db)):
    config = _get_cloud_storage_config(db)
    if config:
        db.delete(config)
        db.commit()
    return RedirectResponse(url="/settings?flash_status=ok&flash_message=Cloud+storage+disconnected", status_code=303)


@app.post("/settings/cloud-storage/set-backup", dependencies=[Depends(require_login)])
def set_cloud_backup_target(db: Session = Depends(get_db)):
    config = _get_cloud_storage_config(db)
    if not config or not config.connected or not config.bucket_name:
        return RedirectResponse(url="/libraries?error=Connect+a+bucket+in+Settings+before+using+it+as+the+cloud+archive", status_code=303)

    # Independent of any local backup target - both can be active at once (see
    # set_backup_location above, which only clears other *local* targets).
    config.is_backup_target = True

    # The transfer pipeline (worker) still keys backup destinations off
    # StorageLocation - this shadow row (location_type="gcs") lets the existing
    # CopyJob/BackupArchive schema represent "the bucket" as a destination
    # without a parallel destination-type system running through every route.
    cloud_location = db.query(StorageLocation).filter_by(location_type="gcs").first()
    if not cloud_location:
        cloud_location = StorageLocation(location_type="gcs")
        db.add(cloud_location)
    cloud_location.name = f"Cloud: {config.bucket_name}"
    cloud_location.path = f"gcs://{config.bucket_name}"
    cloud_location.is_backup_target = True

    # Cloud backups default to encrypted, clumped/split transfers - this just
    # sets the intent, same as the manual toggles in the Backup encryption /
    # Transfer settings sections below.
    encryption = db.query(BackupEncryptionConfig).first()
    if not encryption:
        encryption = BackupEncryptionConfig()
        db.add(encryption)
    encryption.enabled = True

    transfer = db.query(TransferConfig).first()
    if not transfer:
        transfer = TransferConfig()
        db.add(transfer)
    transfer.clump_split_enabled = True

    db.commit()
    return RedirectResponse(url="/libraries?flash_status=ok&flash_message=Bucket+set+as+cloud+archive", status_code=303)


@app.post("/settings/cloud-storage/unset-backup", dependencies=[Depends(require_login)])
def unset_cloud_backup_target(db: Session = Depends(get_db)):
    config = _get_cloud_storage_config(db)
    if config:
        config.is_backup_target = False
    cloud_location = db.query(StorageLocation).filter_by(location_type="gcs").first()
    if cloud_location:
        cloud_location.is_backup_target = False
    db.commit()
    return RedirectResponse(url="/libraries", status_code=303)


@app.post("/settings/jellyfin", dependencies=[Depends(require_login)])
def save_jellyfin_config(server_url: str = Form(...), api_key: str = Form(...), db: Session = Depends(get_db)):
    server_url = server_url.strip()
    api_key = api_key.strip()
    if not server_url or not api_key:
        return RedirectResponse(url="/settings?error=Server+URL+and+API+key+are+required", status_code=303)

    config = db.query(JellyfinConfig).first()
    if config:
        config.server_url = server_url
        config.api_key = api_key
    else:
        db.add(JellyfinConfig(server_url=server_url, api_key=api_key))
    db.commit()
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/jellyfin/delete", dependencies=[Depends(require_login)])
def delete_jellyfin_config(db: Session = Depends(get_db)):
    config = db.query(JellyfinConfig).first()
    if config:
        db.delete(config)
        db.commit()
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/jellyfin/test", dependencies=[Depends(require_login)])
def test_jellyfin_connection(db: Session = Depends(get_db)):
    config = db.query(JellyfinConfig).first()
    if not config:
        return RedirectResponse(url="/settings?flash_status=error&flash_message=No+Jellyfin+server+configured", status_code=303)

    result = jellyfin.test_connection(config.server_url, config.api_key)
    status = "ok" if result["ok"] else "error"
    return RedirectResponse(
        url=f"/settings?flash_status={status}&flash_message={quote(result['message'])}", status_code=303
    )


@app.post("/settings/jellyfin/sync-user", dependencies=[Depends(require_login)])
def save_jellyfin_sync_user(
    sync_user_id: str = Form(...),
    sync_user_name: str = Form(...),
    path_prefix_from: str = Form(...),
    path_prefix_to: str = Form(...),
    db: Session = Depends(get_db),
):
    config = db.query(JellyfinConfig).first()
    if not config:
        return RedirectResponse(url="/settings?error=Save+a+Jellyfin+connection+first", status_code=303)

    config.sync_user_id = sync_user_id
    config.sync_user_name = sync_user_name
    config.path_prefix_from = path_prefix_from.strip() or "/media"
    config.path_prefix_to = path_prefix_to.strip() or "/mediafiles"
    db.commit()
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/jellyfin/sync", dependencies=[Depends(require_login)])
def run_jellyfin_sync(db: Session = Depends(get_db)):
    config = db.query(JellyfinConfig).first()
    if not config or not config.sync_user_id:
        return RedirectResponse(
            url="/settings?flash_status=error&flash_message=Pick+a+Jellyfin+user+to+sync+first", status_code=303
        )

    try:
        result = sync_watch_data(db, config)
    except httpx.HTTPError as exc:
        return RedirectResponse(url=f"/settings?flash_status=error&flash_message={quote(f'Sync failed: {exc}')}", status_code=303)

    message = f"Synced {result['matched']} of {result['items']} movies from Jellyfin ({result['unmatched']} unmatched)"
    return RedirectResponse(url=f"/settings?flash_status=ok&flash_message={quote(message)}", status_code=303)


@app.post("/scan-and-redirect", dependencies=[Depends(require_login)])
def scan_and_redirect(db: Session = Depends(get_db)):
    scan_library(db)
    return RedirectResponse(url="/", status_code=303)


def _queue_copy(
    db: Session, media_file: MediaFile, other_location: StorageLocation, job_type: str = "backup", enqueue: bool = True
) -> CopyJob:
    job = CopyJob(media_file_id=media_file.id, destination_storage_location_id=other_location.id, job_type=job_type)
    db.add(job)
    db.commit()

    if enqueue:
        task_id = enqueue_copy_job(job.id)
        job.celery_task_id = task_id
        db.commit()
    return job


def _get_backup_location(db: Session) -> StorageLocation | None:
    return db.query(StorageLocation).filter_by(is_backup_target=True, location_type="local").first()


def _get_cloud_backup_location(db: Session) -> StorageLocation | None:
    return db.query(StorageLocation).filter_by(is_backup_target=True, location_type="gcs").first()


def _latest_done_backup(db: Session, media_file_id: int, backup_location_id: int) -> CopyJob | None:
    return (
        db.query(CopyJob)
        .filter_by(media_file_id=media_file_id, destination_storage_location_id=backup_location_id, job_type="backup", status="done")
        .order_by(CopyJob.created_at.desc())
        .first()
    )


@app.post("/movies/{media_file_id}/copy", dependencies=[Depends(require_login)])
def copy_media_file(
    media_file_id: int, destination_storage_location_id: int = Form(...), db: Session = Depends(get_db)
):
    media_file = db.get(MediaFile, media_file_id)
    destination = db.get(StorageLocation, destination_storage_location_id)
    if not media_file or not destination:
        return RedirectResponse(url="/?error=File+or+destination+not+found", status_code=303)
    if destination.id == media_file.storage_location_id:
        return RedirectResponse(url="/?error=File+is+already+in+that+location", status_code=303)

    _queue_copy(db, media_file, destination)
    return RedirectResponse(url="/copy-jobs", status_code=303)


@app.post("/movies/{media_file_id}/backup", dependencies=[Depends(require_login)])
def backup_media_file(media_file_id: int, db: Session = Depends(get_db)):
    media_file = db.get(MediaFile, media_file_id)
    backup_location = _get_backup_location(db)
    if not media_file or not backup_location:
        return RedirectResponse(url="/?error=No+backup+location+configured", status_code=303)
    if backup_location.id == media_file.storage_location_id:
        return RedirectResponse(url="/?error=File+is+already+in+the+backup+location", status_code=303)

    _queue_copy(db, media_file, backup_location, job_type="backup")
    return RedirectResponse(url="/copy-jobs", status_code=303)


@app.post("/movies/{media_file_id}/backup-cloud", dependencies=[Depends(require_login)])
def backup_media_file_to_cloud(media_file_id: int, db: Session = Depends(get_db)):
    media_file = db.get(MediaFile, media_file_id)
    cloud_backup_location = _get_cloud_backup_location(db)
    if not media_file or not cloud_backup_location:
        return RedirectResponse(url="/?error=No+cloud+archive+configured", status_code=303)
    if cloud_backup_location.id == media_file.storage_location_id:
        return RedirectResponse(url="/?error=File+is+already+in+the+cloud+archive", status_code=303)

    _queue_copy(db, media_file, cloud_backup_location, job_type="backup")
    return RedirectResponse(url="/copy-jobs", status_code=303)


@app.post("/movies/{media_file_id}/restore", dependencies=[Depends(require_login)])
def restore_media_file(media_file_id: int, db: Session = Depends(get_db)):
    media_file = db.get(MediaFile, media_file_id)
    backup_location = _get_backup_location(db)
    if not media_file or not backup_location:
        return RedirectResponse(url="/?error=No+backup+location+configured", status_code=303)
    if not _latest_done_backup(db, media_file_id, backup_location.id):
        return RedirectResponse(url="/?error=No+completed+backup+to+restore+from", status_code=303)

    _queue_copy(db, media_file, backup_location, job_type="restore")
    return RedirectResponse(url="/copy-jobs", status_code=303)


@app.post("/movies/{media_file_id}/restore-cloud", dependencies=[Depends(require_login)])
def restore_media_file_from_cloud(media_file_id: int, db: Session = Depends(get_db)):
    media_file = db.get(MediaFile, media_file_id)
    cloud_backup_location = _get_cloud_backup_location(db)
    if not media_file or not cloud_backup_location:
        return RedirectResponse(url="/?error=No+cloud+archive+configured", status_code=303)
    if not _latest_done_backup(db, media_file_id, cloud_backup_location.id):
        return RedirectResponse(url="/?error=No+completed+cloud+backup+to+restore+from", status_code=303)

    _queue_copy(db, media_file, cloud_backup_location, job_type="restore")
    return RedirectResponse(url="/copy-jobs", status_code=303)


@app.post("/movies/{media_file_id}/verify", dependencies=[Depends(require_login)])
def verify_media_file(media_file_id: int, db: Session = Depends(get_db)):
    backup_location = _get_backup_location(db)
    if not backup_location:
        return RedirectResponse(url="/?error=No+backup+location+configured", status_code=303)

    job = _latest_done_backup(db, media_file_id, backup_location.id)
    if not job:
        return RedirectResponse(url="/?error=No+completed+backup+to+verify", status_code=303)

    enqueue_verify_job(job.id)
    return RedirectResponse(url="/", status_code=303)


@app.post("/movies/{media_file_id}/verify-cloud", dependencies=[Depends(require_login)])
def verify_media_file_cloud(media_file_id: int, db: Session = Depends(get_db)):
    cloud_backup_location = _get_cloud_backup_location(db)
    if not cloud_backup_location:
        return RedirectResponse(url="/?error=No+cloud+archive+configured", status_code=303)

    job = _latest_done_backup(db, media_file_id, cloud_backup_location.id)
    if not job:
        return RedirectResponse(url="/?error=No+completed+cloud+backup+to+verify", status_code=303)

    enqueue_verify_job(job.id)
    return RedirectResponse(url="/", status_code=303)


@dataclass
class BackupPlanItem:
    """One unit of work decided by _plan_backup: either a single file backed
    up on its own, or a group of 2+ small files that will share one clump
    archive. Splitting a single oversized file is still decided later, inside
    the worker, since it never depends on other files in the batch."""

    kind: str  # "single" | "clump"
    media_files: list[MediaFile]


def _plan_backup(
    db: Session, media_file_ids: list[int], backup_location: StorageLocation, transfer_config: TransferConfig | None
) -> tuple[list[BackupPlanItem], int]:
    """Decides the full clumping strategy for a batch of files up front, before
    any CopyJob rows are created or anything is enqueued. Mirrors step 3 (of
    the 5-step backup process: connectivity test, local file survey,
    clumping/splitting strategy, encryption, transfer) as its own standalone
    planning pass, so the execution phase that follows just walks the
    already-decided plan in order."""
    clump_enabled = bool(transfer_config and transfer_config.clump_split_enabled)
    clump_threshold_bytes = (
        transfer_config.min_size_mb * 1024 * 1024 * (1 - transfer_config.clump_under_percent / 100) if clump_enabled else 0
    )
    max_clump_bytes = int((transfer_config.max_size_gb if transfer_config else 2.0) * 1024**3)

    plan: list[BackupPlanItem] = []
    skipped = 0
    clump_batch: list[MediaFile] = []
    clump_batch_bytes = 0

    def flush_clump_batch() -> None:
        nonlocal clump_batch, clump_batch_bytes
        if len(clump_batch) >= 2:
            plan.append(BackupPlanItem("clump", clump_batch))
        elif clump_batch:
            plan.append(BackupPlanItem("single", clump_batch))
        clump_batch, clump_batch_bytes = [], 0

    for media_file_id in media_file_ids:
        media_file = db.get(MediaFile, media_file_id)
        if not media_file or media_file.storage_location_id == backup_location.id:
            skipped += 1
            continue

        if clump_enabled and media_file.size_bytes < clump_threshold_bytes:
            if clump_batch and clump_batch_bytes + media_file.size_bytes > max_clump_bytes:
                flush_clump_batch()
            clump_batch.append(media_file)
            clump_batch_bytes += media_file.size_bytes
        else:
            plan.append(BackupPlanItem("single", [media_file]))

    flush_clump_batch()
    return plan, skipped


def _execute_backup_plan(db: Session, backup_location: StorageLocation, plan: list[BackupPlanItem]) -> int:
    """Walks the already-decided plan incrementally, one unit at a time, and
    only now creates/enqueues CopyJobs. Clumping the member files together
    still happens inside backup_clump itself, immediately before the
    encrypt/upload step, once that task picks up the job."""
    queued = 0
    for item in plan:
        if item.kind == "clump":
            jobs = [
                _queue_copy(db, media_file, backup_location, job_type="backup", enqueue=False)
                for media_file in item.media_files
            ]
            task_id = enqueue_clump_backup([job.id for job in jobs])
            for job in jobs:
                job.celery_task_id = task_id
            db.commit()
            queued += len(jobs)
        else:
            _queue_copy(db, item.media_files[0], backup_location, job_type="backup")
            queued += 1
    return queued


@app.post("/movies/bulk-backup", dependencies=[Depends(require_login)])
def bulk_backup_media_files(media_file_ids: list[int] = Form(default=[]), db: Session = Depends(get_db)):
    backup_location = _get_backup_location(db)
    if not media_file_ids:
        return RedirectResponse(url="/?error=No+files+selected", status_code=303)
    if not backup_location:
        return RedirectResponse(url="/?error=No+backup+location+configured", status_code=303)

    transfer_config = db.query(TransferConfig).first()
    plan, skipped = _plan_backup(db, media_file_ids, backup_location, transfer_config)
    queued = _execute_backup_plan(db, backup_location, plan)

    message = f"Queued {queued} backup job(s)" + (f", skipped {skipped}" if skipped else "")
    return RedirectResponse(url=f"/copy-jobs?message={quote(message)}", status_code=303)


@app.post("/movies/bulk-backup-cloud", dependencies=[Depends(require_login)])
def bulk_backup_media_files_to_cloud(media_file_ids: list[int] = Form(default=[]), db: Session = Depends(get_db)):
    cloud_backup_location = _get_cloud_backup_location(db)
    if not media_file_ids:
        return RedirectResponse(url="/?error=No+files+selected", status_code=303)
    if not cloud_backup_location:
        return RedirectResponse(url="/?error=No+cloud+archive+configured", status_code=303)

    transfer_config = db.query(TransferConfig).first()
    plan, skipped = _plan_backup(db, media_file_ids, cloud_backup_location, transfer_config)
    queued = _execute_backup_plan(db, cloud_backup_location, plan)

    message = f"Queued {queued} cloud backup job(s)" + (f", skipped {skipped}" if skipped else "")
    return RedirectResponse(url=f"/copy-jobs?message={quote(message)}", status_code=303)


@app.post("/movies/bulk-restore", dependencies=[Depends(require_login)])
def bulk_restore_media_files(media_file_ids: list[int] = Form(default=[]), db: Session = Depends(get_db)):
    backup_location = _get_backup_location(db)
    if not media_file_ids:
        return RedirectResponse(url="/?error=No+files+selected", status_code=303)
    if not backup_location:
        return RedirectResponse(url="/?error=No+backup+location+configured", status_code=303)

    queued = skipped = 0
    for media_file_id in media_file_ids:
        media_file = db.get(MediaFile, media_file_id)
        if not media_file or not _latest_done_backup(db, media_file_id, backup_location.id):
            skipped += 1
            continue
        _queue_copy(db, media_file, backup_location, job_type="restore")
        queued += 1

    message = f"Queued {queued} restore job(s)" + (f", skipped {skipped} (no backup found)" if skipped else "")
    return RedirectResponse(url=f"/copy-jobs?message={quote(message)}", status_code=303)


@app.post("/movies/bulk-verify", dependencies=[Depends(require_login)])
def bulk_verify_media_files(media_file_ids: list[int] = Form(default=[]), db: Session = Depends(get_db)):
    backup_location = _get_backup_location(db)
    if not media_file_ids:
        return RedirectResponse(url="/?error=No+files+selected", status_code=303)
    if not backup_location:
        return RedirectResponse(url="/?error=No+backup+location+configured", status_code=303)

    queued = skipped = 0
    for media_file_id in media_file_ids:
        job = _latest_done_backup(db, media_file_id, backup_location.id)
        if not job:
            skipped += 1
            continue
        enqueue_verify_job(job.id)
        queued += 1

    message = f"Queued {queued} verify job(s)" + (f", skipped {skipped} (no backup found)" if skipped else "")
    return RedirectResponse(url=f"/?message={quote(message)}", status_code=303)


@app.post("/movies/bulk-copy", dependencies=[Depends(require_login)])
def bulk_copy_media_files(
    media_file_ids: list[int] = Form(default=[]),
    destination_storage_location_id: int | None = Form(None),
    backup_target_id: int | None = Form(None),
    db: Session = Depends(get_db),
):
    destination_id = backup_target_id or destination_storage_location_id
    if not media_file_ids:
        return RedirectResponse(url="/?error=No+files+selected", status_code=303)
    if not destination_id:
        return RedirectResponse(url="/?error=No+destination+selected", status_code=303)

    destination = db.get(StorageLocation, destination_id)
    if not destination:
        return RedirectResponse(url="/?error=Destination+not+found", status_code=303)

    queued = skipped = 0
    for media_file_id in media_file_ids:
        media_file = db.get(MediaFile, media_file_id)
        if not media_file or media_file.storage_location_id == destination.id:
            skipped += 1
            continue
        _queue_copy(db, media_file, destination)
        queued += 1

    message = f"Queued {queued} copy job(s) to {destination.name}"
    if skipped:
        message += f", skipped {skipped}"
    return RedirectResponse(url=f"/copy-jobs?message={quote(message)}", status_code=303)


@app.get("/copy-jobs", response_class=HTMLResponse)
def copy_jobs_page(
    request: Request, message: str | None = None, db: Session = Depends(get_db), username: str = Depends(require_login)
):
    jobs = (
        db.query(CopyJob, MediaFile, StorageLocation)
        .join(MediaFile, CopyJob.media_file_id == MediaFile.id)
        .join(StorageLocation, CopyJob.destination_storage_location_id == StorageLocation.id)
        .order_by(CopyJob.created_at.desc())
        .limit(200)
        .all()
    )
    return templates.TemplateResponse(
        request, "copy_jobs.html", {"active": "copy_jobs", "username": username, "jobs": jobs, "message": message}
    )


@app.post("/settings/services/{service}/restart", dependencies=[Depends(require_login)])
def restart_service(service: str):
    try:
        response = httpx.post(
            f"{TERMINATOR_URL}/services/{service}/restart",
            headers={"X-Terminator-Key": app_settings.terminator_api_key},
            timeout=15.0,
        )
    except httpx.RequestError as exc:
        return RedirectResponse(url=f"/settings?flash_status=error&flash_message={quote(f'Could not reach terminator: {exc}')}", status_code=303)

    if response.status_code != 200:
        return RedirectResponse(
            url=f"/settings?flash_status=error&flash_message={quote(f'Restart failed: {response.text}')}", status_code=303
        )
    return RedirectResponse(url=f"/settings?flash_status=ok&flash_message={quote(f'Restarted {service}')}", status_code=303)
