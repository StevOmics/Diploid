from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx
from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from app.auth import verify_password
from app.catalog import scan_library, sync_watch_data
from app.config import VIDEO_EXTENSIONS, settings as app_settings
from app.db import Base, SessionLocal, engine, get_db
from app import jellyfin
from app import models  # noqa: F401  (registers tables with Base.metadata)
from app.models import CopyJob, JellyfinConfig, MediaFile, StorageLocation, User
from app.schemas import MediaFileOut
from app.tasks_client import enqueue_copy_job, enqueue_verify_job

TERMINATOR_URL = "http://terminator:8000"

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
    finally:
        db.close()


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
        query = query.filter(MediaFile.jellyfin_library == library)
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
    libraries = [
        row[0] for row in db.query(MediaFile.jellyfin_library).distinct().order_by(MediaFile.jellyfin_library) if row[0]
    ]
    storage_locations = db.query(StorageLocation).order_by(StorageLocation.name).all()

    backup_location = db.query(StorageLocation).filter_by(is_backup_target=True).first()
    backup_jobs: dict[int, CopyJob] = {}
    if backup_location:
        # Latest *backup* job per file (not restores) to the backup location, in one query (no N+1).
        jobs = (
            db.query(CopyJob)
            .filter_by(destination_storage_location_id=backup_location.id, job_type="backup")
            .order_by(CopyJob.created_at.desc())
        )
        for job in jobs:
            backup_jobs.setdefault(job.media_file_id, job)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "movies": movies,
            "genres": genres,
            "libraries": libraries,
            "storage_locations": storage_locations,
            "backup_location": backup_location,
            "backup_jobs": backup_jobs,
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

    locations = db.query(StorageLocation).order_by(StorageLocation.name).all()
    storage_locations = [
        {"location": location, "exists": Path(location.path).is_dir()} for location in locations
    ]

    jellyfin_config = db.query(JellyfinConfig).first()
    jellyfin_users = []
    if jellyfin_config:
        try:
            jellyfin_users = jellyfin.list_users_cached(jellyfin_config.server_url, jellyfin_config.api_key)
        except httpx.HTTPError:
            pass

    watched_count = db.query(func.count(MediaFile.id)).filter(MediaFile.watched.is_(True)).scalar()

    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "active": "settings",
            "username": username,
            "error": error,
            "storage_locations": storage_locations,
            "video_extensions": ", ".join(sorted(VIDEO_EXTENSIONS)),
            "database_host": database_host,
            "movie_count": movie_count,
            "last_scanned_at": last_scanned_at,
            "jellyfin_config": jellyfin_config,
            "jellyfin_users": jellyfin_users,
            "watched_count": watched_count,
            "flash_status": flash_status,
            "flash_message": flash_message,
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
        db.query(StorageLocation).filter(StorageLocation.id != location_id).update({"is_backup_target": False})
        location.is_backup_target = True
        db.commit()
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/storage-locations/{location_id}/unset-backup", dependencies=[Depends(require_login)])
def unset_backup_location(location_id: int, db: Session = Depends(get_db)):
    location = db.get(StorageLocation, location_id)
    if location:
        location.is_backup_target = False
        db.commit()
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/storage-locations/{location_id}/delete", dependencies=[Depends(require_login)])
def delete_storage_location(location_id: int, db: Session = Depends(get_db)):
    location = db.get(StorageLocation, location_id)
    if location:
        db.query(MediaFile).filter_by(storage_location_id=location.id).delete()
        db.delete(location)
        db.commit()
    return RedirectResponse(url="/settings", status_code=303)


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


def _queue_copy(db: Session, media_file: MediaFile, other_location: StorageLocation, job_type: str = "backup") -> CopyJob:
    job = CopyJob(media_file_id=media_file.id, destination_storage_location_id=other_location.id, job_type=job_type)
    db.add(job)
    db.commit()

    task_id = enqueue_copy_job(job.id)
    job.celery_task_id = task_id
    db.commit()
    return job


def _get_backup_location(db: Session) -> StorageLocation | None:
    return db.query(StorageLocation).filter_by(is_backup_target=True).first()


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


@app.post("/movies/bulk-backup", dependencies=[Depends(require_login)])
def bulk_backup_media_files(media_file_ids: list[int] = Form(default=[]), db: Session = Depends(get_db)):
    backup_location = _get_backup_location(db)
    if not media_file_ids:
        return RedirectResponse(url="/?error=No+files+selected", status_code=303)
    if not backup_location:
        return RedirectResponse(url="/?error=No+backup+location+configured", status_code=303)

    queued = skipped = 0
    for media_file_id in media_file_ids:
        media_file = db.get(MediaFile, media_file_id)
        if not media_file or media_file.storage_location_id == backup_location.id:
            skipped += 1
            continue
        _queue_copy(db, media_file, backup_location, job_type="backup")
        queued += 1

    message = f"Queued {queued} backup job(s)" + (f", skipped {skipped}" if skipped else "")
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
