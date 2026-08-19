from datetime import datetime, timezone
from pathlib import Path

from app.celery_app import app
from app.db import SessionLocal
from app.fingerprint import compute_fingerprint
from app.models import CopyJob, MediaFile, StorageLocation

CHUNK_SIZE = 8 * 1024 * 1024
PROGRESS_COMMIT_INTERVAL = 64 * 1024 * 1024


@app.task(name="ping")
def ping() -> str:
    return "pong"


def _fail(db, job: CopyJob, message: str) -> None:
    job.status = "failed"
    job.error_message = message[:2000]
    db.commit()


def _latest_done_backup(db, media_file_id: int, backup_location_id: int) -> CopyJob | None:
    return (
        db.query(CopyJob)
        .filter_by(
            media_file_id=media_file_id,
            destination_storage_location_id=backup_location_id,
            job_type="backup",
            status="done",
        )
        .order_by(CopyJob.created_at.desc())
        .first()
    )


def _resolve_copy_paths(db, job: CopyJob, media_file: MediaFile, other_location: StorageLocation) -> tuple[Path, Path] | None:
    """Returns (source_path, dest_path) for this job's direction, or None if unresolvable."""
    if job.job_type == "restore":
        existing_backup = _latest_done_backup(db, job.media_file_id, other_location.id)
        if not existing_backup or not existing_backup.destination_path:
            return None
        return Path(existing_backup.destination_path), Path(media_file.path)

    # job_type == "backup"
    source_path = Path(media_file.path)
    source_location = db.get(StorageLocation, media_file.storage_location_id) if media_file.storage_location_id else None
    try:
        rel_path = source_path.relative_to(Path(source_location.path)) if source_location else Path(media_file.filename)
    except ValueError:
        rel_path = Path(media_file.filename)
    return source_path, Path(other_location.path) / rel_path


@app.task(bind=True, name="copy_media_file")
def copy_media_file(self, job_id: int) -> None:
    db = SessionLocal()
    try:
        job = db.get(CopyJob, job_id)
        if not job:
            return

        job.status = "running"
        job.celery_task_id = self.request.id
        db.commit()

        media_file = db.get(MediaFile, job.media_file_id)
        other_location = db.get(StorageLocation, job.destination_storage_location_id)
        if not media_file or not other_location:
            _fail(db, job, "source file or destination location no longer exists")
            return

        paths = _resolve_copy_paths(db, job, media_file, other_location)
        if not paths:
            _fail(db, job, "no completed backup found to restore from")
            return
        source_path, dest_path = paths

        if not source_path.is_file():
            _fail(db, job, f"source file missing: {source_path}")
            return
        if dest_path == source_path:
            _fail(db, job, "source and destination resolve to the same path")
            return

        total = source_path.stat().st_size
        job.total_bytes = total
        db.commit()

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest_path.with_name(dest_path.name + ".mbcopy")

        copied = 0
        last_committed = 0
        with source_path.open("rb") as src, tmp_path.open("wb") as dst:
            while True:
                chunk = src.read(CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
                copied += len(chunk)
                if copied - last_committed >= PROGRESS_COMMIT_INTERVAL:
                    job.progress_bytes = copied
                    db.commit()
                    last_committed = copied

        tmp_path.replace(dest_path)
        job.progress_bytes = copied
        job.destination_path = str(dest_path)
        job.status = "done"
        job.completed_at = datetime.now(timezone.utc)
        db.commit()
    except Exception as exc:
        db.rollback()
        job = db.get(CopyJob, job_id)
        if job:
            _fail(db, job, str(exc))
        raise
    finally:
        db.close()


@app.task(name="verify_copy_job")
def verify_copy_job(job_id: int) -> None:
    db = SessionLocal()
    try:
        job = db.get(CopyJob, job_id)
        if not job or job.job_type != "backup" or job.status != "done" or not job.destination_path:
            return

        media_file = db.get(MediaFile, job.media_file_id)
        backup_path = Path(job.destination_path)

        if not backup_path.is_file():
            job.verify_status = "missing"
        elif media_file and media_file.fingerprint:
            backup_fingerprint = compute_fingerprint(backup_path, backup_path.stat().st_size)
            job.verify_status = "match" if backup_fingerprint == media_file.fingerprint else "mismatch"
        else:
            job.verify_status = "mismatch"

        job.verified_at = datetime.now(timezone.utc)
        db.commit()
    finally:
        db.close()
