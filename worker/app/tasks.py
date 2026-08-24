import json
import secrets
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography.exceptions import InvalidTag

from app import gcs
from app.celery_app import app
from app.db import SessionLocal
from app.encryption import backup_exists, decrypt_file, derive_file_key, derive_master_key, derive_path_id, encrypt_file
from app.fingerprint import compute_fingerprint
from app.models import (
    BackupArchive,
    BackupEncryptionConfig,
    BackupRecord,
    BackupRecordArchive,
    CloudStorageConfig,
    CopyJob,
    MediaFile,
    StorageLocation,
    TransferConfig,
)

CHUNK_SIZE = 8 * 1024 * 1024
PROGRESS_COMMIT_INTERVAL = 64 * 1024 * 1024
# Gap between an immediate retry and the one before it. Short on purpose -
# these are for transient errors (a network blip, a momentarily locked file);
# the hourly sweep (retry_failed_transfers) handles the "come back later" case.
RETRY_BACKOFF_SECONDS = 30
# Only backup/restore jobs get retried - they're the actual file transfers.
# Clump backups (backup_clump) aren't retried automatically yet - see its docstring.
RETRYABLE_JOB_TYPES = ("backup", "restore")


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


def _ledger_parts(db, media_file_id: int, destination_storage_location_id: int) -> list[tuple[BackupRecordArchive, BackupArchive]]:
    """The current backup ledger for a file at one specific destination,
    ordered for reconstruction: one row for a normal single-archive backup, N
    rows (one per BackupArchive) for a split file, or one row pointing at a
    shared clump archive. A file backed up to both a local target and the
    cloud archive has two independent ledger rows (one per destination) - this
    always reconstructs from the one the caller actually asked for. Empty if
    this (file, destination) pair predates the ledger (e.g. a backup made
    before this feature existed) - callers fall back to
    CopyJob.destination_path/encrypted in that case."""
    record = (
        db.query(BackupRecord)
        .filter_by(media_file_id=media_file_id, destination_storage_location_id=destination_storage_location_id)
        .one_or_none()
    )
    if not record:
        return []
    return (
        db.query(BackupRecordArchive, BackupArchive)
        .join(BackupArchive, BackupArchive.id == BackupRecordArchive.backup_archive_id)
        .filter(BackupRecordArchive.backup_record_id == record.id)
        .order_by(BackupRecordArchive.part_index)
        .all()
    )


def _record_backup(db, media_file: MediaFile, destination_location: StorageLocation, job: CopyJob) -> None:
    """Upserts the BackupRecord/BackupArchive/BackupRecordArchive ledger after a
    plain (non-split, non-clump) backup - the always-current "what's backed up
    and where" state, as opposed to CopyJob's append-only log of individual
    operations. Split backups use _record_split_backup instead; clumps are
    recorded inline in backup_clump - both write directly into this same
    ledger, just with more than one row.
    """
    archive = (
        db.query(BackupArchive)
        .filter_by(storage_location_id=destination_location.id, path=job.destination_path)
        .one_or_none()
    )
    if not archive:
        archive = BackupArchive(storage_location_id=destination_location.id, path=job.destination_path)
        db.add(archive)
    archive.size_bytes = media_file.size_bytes
    archive.encrypted = job.encrypted
    archive.checksum = None if job.encrypted else media_file.fingerprint
    archive.is_clump = False
    archive.encryption_key_id = None

    record = (
        db.query(BackupRecord)
        .filter_by(media_file_id=media_file.id, destination_storage_location_id=destination_location.id)
        .one_or_none()
    )
    if not record:
        record = BackupRecord(media_file_id=media_file.id, destination_storage_location_id=destination_location.id)
        db.add(record)
    record.local_path = media_file.path
    record.local_checksum = media_file.fingerprint
    record.status = "done"
    # A fresh backup makes any earlier verify result stale until re-verified.
    record.verify_status = None
    record.verified_at = None
    db.flush()  # need archive.id/record.id before linking them

    db.query(BackupRecordArchive).filter_by(backup_record_id=record.id).delete()
    db.add(
        BackupRecordArchive(
            backup_record_id=record.id,
            backup_archive_id=archive.id,
            part_index=0,
            archive_offset=0,
            archive_length=media_file.size_bytes,
        )
    )


def _get_master_key(db) -> bytes | None:
    config = db.query(BackupEncryptionConfig).first()
    if not config or not config.enabled or not config.password or not config.kdf_salt:
        return None
    return derive_master_key(config.password, bytes.fromhex(config.kdf_salt))


def _relative_source_path(db, media_file: MediaFile) -> Path:
    source_path = Path(media_file.path)
    source_location = db.get(StorageLocation, media_file.storage_location_id) if media_file.storage_location_id else None
    try:
        return source_path.relative_to(Path(source_location.path)) if source_location else Path(media_file.filename)
    except ValueError:
        return Path(media_file.filename)


def _get_cloud_storage_config(db) -> CloudStorageConfig | None:
    return db.query(CloudStorageConfig).first()


def _parse_gcs_path(destination_path: str) -> tuple[str, str]:
    """"gcs://bucket/key-or-prefix" -> ("bucket", "key-or-prefix")."""
    rest = destination_path[len("gcs://") :]
    bucket, _, key = rest.partition("/")
    return bucket, key


def _stage_gcs_encrypted_backup(cloud_config: CloudStorageConfig, bucket_name: str, path_id: str, tmp_dir: Path) -> None:
    """Downloads path_id's manifest + chunk objects from GCS into tmp_dir, so the
    existing local-directory decrypt_file/backup_exists helpers work unchanged."""
    tmp_dir.mkdir(parents=True, exist_ok=True)
    manifest_name = f"{path_id}.manifest.json"
    gcs.download_file(
        cloud_config.service_account_json, bucket_name, manifest_name, tmp_dir / manifest_name, cloud_config.project_id
    )
    manifest = json.loads((tmp_dir / manifest_name).read_text())
    for index in range(manifest["chunk_count"]):
        chunk_name = f"{path_id}.{index:06d}.chunk"
        gcs.download_file(
            cloud_config.service_account_json, bucket_name, chunk_name, tmp_dir / chunk_name, cloud_config.project_id
        )


def _make_progress_cb(db, job: CopyJob):
    last_committed = {"n": 0}

    def progress_cb(copied: int, total: int) -> None:
        job.total_bytes = total
        if copied - last_committed["n"] >= PROGRESS_COMMIT_INTERVAL or copied == total:
            job.progress_bytes = copied
            db.commit()
            last_committed["n"] = copied

    return progress_cb


def _run_plain_copy(db, job: CopyJob, source_path: Path, dest_path: Path) -> None:
    if not source_path.is_file():
        raise FileNotFoundError(f"source file missing: {source_path}")
    if dest_path == source_path:
        raise ValueError("source and destination resolve to the same path")

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


# --- Split/clump support -----------------------------------------------------
#
# Both features reduce to the same two primitives: materialize a fully-formed
# local plaintext file representing one "unit" (a whole file, one split part,
# or one clump's concatenated body), store it at the destination
# (_store_plaintext_as_archive), and later read a unit back
# (_materialize_archive). Splitting a file writes N archives - one wholly
# dedicated to each part - linked to one BackupRecord. Clumping several files
# writes one archive shared by several BackupRecords, each pointing at its own
# byte range within it (BackupRecordArchive.archive_offset/archive_length).


def _should_split(size_bytes: int, config: TransferConfig | None) -> bool:
    if not config or not config.clump_split_enabled:
        return False
    max_bytes = config.max_size_gb * (1024**3)
    return size_bytes > max_bytes * (1 + config.split_over_percent / 100)


def _split_part_lengths(size_bytes: int, max_size_gb: float) -> list[int]:
    max_bytes = max(1, int(max_size_gb * (1024**3)))
    parts = []
    remaining = size_bytes
    while remaining > 0:
        part = min(max_bytes, remaining)
        parts.append(part)
        remaining -= part
    return parts


def _copy_range(source_path: Path, dest_path: Path, offset: int, length: int) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with source_path.open("rb") as src, dest_path.open("wb") as dst:
        src.seek(offset)
        remaining = length
        while remaining > 0:
            chunk = src.read(min(CHUNK_SIZE, remaining))
            if not chunk:
                break
            dst.write(chunk)
            remaining -= len(chunk)


def _store_plaintext_as_archive(
    db, other_location: StorageLocation, master_key: bytes | None, local_plain_path: Path, encryption_unit_id: str, plain_dest_name: str
) -> tuple[str, bool]:
    """Stores a fully-materialized local plaintext file (a whole file, a split
    part, or a clump body) at the destination, encrypting first if master_key
    is set. Returns (archive_path, encrypted). encryption_unit_id derives the
    path_id/file_key when encrypting - the caller's job is to make sure it's
    unique per archive; plain_dest_name only matters when NOT encrypting
    (encrypted backups are always flattened to path_id, never a plaintext name).
    """
    if master_key:
        file_key = derive_file_key(master_key, encryption_unit_id)
        path_id = derive_path_id(master_key, encryption_unit_id)
        if other_location.location_type == "gcs":
            cloud_config = _get_cloud_storage_config(db)
            if not cloud_config or not cloud_config.service_account_json or not cloud_config.bucket_name:
                raise RuntimeError("cloud storage isn't configured")
            tmp_dir = Path(f"/tmp/mediabridge-encrypt-{path_id}")
            try:
                encrypt_file(local_plain_path, tmp_dir, file_key, path_id)
                # A re-backup with fewer chunks than last time would otherwise
                # leave the old run's trailing chunk objects (and its old
                # manifest) orphaned in the bucket - clear this path_id's
                # objects before uploading the fresh set.
                gcs.delete_blobs_with_prefix(
                    cloud_config.service_account_json, cloud_config.bucket_name, f"{path_id}.", cloud_config.project_id
                )
                throttle = gcs.upload_mbps_to_throttle_bytes_per_sec(cloud_config.upload_mbps)
                for local_file in sorted(tmp_dir.glob(f"{path_id}*")):
                    gcs.upload_file(
                        cloud_config.service_account_json,
                        cloud_config.bucket_name,
                        local_file.name,
                        local_file,
                        cloud_config.project_id,
                        max_bytes_per_sec=throttle,
                    )
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            return f"gcs://{cloud_config.bucket_name}/{path_id}", True

        dest_dir = Path(other_location.path)
        encrypt_file(local_plain_path, dest_dir, file_key, path_id)
        return str(dest_dir / path_id), True

    if other_location.location_type == "gcs":
        cloud_config = _get_cloud_storage_config(db)
        if not cloud_config or not cloud_config.service_account_json or not cloud_config.bucket_name:
            raise RuntimeError("cloud storage isn't configured")
        gcs.upload_file(
            cloud_config.service_account_json,
            cloud_config.bucket_name,
            plain_dest_name,
            local_plain_path,
            cloud_config.project_id,
            max_bytes_per_sec=gcs.upload_mbps_to_throttle_bytes_per_sec(cloud_config.upload_mbps),
        )
        return f"gcs://{cloud_config.bucket_name}/{plain_dest_name}", False

    dest_path = Path(other_location.path) / plain_dest_name
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_name(dest_path.name + ".mbcopy")
    shutil.copyfile(local_plain_path, tmp_path)
    tmp_path.replace(dest_path)
    return str(dest_path), False


def _materialize_archive(db, archive: BackupArchive, media_file: MediaFile, tag: str) -> Path:
    """Downloads/decrypts an entire BackupArchive to a local plaintext temp file
    and returns its path - the read-side counterpart of
    _store_plaintext_as_archive. Caller must delete the returned path."""
    is_gcs = archive.path.startswith("gcs://")
    # Unique per call, not just per archive - concurrent jobs commonly share one
    # archive (every member of a clump verifies/restores against the same
    # BackupArchive), and reusing a path across them raced and corrupted each
    # other's staged files.
    invocation_id = secrets.token_hex(4)
    tmp_path = Path(f"/tmp/mediabridge-{tag}-{archive.id}-{invocation_id}.tmp")

    if archive.encrypted:
        master_key = _get_master_key(db)
        if not master_key:
            raise RuntimeError("backup is encrypted but no backup password is configured")
        unit_id = archive.encryption_key_id or media_file.uuid
        file_key = derive_file_key(master_key, unit_id)

        if is_gcs:
            cloud_config = _get_cloud_storage_config(db)
            if not cloud_config or not cloud_config.service_account_json:
                raise RuntimeError("cloud storage isn't configured")
            bucket_name, path_id = _parse_gcs_path(archive.path)
            stage_dir = Path(f"/tmp/mediabridge-{tag}-stage-{archive.id}-{invocation_id}")
            try:
                _stage_gcs_encrypted_backup(cloud_config, bucket_name, path_id, stage_dir)
                if not backup_exists(stage_dir, path_id):
                    raise FileNotFoundError(f"encrypted backup files missing: {archive.path}")
                decrypt_file(stage_dir, file_key, path_id, tmp_path)
            finally:
                shutil.rmtree(stage_dir, ignore_errors=True)
        else:
            prefix = Path(archive.path)
            dest_dir, path_id = prefix.parent, prefix.name
            if not backup_exists(dest_dir, path_id):
                raise FileNotFoundError(f"encrypted backup files missing: {archive.path}")
            decrypt_file(dest_dir, file_key, path_id, tmp_path)
    elif is_gcs:
        cloud_config = _get_cloud_storage_config(db)
        if not cloud_config or not cloud_config.service_account_json:
            raise RuntimeError("cloud storage isn't configured")
        bucket_name, object_name = _parse_gcs_path(archive.path)
        if not gcs.blob_exists(cloud_config.service_account_json, bucket_name, object_name, cloud_config.project_id):
            raise FileNotFoundError(f"backup object missing: {archive.path}")
        gcs.download_file(cloud_config.service_account_json, bucket_name, object_name, tmp_path, cloud_config.project_id)
    else:
        backup_path = Path(archive.path)
        if not backup_path.is_file():
            raise FileNotFoundError(f"backup file missing: {archive.path}")
        shutil.copyfile(backup_path, tmp_path)

    return tmp_path


def _backup_split(db, job: CopyJob, media_file: MediaFile, other_location: StorageLocation, master_key: bytes | None, max_size_gb: float, source_path: Path, size_bytes: int) -> None:
    """A file too big for one archive gets split into N parts, each stored as
    its own independent, self-contained archive (own encryption key/path_id if
    encrypted) - see the module docstring above. Linked to one BackupRecord via
    N BackupRecordArchive rows (part_index 0..N-1) so restore/verify can
    reconstruct them in order."""
    part_lengths = _split_part_lengths(size_bytes, max_size_gb)
    job.total_bytes = size_bytes
    db.commit()

    record = (
        db.query(BackupRecord)
        .filter_by(media_file_id=media_file.id, destination_storage_location_id=other_location.id)
        .one_or_none()
    )
    if not record:
        record = BackupRecord(media_file_id=media_file.id, destination_storage_location_id=other_location.id)
        db.add(record)
    record.local_path = media_file.path
    record.local_checksum = media_file.fingerprint
    record.status = "done"
    record.verify_status = None
    record.verified_at = None
    db.flush()
    db.query(BackupRecordArchive).filter_by(backup_record_id=record.id).delete()

    offset = 0
    copied_total = 0
    encrypted = bool(master_key)
    for part_index, part_length in enumerate(part_lengths):
        tmp_part = Path(f"/tmp/mediabridge-split-{job.id}-{part_index}.tmp")
        try:
            _copy_range(source_path, tmp_part, offset, part_length)
            unit_id = f"{media_file.uuid}:part{part_index}"
            dest_name = f"{media_file.filename}.part{part_index:03d}"
            archive_path, encrypted = _store_plaintext_as_archive(db, other_location, master_key, tmp_part, unit_id, dest_name)
        finally:
            tmp_part.unlink(missing_ok=True)

        archive = BackupArchive(
            storage_location_id=other_location.id,
            path=archive_path,
            size_bytes=part_length,
            encrypted=encrypted,
            is_clump=False,
            # Per-part checksums aren't tracked separately - encrypted parts are
            # covered by AES-GCM's own auth tags, and plain parts are covered by
            # verify's whole-file reconstruction against media_file.fingerprint.
            checksum=None,
            encryption_key_id=unit_id if encrypted else None,
        )
        db.add(archive)
        db.flush()
        db.add(
            BackupRecordArchive(
                backup_record_id=record.id,
                backup_archive_id=archive.id,
                part_index=part_index,
                archive_offset=0,
                archive_length=part_length,
            )
        )

        offset += part_length
        copied_total += part_length
        job.progress_bytes = copied_total
        db.commit()

    job.destination_path = f"split://{media_file.uuid}"  # sentinel - real locations live in the ledger
    job.encrypted = encrypted


def _restore_from_parts(db, job: CopyJob, media_file: MediaFile, parts: list[tuple[BackupRecordArchive, BackupArchive]], output_path: Path) -> None:
    """Reconstructs output_path from ledger parts, in part_index order. Handles
    all three shapes uniformly: a normal single-archive backup (one part
    spanning the whole archive), a split file (several dedicated archives), and
    one member of a clump (one part sliced out of a shared archive)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_final = output_path.with_name(output_path.name + ".mbcopy")
    total_length = sum(bra.archive_length for bra, _ in parts)
    job.total_bytes = total_length
    db.commit()

    copied = 0
    with tmp_final.open("wb") as out:
        for bra, archive in parts:
            slice_path = _materialize_archive(db, archive, media_file, "restore")
            try:
                with slice_path.open("rb") as sf:
                    sf.seek(bra.archive_offset)
                    remaining = bra.archive_length
                    while remaining > 0:
                        chunk = sf.read(min(CHUNK_SIZE, remaining))
                        if not chunk:
                            break
                        out.write(chunk)
                        remaining -= len(chunk)
                        copied += len(chunk)
                job.progress_bytes = copied
                db.commit()
            finally:
                slice_path.unlink(missing_ok=True)

    tmp_final.replace(output_path)
    job.destination_path = str(output_path)
    job.encrypted = parts[0][1].encrypted


@app.task(bind=True, name="copy_media_file")
def copy_media_file(self, job_id: int) -> None:
    db = SessionLocal()
    try:
        job = db.get(CopyJob, job_id)
        if not job:
            return

        job.status = "running"
        job.celery_task_id = self.request.id
        job.progress_bytes = 0
        job.next_retry_at = None
        db.commit()

        media_file = db.get(MediaFile, job.media_file_id)
        other_location = db.get(StorageLocation, job.destination_storage_location_id)
        if not media_file or not other_location:
            raise RuntimeError("source file or destination location no longer exists")

        if job.job_type == "backup":
            source_path = Path(media_file.path)
            if not source_path.is_file():
                raise FileNotFoundError(f"source file missing: {source_path}")

            master_key = _get_master_key(db)
            transfer_config = db.query(TransferConfig).first()
            size_bytes = source_path.stat().st_size

            if _should_split(size_bytes, transfer_config):
                _backup_split(db, job, media_file, other_location, master_key, transfer_config.max_size_gb, source_path, size_bytes)
            elif other_location.location_type == "gcs":
                cloud_config = _get_cloud_storage_config(db)
                if not cloud_config or not cloud_config.service_account_json or not cloud_config.bucket_name:
                    raise RuntimeError("cloud storage isn't configured")

                if master_key:
                    # Encrypt to a local temp dir (reusing the same encrypt_file used for
                    # local backups), then upload the resulting chunk/manifest files as
                    # objects under their own filenames, then discard the temp dir.
                    file_key = derive_file_key(master_key, media_file.uuid)
                    path_id = derive_path_id(master_key, media_file.uuid)
                    tmp_dir = Path(f"/tmp/mediabridge-cloud-{job.id}")
                    try:
                        encrypt_file(source_path, tmp_dir, file_key, path_id, progress_cb=_make_progress_cb(db, job))
                        throttle = gcs.upload_mbps_to_throttle_bytes_per_sec(cloud_config.upload_mbps)
                        for local_file in sorted(tmp_dir.glob(f"{path_id}*")):
                            gcs.upload_file(
                                cloud_config.service_account_json,
                                cloud_config.bucket_name,
                                local_file.name,
                                local_file,
                                cloud_config.project_id,
                                max_bytes_per_sec=throttle,
                            )
                    finally:
                        shutil.rmtree(tmp_dir, ignore_errors=True)
                    job.destination_path = f"gcs://{cloud_config.bucket_name}/{path_id}"
                    job.encrypted = True
                else:
                    rel_path = _relative_source_path(db, media_file)
                    object_name = str(rel_path).replace("\\", "/")
                    job.total_bytes = source_path.stat().st_size
                    db.commit()
                    gcs.upload_file(
                        cloud_config.service_account_json,
                        cloud_config.bucket_name,
                        object_name,
                        source_path,
                        cloud_config.project_id,
                        max_bytes_per_sec=gcs.upload_mbps_to_throttle_bytes_per_sec(cloud_config.upload_mbps),
                    )
                    job.progress_bytes = job.total_bytes
                    job.destination_path = f"gcs://{cloud_config.bucket_name}/{object_name}"
                    job.encrypted = False
            elif master_key:
                # Encrypted: flatten into <backup_root>/<path_id>.NNNNNN.chunk -
                # no genre subfolder, no original filename anywhere on disk.
                file_key = derive_file_key(master_key, media_file.uuid)
                path_id = derive_path_id(master_key, media_file.uuid)
                dest_dir = Path(other_location.path)
                encrypt_file(source_path, dest_dir, file_key, path_id, progress_cb=_make_progress_cb(db, job))
                job.destination_path = str(dest_dir / path_id)
                job.encrypted = True
            else:
                rel_path = _relative_source_path(db, media_file)
                dest_path = Path(other_location.path) / rel_path
                _run_plain_copy(db, job, source_path, dest_path)
                job.encrypted = False

            if not job.destination_path.startswith("split://"):
                _record_backup(db, media_file, other_location, job)

        elif job.job_type == "restore":
            output_path = Path(media_file.path)
            parts = _ledger_parts(db, job.media_file_id, other_location.id)

            if parts:
                _restore_from_parts(db, job, media_file, parts, output_path)
            else:
                # Legacy pre-ledger backup (made before the BackupRecord ledger
                # existed) - fall back to CopyJob's own append-only log.
                existing_backup = _latest_done_backup(db, job.media_file_id, other_location.id)
                if not existing_backup or not existing_backup.destination_path:
                    raise RuntimeError("no completed backup found to restore from")

                is_gcs = existing_backup.destination_path.startswith("gcs://")

                if existing_backup.encrypted:
                    master_key = _get_master_key(db)
                    if not master_key:
                        raise RuntimeError("backup is encrypted but no backup password is configured")
                    file_key = derive_file_key(master_key, media_file.uuid)

                    if is_gcs:
                        cloud_config = _get_cloud_storage_config(db)
                        if not cloud_config or not cloud_config.service_account_json:
                            raise RuntimeError("cloud storage isn't configured")
                        bucket_name, path_id = _parse_gcs_path(existing_backup.destination_path)
                        tmp_dir = Path(f"/tmp/mediabridge-cloud-restore-{job.id}")
                        try:
                            _stage_gcs_encrypted_backup(cloud_config, bucket_name, path_id, tmp_dir)
                            decrypt_file(tmp_dir, file_key, path_id, output_path, progress_cb=_make_progress_cb(db, job))
                        finally:
                            shutil.rmtree(tmp_dir, ignore_errors=True)
                    else:
                        prefix = Path(existing_backup.destination_path)
                        dest_dir, path_id = prefix.parent, prefix.name
                        if not backup_exists(dest_dir, path_id):
                            raise FileNotFoundError(f"encrypted backup files missing: {prefix}")
                        decrypt_file(dest_dir, file_key, path_id, output_path, progress_cb=_make_progress_cb(db, job))
                    job.destination_path = str(output_path)
                elif is_gcs:
                    cloud_config = _get_cloud_storage_config(db)
                    if not cloud_config or not cloud_config.service_account_json:
                        raise RuntimeError("cloud storage isn't configured")
                    bucket_name, object_name = _parse_gcs_path(existing_backup.destination_path)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    tmp_path = output_path.with_name(output_path.name + ".mbcopy")
                    gcs.download_file(
                        cloud_config.service_account_json, bucket_name, object_name, tmp_path, cloud_config.project_id
                    )
                    tmp_path.replace(output_path)
                    job.destination_path = str(output_path)
                    job.total_bytes = job.progress_bytes = output_path.stat().st_size
                else:
                    source_path = Path(existing_backup.destination_path)
                    _run_plain_copy(db, job, source_path, output_path)

        job.status = "done"
        job.completed_at = datetime.now(timezone.utc)
        db.commit()
    except (Exception, InvalidTag) as exc:
        db.rollback()
        job = db.get(CopyJob, job_id)
        message = str(exc) or "decryption failed (wrong password or corrupted backup)"

        if job and job.job_type in RETRYABLE_JOB_TYPES:
            config = db.query(TransferConfig).first()
            retry_limit = config.retry_count if config else 1
            if self.request.retries < retry_limit:
                job.status = "retrying"
                job.error_message = message[:2000]
                db.commit()
                raise self.retry(exc=exc, countdown=RETRY_BACKOFF_SECONDS, max_retries=retry_limit)

            interval_minutes = config.retry_interval_minutes if config else 60
            job.next_retry_at = datetime.now(timezone.utc) + timedelta(minutes=interval_minutes)

        if job:
            _fail(db, job, message)
        raise
    finally:
        db.close()


@app.task(bind=True, name="backup_clump")
def backup_clump(self, job_ids: list[int]) -> None:
    """Backs up several small files as one shared archive instead of N separate
    ones - triggered by /movies/bulk-backup when clump_split_enabled and a
    batch of selected files are all under the clump size threshold (see
    web/app/main.py). Each file keeps its own CopyJob row (so status/progress
    still show per-file in the catalog); they just all point at one physical
    archive, sliced by byte range (BackupRecordArchive.archive_offset/length).

    Unlike copy_media_file, this isn't retried automatically yet - a failure
    here fails every job in the batch and needs a manual re-backup. Retrying a
    partially-written multi-file group safely is more involved than the
    single-file case and wasn't built out in this pass.
    """
    db = SessionLocal()
    jobs: list[CopyJob] = []
    try:
        jobs = [j for j in (db.get(CopyJob, jid) for jid in job_ids) if j]
        if not jobs:
            return

        other_location = db.get(StorageLocation, jobs[0].destination_storage_location_id)
        if not other_location:
            raise RuntimeError("destination location no longer exists")

        members: list[tuple[CopyJob, MediaFile]] = []
        for j in jobs:
            j.status = "running"
            j.celery_task_id = self.request.id
            j.progress_bytes = 0
            j.next_retry_at = None
            media_file = db.get(MediaFile, j.media_file_id)
            if not media_file or not Path(media_file.path).is_file():
                raise FileNotFoundError(f"source file missing for media_file_id={j.media_file_id}")
            members.append((j, media_file))
        db.commit()

        master_key = _get_master_key(db)
        clump_id = secrets.token_hex(16)
        tmp_body = Path(f"/tmp/mediabridge-clump-{clump_id}.tmp")
        member_ranges: list[tuple[CopyJob, MediaFile, int, int]] = []

        try:
            offset = 0
            with tmp_body.open("wb") as out:
                for j, media_file in members:
                    length = media_file.size_bytes
                    with Path(media_file.path).open("rb") as src:
                        while True:
                            chunk = src.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            out.write(chunk)
                    member_ranges.append((j, media_file, offset, length))
                    offset += length
            total_bytes = offset

            for j, _media_file, _offset, _length in member_ranges:
                j.total_bytes = total_bytes
            db.commit()

            dest_name = f"clump-{clump_id}.bin"
            archive_path, encrypted = _store_plaintext_as_archive(db, other_location, master_key, tmp_body, clump_id, dest_name)
        finally:
            tmp_body.unlink(missing_ok=True)

        archive = BackupArchive(
            storage_location_id=other_location.id,
            path=archive_path,
            size_bytes=total_bytes,
            encrypted=encrypted,
            is_clump=True,
            checksum=None,
            encryption_key_id=clump_id if encrypted else None,
        )
        db.add(archive)
        db.flush()

        now = datetime.now(timezone.utc)
        for j, media_file, member_offset, member_length in member_ranges:
            record = (
                db.query(BackupRecord)
                .filter_by(media_file_id=media_file.id, destination_storage_location_id=other_location.id)
                .one_or_none()
            )
            if not record:
                record = BackupRecord(media_file_id=media_file.id, destination_storage_location_id=other_location.id)
                db.add(record)
            record.local_path = media_file.path
            record.local_checksum = media_file.fingerprint
            record.status = "done"
            record.verify_status = None
            record.verified_at = None
            db.flush()
            db.query(BackupRecordArchive).filter_by(backup_record_id=record.id).delete()
            db.add(
                BackupRecordArchive(
                    backup_record_id=record.id,
                    backup_archive_id=archive.id,
                    part_index=0,
                    archive_offset=member_offset,
                    archive_length=member_length,
                )
            )

            j.destination_path = f"clump://{clump_id}"
            j.encrypted = encrypted
            j.progress_bytes = j.total_bytes
            j.status = "done"
            j.completed_at = now
        db.commit()
    except (Exception, InvalidTag) as exc:
        db.rollback()
        message = str(exc) or "clump backup failed"
        for j in jobs:
            fresh = db.get(CopyJob, j.id)
            if fresh:
                _fail(db, fresh, message)
        raise
    finally:
        db.close()


@app.task(name="verify_copy_job")
def verify_copy_job(job_id: int) -> None:
    db = SessionLocal()
    try:
        job = db.get(CopyJob, job_id)
        if not job or job.job_type != "backup" or job.status != "done":
            return

        media_file = db.get(MediaFile, job.media_file_id)
        parts = _ledger_parts(db, job.media_file_id, job.destination_storage_location_id)

        if parts:
            tmp_final = Path(f"/tmp/mediabridge-verify-{job.id}.tmp")
            status = None
            try:
                with tmp_final.open("wb") as out:
                    for bra, archive in parts:
                        try:
                            slice_path = _materialize_archive(db, archive, media_file, "verify")
                        except InvalidTag:
                            status = "mismatch"
                            break
                        except Exception:
                            status = "missing"
                            break
                        try:
                            with slice_path.open("rb") as sf:
                                sf.seek(bra.archive_offset)
                                remaining = bra.archive_length
                                while remaining > 0:
                                    chunk = sf.read(min(CHUNK_SIZE, remaining))
                                    if not chunk:
                                        break
                                    out.write(chunk)
                                    remaining -= len(chunk)
                        finally:
                            slice_path.unlink(missing_ok=True)

                if status is None:
                    if media_file and media_file.fingerprint:
                        backup_fingerprint = compute_fingerprint(tmp_final, tmp_final.stat().st_size)
                        status = "match" if backup_fingerprint == media_file.fingerprint else "mismatch"
                    else:
                        status = "mismatch"
                job.verify_status = status
            finally:
                tmp_final.unlink(missing_ok=True)
        elif not job.destination_path:
            return
        else:
            # Legacy pre-ledger backup.
            is_gcs = job.destination_path.startswith("gcs://")

            if job.encrypted:
                master_key = _get_master_key(db)

                if is_gcs:
                    cloud_config = _get_cloud_storage_config(db)
                    tmp_dir = Path(f"/tmp/mediabridge-cloud-verify-{job.id}")
                    staged = False
                    if master_key and cloud_config and cloud_config.service_account_json:
                        bucket_name, path_id = _parse_gcs_path(job.destination_path)
                        try:
                            _stage_gcs_encrypted_backup(cloud_config, bucket_name, path_id, tmp_dir)
                            staged = backup_exists(tmp_dir, path_id)
                        except Exception:
                            staged = False

                    if not staged:
                        job.verify_status = "missing"
                        shutil.rmtree(tmp_dir, ignore_errors=True)
                    else:
                        tmp_path = Path(f"/tmp/mediabridge-verify-{job.id}.tmp")
                        try:
                            file_key = derive_file_key(master_key, media_file.uuid)
                            decrypt_file(tmp_dir, file_key, path_id, tmp_path)
                            backup_fingerprint = compute_fingerprint(tmp_path, tmp_path.stat().st_size)
                            if media_file and media_file.fingerprint and backup_fingerprint == media_file.fingerprint:
                                job.verify_status = "match"
                            else:
                                job.verify_status = "mismatch"
                        except InvalidTag:
                            job.verify_status = "mismatch"
                        finally:
                            tmp_path.unlink(missing_ok=True)
                            shutil.rmtree(tmp_dir, ignore_errors=True)
                else:
                    prefix = Path(job.destination_path)
                    dest_dir, path_id = prefix.parent, prefix.name

                    if not master_key or not backup_exists(dest_dir, path_id):
                        job.verify_status = "missing"
                    else:
                        tmp_path = Path(f"/tmp/mediabridge-verify-{job.id}.tmp")
                        try:
                            file_key = derive_file_key(master_key, media_file.uuid)
                            decrypt_file(dest_dir, file_key, path_id, tmp_path)
                            backup_fingerprint = compute_fingerprint(tmp_path, tmp_path.stat().st_size)
                            if media_file and media_file.fingerprint and backup_fingerprint == media_file.fingerprint:
                                job.verify_status = "match"
                            else:
                                job.verify_status = "mismatch"
                        except InvalidTag:
                            job.verify_status = "mismatch"
                        finally:
                            tmp_path.unlink(missing_ok=True)
            elif is_gcs:
                cloud_config = _get_cloud_storage_config(db)
                bucket_name, object_name = _parse_gcs_path(job.destination_path)
                exists = (
                    cloud_config
                    and cloud_config.service_account_json
                    and gcs.blob_exists(cloud_config.service_account_json, bucket_name, object_name, cloud_config.project_id)
                )
                if not exists:
                    job.verify_status = "missing"
                elif media_file and media_file.fingerprint:
                    tmp_path = Path(f"/tmp/mediabridge-verify-{job.id}.tmp")
                    try:
                        gcs.download_file(
                            cloud_config.service_account_json, bucket_name, object_name, tmp_path, cloud_config.project_id
                        )
                        backup_fingerprint = compute_fingerprint(tmp_path, tmp_path.stat().st_size)
                        job.verify_status = "match" if backup_fingerprint == media_file.fingerprint else "mismatch"
                    finally:
                        tmp_path.unlink(missing_ok=True)
                else:
                    job.verify_status = "mismatch"
            else:
                backup_path = Path(job.destination_path)
                if not backup_path.is_file():
                    job.verify_status = "missing"
                elif media_file and media_file.fingerprint:
                    backup_fingerprint = compute_fingerprint(backup_path, backup_path.stat().st_size)
                    job.verify_status = "match" if backup_fingerprint == media_file.fingerprint else "mismatch"
                else:
                    job.verify_status = "mismatch"

        job.verified_at = datetime.now(timezone.utc)

        record = (
            db.query(BackupRecord)
            .filter_by(media_file_id=job.media_file_id, destination_storage_location_id=job.destination_storage_location_id)
            .one_or_none()
        )
        if record:
            record.verify_status = job.verify_status
            record.verified_at = job.verified_at

        db.commit()
    finally:
        db.close()


@app.task(name="retry_failed_transfers")
def retry_failed_transfers() -> None:
    """Celery Beat runs this every few minutes (see celery_app.py). Re-queues any
    backup/restore job that exhausted its immediate retries and whose
    next_retry_at has now passed - the "come back after an hour" half of the
    retry setting (copy_media_file's own self.retry handles the immediate half).
    """
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        due_jobs = (
            db.query(CopyJob)
            .filter(
                CopyJob.status == "failed",
                CopyJob.job_type.in_(RETRYABLE_JOB_TYPES),
                CopyJob.next_retry_at.isnot(None),
                CopyJob.next_retry_at <= now,
            )
            .all()
        )
        for job in due_jobs:
            job.status = "pending"
            job.next_retry_at = None
            job.error_message = None
            db.commit()
            copy_media_file.apply_async(args=[job.id], queue="copy")
    finally:
        db.close()
