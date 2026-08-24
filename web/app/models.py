import uuid as uuid_lib
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class StorageLocation(Base):
    __tablename__ = "storage_locations"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)
    # "local" is the only supported type today; the column exists so remote
    # storage backends can be added later without a schema change.
    location_type: Mapped[str] = mapped_column(String, default="local")
    path: Mapped[str] = mapped_column(String, unique=True)
    # At most one location is the backup target at a time; Catalog uses it to
    # show per-file backup status and offer a one-click "Back up" action.
    is_backup_target: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MediaFile(Base):
    __tablename__ = "media_files"

    id: Mapped[int] = mapped_column(primary_key=True)
    uuid: Mapped[str] = mapped_column(String, unique=True, index=True, default=lambda: str(uuid_lib.uuid4()))
    path: Mapped[str] = mapped_column(String, unique=True, index=True)
    filename: Mapped[str] = mapped_column(String, index=True)
    extension: Mapped[str] = mapped_column(String)
    genre: Mapped[Optional[str]] = mapped_column(String, index=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    media_type: Mapped[str] = mapped_column(String, default="movie")
    storage_location_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("storage_locations.id", ondelete="CASCADE"), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    scanned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Content fingerprint (size + first/last ~1MiB, BLAKE2b) - identifies a file's
    # content independent of its path, without hashing multi-GB files in full.
    fingerprint: Mapped[Optional[str]] = mapped_column(String, index=True)

    # Enrichment parsed from a sibling .nfo sidecar file, when present.
    title: Mapped[Optional[str]] = mapped_column(String)
    overview: Mapped[Optional[str]] = mapped_column(Text)
    year: Mapped[Optional[int]] = mapped_column(Integer)
    imdb_id: Mapped[Optional[str]] = mapped_column(String)
    tmdb_id: Mapped[Optional[str]] = mapped_column(String)
    rating: Mapped[Optional[float]] = mapped_column(Float)
    runtime_minutes: Mapped[Optional[int]] = mapped_column(Integer)

    # Watch data synced from Jellyfin, for the one user configured in JellyfinConfig.
    jellyfin_item_id: Mapped[Optional[str]] = mapped_column(String, index=True)
    jellyfin_library: Mapped[Optional[str]] = mapped_column(String, index=True)
    watched: Mapped[bool] = mapped_column(Boolean, default=False)
    play_count: Mapped[int] = mapped_column(Integer, default=0)
    last_played_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class CopyJob(Base):
    __tablename__ = "copy_jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    media_file_id: Mapped[int] = mapped_column(ForeignKey("media_files.id", ondelete="CASCADE"))
    destination_storage_location_id: Mapped[int] = mapped_column(
        ForeignKey("storage_locations.id", ondelete="CASCADE")
    )
    destination_path: Mapped[Optional[str]] = mapped_column(String)
    # "backup": media_file.path -> destination_storage_location. "restore": the
    # reverse - the latest done "backup" job's file -> media_file.path.
    job_type: Mapped[str] = mapped_column(String, default="backup")
    status: Mapped[str] = mapped_column(String, default="pending")
    progress_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    total_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    celery_task_id: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Set by re-hashing the backup copy and comparing to media_files.fingerprint.
    # Only meaningful on job_type="backup" rows.
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    verify_status: Mapped[Optional[str]] = mapped_column(String)  # "match" | "mismatch" | "missing"

    # Whether this specific job's backup was written encrypted. Fixed at backup
    # time, independent of BackupEncryptionConfig.enabled later changing, so
    # restore/verify always know unambiguously how to read this job's files.
    encrypted: Mapped[bool] = mapped_column(Boolean, default=False)

    # Set once a backup/restore job exhausts its immediate retries and is
    # parked as "failed" - the hourly retry_failed_transfers sweep re-queues
    # it once this time passes. Null otherwise (including while retrying).
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class BackupArchive(Base):
    """A single physical file that exists on a backup destination. Usually holds
    exactly one MediaFile's data (see BackupRecordArchive below), but a clump
    holds several files' data packed together, and one very large file's data
    can span several archives (a split)."""

    __tablename__ = "backup_archives"

    id: Mapped[int] = mapped_column(primary_key=True)
    storage_location_id: Mapped[int] = mapped_column(ForeignKey("storage_locations.id", ondelete="CASCADE"))
    path: Mapped[str] = mapped_column(String)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    # Fingerprint of the archive's own bytes, for detecting bit rot on the backup
    # destination independent of source-file comparisons. Not meaningful for
    # encrypted archives - AES-GCM's per-chunk auth tags already guarantee
    # ciphertext integrity on decrypt, so this is left null there.
    checksum: Mapped[Optional[str]] = mapped_column(String)
    encrypted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_clump: Mapped[bool] = mapped_column(Boolean, default=False)
    # The string used to derive this archive's file_key/path_id, when it isn't
    # simply the owning MediaFile's uuid - a split part ("<uuid>:partN") or a
    # clump (a synthetic id shared by every file bundled into it, since a clump
    # isn't "owned" by any single file). Null means "derive from the
    # MediaFile's uuid directly", preserving the original single-file behavior.
    encryption_key_id: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BackupRecord(Base):
    """The current backup state of one (MediaFile, destination) pair - one row
    per file per backup destination, updated in place on every successful
    backup to that destination. Unlike CopyJob (an append-only log of every
    individual backup/restore/verify operation), this is always just "what's
    the latest backup situation for this file at this destination right now".
    A file backed up to both a local target and the cloud archive gets two
    independent rows, so restoring/verifying one destination is unaffected by
    the other."""

    __tablename__ = "backup_records"
    __table_args__ = (
        UniqueConstraint("media_file_id", "destination_storage_location_id", name="uq_backup_record_media_file_destination"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    media_file_id: Mapped[int] = mapped_column(ForeignKey("media_files.id", ondelete="CASCADE"), index=True)
    destination_storage_location_id: Mapped[int] = mapped_column(
        ForeignKey("storage_locations.id", ondelete="CASCADE"), index=True
    )
    # Snapshots of the source file as of this backup, kept even if media_files
    # later changes - local_checksum can be compared against the live
    # MediaFile.fingerprint to tell whether the source has drifted since.
    local_path: Mapped[str] = mapped_column(String)
    local_checksum: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="done")
    verify_status: Mapped[Optional[str]] = mapped_column(String)
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class BackupRecordArchive(Base):
    """Join table between BackupRecord and BackupArchive. Many-to-many so both
    directions are representable: one record spanning several archives (split)
    and one archive holding several records (clump). archive_offset/
    archive_length locate this record's bytes within the archive - for today's
    ordinary one-record-one-archive backups that's just the whole file."""

    __tablename__ = "backup_record_archives"

    id: Mapped[int] = mapped_column(primary_key=True)
    backup_record_id: Mapped[int] = mapped_column(ForeignKey("backup_records.id", ondelete="CASCADE"), index=True)
    backup_archive_id: Mapped[int] = mapped_column(ForeignKey("backup_archives.id", ondelete="CASCADE"), index=True)
    part_index: Mapped[int] = mapped_column(Integer, default=0)
    archive_offset: Mapped[int] = mapped_column(BigInteger, default=0)
    archive_length: Mapped[int] = mapped_column(BigInteger)


class BackupEncryptionConfig(Base):
    __tablename__ = "backup_encryption_config"

    id: Mapped[int] = mapped_column(primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    # Plaintext, like JellyfinConfig.api_key - stored so backups can run
    # unattended. This protects backups from anyone with access only to the
    # backup destination (e.g. untrusted remote/cloud storage), not from
    # anyone with access to this database.
    password: Mapped[Optional[str]] = mapped_column(String)
    # Salt for deriving the master key from the password (PBKDF2). Not secret,
    # just needs to be fixed once a password is set.
    kdf_salt: Mapped[Optional[str]] = mapped_column(String)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class TransferConfig(Base):
    __tablename__ = "transfer_config"

    id: Mapped[int] = mapped_column(primary_key=True)
    max_speed_mbps: Mapped[int] = mapped_column(Integer, default=100)
    # A file over max_size_gb * (1 + split_over_percent / 100) gets split into parts.
    max_size_gb: Mapped[float] = mapped_column(Float, default=2.0)
    split_over_percent: Mapped[int] = mapped_column(Integer, default=10)
    # A file under min_size_mb * (1 - clump_under_percent / 100) gets grouped with
    # other small files into one transfer unit instead of moved individually.
    min_size_mb: Mapped[float] = mapped_column(Float, default=200.0)
    clump_under_percent: Mapped[int] = mapped_column(Integer, default=10)
    # Whether the split/clump thresholds above are meant to apply. Off by
    # default; auto-enabled when a cloud storage bucket is set as the backup
    # target (see CloudStorageConfig.is_backup_target), user-toggleable too.
    clump_split_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    # How many times a failed backup/restore is immediately retried (short
    # backoff between attempts) before being parked as "failed" to wait for
    # the hourly sweep. 0 skips straight to the hourly cycle.
    retry_count: Mapped[int] = mapped_column(Integer, default=1)
    # How long a parked "failed" backup/restore waits before retry_failed_transfers
    # picks it back up.
    retry_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class CloudStorageConfig(Base):
    """Connection state for an optional cloud storage backend, set up through the
    Settings page's Cloud Storage workflow (upload service account key -> list
    buckets -> select one). Only Google Cloud Storage today; provider exists so
    other backends can be added later without a schema change. This is
    connection setup only - nothing transfers here yet, the same way
    TransferConfig's split/clump settings existed before the pipeline used them."""

    __tablename__ = "cloud_storage_config"

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String, default="gcs")
    # Full service account JSON key, as downloaded from GCP - stored plaintext,
    # like JellyfinConfig.api_key and BackupEncryptionConfig.password, so
    # scheduled transfers can run unattended later.
    service_account_json: Mapped[Optional[str]] = mapped_column(Text)
    service_account_email: Mapped[Optional[str]] = mapped_column(String)
    project_id: Mapped[Optional[str]] = mapped_column(String)
    # JSON-encoded list of bucket names from the last successful "List buckets" -
    # rendered as the picker in Settings. Null until that's been run once.
    available_buckets: Mapped[Optional[str]] = mapped_column(Text)
    bucket_name: Mapped[Optional[str]] = mapped_column(String)
    connected: Mapped[bool] = mapped_column(Boolean, default=False)
    # Mutually exclusive with StorageLocation.is_backup_target - at most one
    # backup target (local or cloud) is active at a time.
    is_backup_target: Mapped[bool] = mapped_column(Boolean, default=False)
    # Results of the last "Test connectivity & speed" run (gcs.test_connectivity_and_speed) - a real
    # upload/download of a throwaway blob, timed. Used to estimate backup duration.
    last_speed_test_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    upload_mbps: Mapped[Optional[float]] = mapped_column(Float)
    download_mbps: Mapped[Optional[float]] = mapped_column(Float)
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class JellyfinConfig(Base):
    __tablename__ = "jellyfin_config"

    id: Mapped[int] = mapped_column(primary_key=True)
    server_url: Mapped[str] = mapped_column(String)
    api_key: Mapped[str] = mapped_column(String)
    # Jellyfin's watch data is per-user; pick one Jellyfin account to sync against.
    sync_user_id: Mapped[Optional[str]] = mapped_column(String)
    sync_user_name: Mapped[Optional[str]] = mapped_column(String)
    # Same host folder, mounted at different paths in each container - swap one prefix for the other to match files.
    path_prefix_from: Mapped[str] = mapped_column(String, default="/media")
    path_prefix_to: Mapped[str] = mapped_column(String, default="/mediafiles")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
