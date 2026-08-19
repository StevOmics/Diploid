import uuid as uuid_lib
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, func
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
