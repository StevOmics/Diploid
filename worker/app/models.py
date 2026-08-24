"""
Mirrors the subset of MediaBridge's shared schema this service touches.
The web service (web/app/models.py) owns table creation; keep column
definitions here in sync with it by hand.
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class StorageLocation(Base):
    __tablename__ = "storage_locations"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)
    location_type: Mapped[str] = mapped_column(String, default="local")
    path: Mapped[str] = mapped_column(String, unique=True)
    is_backup_target: Mapped[bool] = mapped_column(Boolean, default=False)


class MediaFile(Base):
    __tablename__ = "media_files"

    id: Mapped[int] = mapped_column(primary_key=True)
    uuid: Mapped[str] = mapped_column(String, unique=True, index=True)
    path: Mapped[str] = mapped_column(String, unique=True, index=True)
    filename: Mapped[str] = mapped_column(String)
    storage_location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("storage_locations.id"))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    fingerprint: Mapped[Optional[str]] = mapped_column(String)


class BackupEncryptionConfig(Base):
    __tablename__ = "backup_encryption_config"

    id: Mapped[int] = mapped_column(primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    password: Mapped[Optional[str]] = mapped_column(String)
    kdf_salt: Mapped[Optional[str]] = mapped_column(String)


class CopyJob(Base):
    __tablename__ = "copy_jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    media_file_id: Mapped[int] = mapped_column(ForeignKey("media_files.id", ondelete="CASCADE"))
    destination_storage_location_id: Mapped[int] = mapped_column(
        ForeignKey("storage_locations.id", ondelete="CASCADE")
    )
    destination_path: Mapped[Optional[str]] = mapped_column(String)
    job_type: Mapped[str] = mapped_column(String, default="backup")
    status: Mapped[str] = mapped_column(String, default="pending")
    progress_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    total_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    celery_task_id: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    verify_status: Mapped[Optional[str]] = mapped_column(String)
    encrypted: Mapped[bool] = mapped_column(Boolean, default=False)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class BackupArchive(Base):
    __tablename__ = "backup_archives"

    id: Mapped[int] = mapped_column(primary_key=True)
    storage_location_id: Mapped[int] = mapped_column(ForeignKey("storage_locations.id", ondelete="CASCADE"))
    path: Mapped[str] = mapped_column(String)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    checksum: Mapped[Optional[str]] = mapped_column(String)
    encrypted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_clump: Mapped[bool] = mapped_column(Boolean, default=False)
    encryption_key_id: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BackupRecord(Base):
    __tablename__ = "backup_records"
    __table_args__ = (
        UniqueConstraint("media_file_id", "destination_storage_location_id", name="uq_backup_record_media_file_destination"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    media_file_id: Mapped[int] = mapped_column(ForeignKey("media_files.id", ondelete="CASCADE"), index=True)
    destination_storage_location_id: Mapped[int] = mapped_column(
        ForeignKey("storage_locations.id", ondelete="CASCADE"), index=True
    )
    local_path: Mapped[str] = mapped_column(String)
    local_checksum: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="done")
    verify_status: Mapped[Optional[str]] = mapped_column(String)
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class BackupRecordArchive(Base):
    __tablename__ = "backup_record_archives"

    id: Mapped[int] = mapped_column(primary_key=True)
    backup_record_id: Mapped[int] = mapped_column(ForeignKey("backup_records.id", ondelete="CASCADE"), index=True)
    backup_archive_id: Mapped[int] = mapped_column(ForeignKey("backup_archives.id", ondelete="CASCADE"), index=True)
    part_index: Mapped[int] = mapped_column(Integer, default=0)
    archive_offset: Mapped[int] = mapped_column(BigInteger, default=0)
    archive_length: Mapped[int] = mapped_column(BigInteger)


class TransferConfig(Base):
    __tablename__ = "transfer_config"

    id: Mapped[int] = mapped_column(primary_key=True)
    max_speed_mbps: Mapped[int] = mapped_column(Integer, default=100)
    max_size_gb: Mapped[float] = mapped_column(Float, default=2.0)
    split_over_percent: Mapped[int] = mapped_column(Integer, default=10)
    min_size_mb: Mapped[float] = mapped_column(Float, default=200.0)
    clump_under_percent: Mapped[int] = mapped_column(Integer, default=10)
    clump_split_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=1)
    retry_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class CloudStorageConfig(Base):
    __tablename__ = "cloud_storage_config"

    id: Mapped[int] = mapped_column(primary_key=True)
    service_account_json: Mapped[Optional[str]] = mapped_column(Text)
    project_id: Mapped[Optional[str]] = mapped_column(String)
    bucket_name: Mapped[Optional[str]] = mapped_column(String)
    upload_mbps: Mapped[Optional[float]] = mapped_column(Float)
