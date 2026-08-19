"""
Mirrors the subset of MediaBridge's shared schema this service touches.
The web service (web/app/models.py) owns table creation; keep column
definitions here in sync with it by hand.
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class StorageLocation(Base):
    __tablename__ = "storage_locations"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)
    path: Mapped[str] = mapped_column(String, unique=True)


class MediaFile(Base):
    __tablename__ = "media_files"

    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column(String, unique=True, index=True)
    filename: Mapped[str] = mapped_column(String)
    storage_location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("storage_locations.id"))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    fingerprint: Mapped[Optional[str]] = mapped_column(String)


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
