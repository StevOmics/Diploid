from datetime import datetime
from pathlib import Path

import httpx
from sqlalchemy.orm import Session

from app import jellyfin
from app.config import VIDEO_EXTENSIONS
from app.fingerprint import compute_fingerprint
from app.models import JellyfinConfig, MediaFile, StorageLocation
from app.nfo import parse_nfo


def scan_library(db: Session) -> dict:
    totals = {"found": 0, "added": 0, "updated": 0}
    locations = db.query(StorageLocation).filter_by(location_type="local").all()
    for location in locations:
        for key, value in _scan_location(db, location).items():
            totals[key] += value
    return totals


def _scan_location(db: Session, location: StorageLocation) -> dict:
    root = Path(location.path)
    found = added = updated = 0

    if not root.is_dir():
        return {"found": 0, "added": 0, "updated": 0}

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        extension = path.suffix.lower().lstrip(".")
        if extension not in VIDEO_EXTENSIONS:
            continue

        found += 1
        rel_parts = path.relative_to(root).parts
        genre = rel_parts[0] if len(rel_parts) > 1 else None
        size_bytes = path.stat().st_size
        path_str = str(path)
        nfo_fields = parse_nfo(path.with_suffix(".nfo")) or {}

        existing = db.query(MediaFile).filter_by(path=path_str).one_or_none()
        if existing:
            size_changed = existing.size_bytes != size_bytes
            existing.size_bytes = size_bytes
            existing.genre = genre
            existing.storage_location_id = location.id
            for key, value in nfo_fields.items():
                setattr(existing, key, value)
            if size_changed or not existing.fingerprint:
                existing.fingerprint = compute_fingerprint(path, size_bytes)
            updated += 1
        else:
            db.add(
                MediaFile(
                    path=path_str,
                    filename=path.name,
                    extension=extension,
                    genre=genre,
                    size_bytes=size_bytes,
                    storage_location_id=location.id,
                    fingerprint=compute_fingerprint(path, size_bytes),
                    **nfo_fields,
                )
            )
            added += 1

    db.commit()
    return {"found": found, "added": added, "updated": updated}


def _parse_jellyfin_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    if "." in value:
        head, rest = value.split(".", 1)
        frac_digits = 0
        while frac_digits < len(rest) and rest[frac_digits].isdigit():
            frac_digits += 1
        frac = rest[:frac_digits][:6].ljust(6, "0")
        value = f"{head}.{frac}{rest[frac_digits:]}"
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _map_jellyfin_path(config: JellyfinConfig, jf_path: str) -> str | None:
    if not jf_path.startswith(config.path_prefix_from):
        return None
    return config.path_prefix_to + jf_path[len(config.path_prefix_from):]


def _library_for_path(mapped_path: str, library_prefixes: list[tuple[str, str]]) -> str | None:
    for prefix, name in library_prefixes:
        if mapped_path.startswith(prefix):
            return name
    return None


def sync_watch_data(db: Session, config: JellyfinConfig) -> dict:
    items = jellyfin.fetch_movie_watch_data(config.server_url, config.api_key, config.sync_user_id)
    matched = unmatched = 0

    # Map each library's Jellyfin-side location into MediaBridge's path space,
    # same prefix swap used for individual items, so we can tell them apart.
    library_prefixes: list[tuple[str, str]] = []
    try:
        for lib in jellyfin.list_libraries(config.server_url, config.api_key):
            for location in lib["locations"]:
                mapped = _map_jellyfin_path(config, location)
                if mapped:
                    library_prefixes.append((mapped, lib["name"]))
    except httpx.HTTPError:
        pass

    for item in items:
        jf_path = item.get("Path")
        mapped_path = _map_jellyfin_path(config, jf_path) if jf_path else None
        if not mapped_path:
            unmatched += 1
            continue

        media_file = db.query(MediaFile).filter_by(path=mapped_path).one_or_none()
        if not media_file:
            unmatched += 1
            continue

        user_data = item.get("UserData", {})
        media_file.jellyfin_item_id = item.get("Id")
        media_file.jellyfin_library = _library_for_path(mapped_path, library_prefixes)
        media_file.watched = bool(user_data.get("Played", False))
        media_file.play_count = user_data.get("PlayCount", 0)
        media_file.last_played_at = _parse_jellyfin_datetime(user_data.get("LastPlayedDate"))
        matched += 1

    db.commit()
    return {"items": len(items), "matched": matched, "unmatched": unmatched}
