"""
Unit tests for the "backup" direction of _resolve_copy_paths, using a fake db
that only implements the .get() call this branch makes. The "restore"
direction needs a real query chain (db.query().filter_by().order_by().first())
and is instead covered by manual end-to-end verification (see PR/commit notes) -
not worth a mock-heavy unit test for a single query.
"""
from pathlib import Path
from types import SimpleNamespace

from app.tasks import _resolve_copy_paths


class FakeDB:
    def __init__(self, locations_by_id: dict):
        self._locations = locations_by_id

    def get(self, _model, location_id):
        return self._locations.get(location_id)


def test_backup_preserves_relative_path_under_destination():
    source_location = SimpleNamespace(id=1, path="/mediafiles/Movies")
    destination = SimpleNamespace(id=2, path="/data/backup")
    media_file = SimpleNamespace(
        path="/mediafiles/Movies/Comedy/Foo.mp4", filename="Foo.mp4", storage_location_id=1
    )
    job = SimpleNamespace(job_type="backup", media_file_id=99)
    db = FakeDB({1: source_location})

    result = _resolve_copy_paths(db, job, media_file, destination)

    assert result == (Path("/mediafiles/Movies/Comedy/Foo.mp4"), Path("/data/backup/Comedy/Foo.mp4"))


def test_backup_falls_back_to_filename_when_source_location_unknown():
    destination = SimpleNamespace(id=2, path="/data/backup")
    media_file = SimpleNamespace(path="/mediafiles/Movies/Foo.mp4", filename="Foo.mp4", storage_location_id=None)
    job = SimpleNamespace(job_type="backup", media_file_id=99)
    db = FakeDB({})

    result = _resolve_copy_paths(db, job, media_file, destination)

    assert result == (Path("/mediafiles/Movies/Foo.mp4"), Path("/data/backup/Foo.mp4"))


def test_backup_falls_back_to_filename_when_path_is_not_under_source_location():
    # media_file.path doesn't actually live under source_location.path (e.g. a
    # storage location was edited after the file was cataloged) - relative_to()
    # raises ValueError, and the fallback keeps the copy from crashing.
    source_location = SimpleNamespace(id=1, path="/mediafiles/OtherRoot")
    destination = SimpleNamespace(id=2, path="/data/backup")
    media_file = SimpleNamespace(
        path="/mediafiles/Movies/Comedy/Foo.mp4", filename="Foo.mp4", storage_location_id=1
    )
    job = SimpleNamespace(job_type="backup", media_file_id=99)
    db = FakeDB({1: source_location})

    result = _resolve_copy_paths(db, job, media_file, destination)

    assert result == (Path("/mediafiles/Movies/Comedy/Foo.mp4"), Path("/data/backup/Foo.mp4"))
