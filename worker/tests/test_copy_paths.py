"""
Unit tests for _relative_source_path, the helper that computes a plain
(unencrypted) backup's destination path relative to the source storage
location. Encrypted backups don't use this (they flatten to <path_id> files),
and restore/backup dispatch logic is covered by manual end-to-end verification
(see commit notes) rather than a mock-heavy unit test of the full task.
"""
from pathlib import Path
from types import SimpleNamespace

from app.tasks import _relative_source_path


class FakeDB:
    def __init__(self, locations_by_id: dict):
        self._locations = locations_by_id

    def get(self, _model, location_id):
        return self._locations.get(location_id)


def test_preserves_relative_path_under_source_location():
    source_location = SimpleNamespace(id=1, path="/mediafiles/Movies")
    media_file = SimpleNamespace(
        path="/mediafiles/Movies/Comedy/Foo.mp4", filename="Foo.mp4", storage_location_id=1
    )
    db = FakeDB({1: source_location})

    assert _relative_source_path(db, media_file) == Path("Comedy/Foo.mp4")


def test_falls_back_to_filename_when_source_location_unknown():
    media_file = SimpleNamespace(path="/mediafiles/Movies/Foo.mp4", filename="Foo.mp4", storage_location_id=None)
    db = FakeDB({})

    assert _relative_source_path(db, media_file) == Path("Foo.mp4")


def test_falls_back_to_filename_when_path_is_not_under_source_location():
    # media_file.path doesn't actually live under source_location.path (e.g. a
    # storage location was edited after the file was cataloged) - relative_to()
    # raises ValueError, and the fallback keeps the copy from crashing.
    source_location = SimpleNamespace(id=1, path="/mediafiles/OtherRoot")
    media_file = SimpleNamespace(
        path="/mediafiles/Movies/Comedy/Foo.mp4", filename="Foo.mp4", storage_location_id=1
    )
    db = FakeDB({1: source_location})

    assert _relative_source_path(db, media_file) == Path("Foo.mp4")
