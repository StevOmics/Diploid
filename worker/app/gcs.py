"""
Thin wrapper around google-cloud-storage for uploading/downloading backup
files to/from a connected bucket. Mirrors web/app/gcs.py's client
construction; only the primitives copy_media_file/verify_copy_job need.
"""
import json
import time
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write"]

# google-cloud-storage's default per-request timeout (60s) comfortably covers
# small objects, but archive chunk files can now be up to encryption.CHUNK_SIZE
# (256MB) - at this environment's measured upload speed (~35Mbps, i.e. ~60s to
# move 256MB on its own) that default timeout is right on the edge and trips
# under any real-world variance ("Connection aborted: The write operation
# timed out"). REQUEST_TIMEOUT_SECONDS gives real headroom; CHUNK_SIZE bounds
# each individual resumable-upload HTTP request so a slow/interrupted
# connection only has to retry one small piece, not the whole object.
REQUEST_TIMEOUT_SECONDS = 600
CHUNK_SIZE = 8 * 1024 * 1024

# Real uploads are deliberately capped at this fraction of the link's last
# measured capacity (CloudStorageConfig.upload_mbps) rather than using all of
# it - a backup saturating the connection would interfere with everything
# else on it. web/app/main.py's ETA estimate uses the same fraction so
# displayed estimates match what actually happens; keep the two in sync.
UPLOAD_THROTTLE_FRACTION = 0.5


class _ThrottledReader:
    """Wraps a binary file object so sequential reads average out to at most
    max_bytes_per_sec - passed to Blob.upload_from_file so the resumable
    uploader's own chunked reads get paced without changing what's actually
    sent."""

    def __init__(self, fileobj, max_bytes_per_sec: float):
        self._f = fileobj
        self._max_bps = max_bytes_per_sec
        self._start = time.monotonic()
        self._sent = 0

    def read(self, size=-1):
        chunk = self._f.read(size)
        if chunk:
            self._sent += len(chunk)
            expected_elapsed = self._sent / self._max_bps
            actual_elapsed = time.monotonic() - self._start
            if expected_elapsed > actual_elapsed:
                time.sleep(expected_elapsed - actual_elapsed)
        return chunk

    def __getattr__(self, name):
        return getattr(self._f, name)


def upload_mbps_to_throttle_bytes_per_sec(upload_mbps: float | None) -> float | None:
    """UPLOAD_THROTTLE_FRACTION of the last measured link capacity, in
    bytes/sec - None (no throttling) if capacity hasn't been measured yet."""
    if not upload_mbps:
        return None
    return upload_mbps * UPLOAD_THROTTLE_FRACTION * 1_000_000 / 8


def _client(service_account_json: str, project_id: str | None = None) -> storage.Client:
    info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return storage.Client(project=project_id or info.get("project_id"), credentials=credentials)


def upload_file(
    service_account_json: str,
    bucket_name: str,
    object_name: str,
    local_path,
    project_id: str | None = None,
    max_bytes_per_sec: float | None = None,
) -> None:
    client = _client(service_account_json, project_id)
    blob = client.bucket(bucket_name).blob(object_name)
    blob.chunk_size = CHUNK_SIZE
    if max_bytes_per_sec:
        size = Path(local_path).stat().st_size
        with open(local_path, "rb") as f:
            blob.upload_from_file(_ThrottledReader(f, max_bytes_per_sec), size=size, timeout=REQUEST_TIMEOUT_SECONDS)
    else:
        blob.upload_from_filename(str(local_path), timeout=REQUEST_TIMEOUT_SECONDS)


def download_file(service_account_json: str, bucket_name: str, object_name: str, local_path, project_id: str | None = None) -> None:
    client = _client(service_account_json, project_id)
    blob = client.bucket(bucket_name).blob(object_name)
    blob.chunk_size = CHUNK_SIZE
    blob.download_to_filename(str(local_path), timeout=REQUEST_TIMEOUT_SECONDS)


def blob_exists(service_account_json: str, bucket_name: str, object_name: str, project_id: str | None = None) -> bool:
    client = _client(service_account_json, project_id)
    return client.bucket(bucket_name).blob(object_name).exists()


def delete_blobs_with_prefix(service_account_json: str, bucket_name: str, prefix: str, project_id: str | None = None) -> None:
    """Deletes every object under prefix - used to clear a path_id's old
    encrypted chunks/manifest from the bucket before re-uploading a fresh set,
    so a shrinking chunk count doesn't leave orphaned (still-billed) objects."""
    client = _client(service_account_json, project_id)
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        blob.delete()
