"""
Thin wrapper around google-cloud-storage for the Settings page's cloud storage
connection workflow. Credentials always come in as a service account JSON
string (CloudStorageConfig.service_account_json in the database) rather than a
file path, since nothing here ever touches disk.
"""
import json
import secrets
import time

from google.cloud import storage
from google.oauth2 import service_account

REQUIRED_KEY_FIELDS = ("type", "project_id", "private_key", "client_email")
SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write"]

SPEED_TEST_PREFIX = "_mediabridge_speedtest/"
SPEED_TEST_PAYLOAD_BYTES = 5 * 1024 * 1024  # 5 MiB - big enough for a real throughput sample, small enough to be quick/cheap


def parse_and_validate_key(raw_json: str) -> dict:
    """Parses a service account key and checks it looks like one. Raises
    ValueError with a message suitable for showing the user if not."""
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError("That doesn't look like a valid JSON file") from exc

    if data.get("type") != "service_account":
        raise ValueError("That key isn't a service account key (unexpected or missing \"type\" field)")
    missing = [field for field in REQUIRED_KEY_FIELDS if not data.get(field)]
    if missing:
        raise ValueError(f"Key is missing required field(s): {', '.join(missing)}")
    return data


def _client(service_account_json: str, project_id: str | None = None) -> storage.Client:
    info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return storage.Client(project=project_id or info.get("project_id"), credentials=credentials)


def list_buckets(service_account_json: str, project_id: str | None = None) -> list[str]:
    client = _client(service_account_json, project_id)
    return sorted(bucket.name for bucket in client.list_buckets())


def verify_bucket_access(service_account_json: str, bucket_name: str, project_id: str | None = None) -> None:
    """Raises if the bucket doesn't exist or isn't reachable with this key."""
    client = _client(service_account_json, project_id)
    if not client.bucket(bucket_name).exists():
        raise ValueError(f"Bucket '{bucket_name}' doesn't exist or isn't accessible with this service account")


def _mbps(payload_bytes: int, seconds: float) -> float:
    return (payload_bytes * 8 / 1_000_000) / seconds if seconds > 0 else 0.0


def test_connectivity_and_speed(service_account_json: str, bucket_name: str, project_id: str | None = None) -> dict:
    """Uploads then downloads a throwaway random-content blob to measure real
    upload/download throughput against the bucket, deleting it afterward either
    way. Raises on any failure (auth, missing bucket, permission denied, a
    round-trip mismatch, ...) with a message suitable for showing the user."""
    client = _client(service_account_json, project_id)
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        raise ValueError(f"Bucket '{bucket_name}' doesn't exist or isn't accessible with this service account")

    payload = secrets.token_bytes(SPEED_TEST_PAYLOAD_BYTES)
    blob = bucket.blob(f"{SPEED_TEST_PREFIX}{secrets.token_hex(8)}.bin")
    try:
        start = time.monotonic()
        blob.upload_from_string(payload, content_type="application/octet-stream")
        upload_seconds = time.monotonic() - start

        start = time.monotonic()
        downloaded = blob.download_as_bytes()
        download_seconds = time.monotonic() - start
        if downloaded != payload:
            raise ValueError("Downloaded test data didn't match what was uploaded")
    finally:
        try:
            blob.delete()
        except Exception:
            pass  # best-effort cleanup - a stray test blob isn't worth failing the test over

    return {
        "payload_bytes": SPEED_TEST_PAYLOAD_BYTES,
        "upload_mbps": _mbps(SPEED_TEST_PAYLOAD_BYTES, upload_seconds),
        "download_mbps": _mbps(SPEED_TEST_PAYLOAD_BYTES, download_seconds),
    }
