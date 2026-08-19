import hashlib
from pathlib import Path

# Mirrors web/app/fingerprint.py - keep the algorithm identical so fingerprints
# computed here are comparable to the ones stored by the web service's scan.
SAMPLE_SIZE = 1024 * 1024


def compute_fingerprint(path: Path, size_bytes: int) -> str:
    hasher = hashlib.blake2b(digest_size=32)
    hasher.update(str(size_bytes).encode())

    with path.open("rb") as f:
        hasher.update(f.read(SAMPLE_SIZE))
        if size_bytes > SAMPLE_SIZE:
            f.seek(max(size_bytes - SAMPLE_SIZE, SAMPLE_SIZE))
            hasher.update(f.read())

    return hasher.hexdigest()
