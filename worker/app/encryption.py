"""
Streaming AES-256-GCM encryption for backups.

Design:
- One master key is derived from the backup password via PBKDF2-HMAC-SHA256,
  using a salt generated once when the password is first set (not secret,
  just needs to be fixed).
- Each file gets its own encryption key, derived deterministically from the
  master key + that file's UUID (HMAC-SHA256). Nothing per-file needs to be
  stored to decrypt later - just the password and the file's UUID (already in
  the catalog), so there's no separate key store to keep in sync or lose.
- Chunk/manifest filenames on disk are a keyed hash of the UUID (a "path ID"),
  never the raw file key - putting real key material in a filename would leak
  it to anyone who can list the backup destination.
- AES-GCM requires a unique (key, nonce) pair per encryption. The key is fixed
  per file, so each chunk uses nonce = <4-byte random prefix, fresh per backup
  run><8-byte big-endian chunk index>. Re-backing up a changed file later gets
  a fresh random prefix, so nonces never repeat for a given key even across
  runs. The prefix isn't secret and is stored in that run's manifest.
- Each chunk is its own independent AEAD operation (encrypt-then-authenticate),
  so a corrupted/tampered chunk fails to decrypt on its own with InvalidTag,
  rather than silently producing wrong bytes.
- CHUNK_SIZE bounds how much plaintext/ciphertext is held in memory at once -
  it's deliberately unrelated to TransferConfig's clump/split sizing (which
  decides how many separate *archives* a backup is stored as, at the worker
  task level in tasks.py). A file/part smaller than CHUNK_SIZE is always
  written as a single .000000.chunk, so raising this just means fewer,
  larger chunk files per archive - it never changes how many archives a
  backup produces.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import Callable, Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# 256MB keeps a typical multi-GB backup down to single-digit chunk files
# (an 870MB file is now 4 chunks instead of 104 at the old 8MB) while still
# bounding peak memory per chunk (~2x this, for the plaintext read buffer and
# the ciphertext it's encrypted into) regardless of how large the source file is.
CHUNK_SIZE = 256 * 1024 * 1024
KDF_ITERATIONS = 200_000
NONCE_PREFIX_SIZE = 4
NONCE_COUNTER_SIZE = 8

ProgressCallback = Optional[Callable[[int, int], None]]


def derive_master_key(password: str, kdf_salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), kdf_salt, KDF_ITERATIONS, dklen=32)


def derive_file_key(master_key: bytes, file_uuid: str) -> bytes:
    return hmac.new(master_key, b"mediabridge-backup-file-key:" + file_uuid.encode(), hashlib.sha256).digest()


def derive_path_id(master_key: bytes, file_uuid: str) -> str:
    return hmac.new(master_key, b"mediabridge-backup-path-id:" + file_uuid.encode(), hashlib.sha256).hexdigest()


def _nonce_for_chunk(nonce_prefix: bytes, index: int) -> bytes:
    return nonce_prefix + index.to_bytes(NONCE_COUNTER_SIZE, "big")


def _chunk_path(dest_dir: Path, path_id: str, index: int) -> Path:
    return dest_dir / f"{path_id}.{index:06d}.chunk"


def _manifest_path(dest_dir: Path, path_id: str) -> Path:
    return dest_dir / f"{path_id}.manifest.json"


def encrypt_file(
    source_path: Path, dest_dir: Path, file_key: bytes, path_id: str, progress_cb: ProgressCallback = None
) -> dict:
    """Encrypts source_path into dest_dir/<path_id>.NNNNNN.chunk files + a manifest."""
    aesgcm = AESGCM(file_key)
    nonce_prefix = os.urandom(NONCE_PREFIX_SIZE)
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Re-backing up the same file (same path_id) can end up with fewer chunks
    # than a prior run - a changed CHUNK_SIZE, or a file that's shrunk - so
    # clear any chunks already on disk for this path_id first. Otherwise the
    # trailing ones from the old run never get overwritten and just sit there
    # as orphaned, undecryptable (and confusing) leftovers.
    for stale in dest_dir.glob(f"{path_id}.*.chunk"):
        stale.unlink()

    total_size = source_path.stat().st_size
    copied = 0
    index = 0
    with source_path.open("rb") as src:
        while True:
            chunk = src.read(CHUNK_SIZE)
            if not chunk:
                break
            nonce = _nonce_for_chunk(nonce_prefix, index)
            ciphertext = aesgcm.encrypt(nonce, chunk, None)

            chunk_path = _chunk_path(dest_dir, path_id, index)
            tmp_path = chunk_path.with_suffix(chunk_path.suffix + ".tmp")
            tmp_path.write_bytes(ciphertext)
            tmp_path.replace(chunk_path)

            copied += len(chunk)
            index += 1
            if progress_cb:
                progress_cb(copied, total_size)

    manifest = {
        "algorithm": "AES-256-GCM",
        "nonce_prefix": nonce_prefix.hex(),
        "chunk_size": CHUNK_SIZE,
        "chunk_count": index,
        "total_size": total_size,
    }
    _manifest_path(dest_dir, path_id).write_text(json.dumps(manifest))
    return manifest


def decrypt_file(
    dest_dir: Path, file_key: bytes, path_id: str, output_path: Path, progress_cb: ProgressCallback = None
) -> int:
    """Decrypts dest_dir/<path_id>.*.chunk files back into output_path. Raises
    cryptography.exceptions.InvalidTag if a chunk is corrupted, tampered with,
    or the password/key is wrong."""
    manifest = json.loads(_manifest_path(dest_dir, path_id).read_text())
    nonce_prefix = bytes.fromhex(manifest["nonce_prefix"])
    total_size = manifest["total_size"]
    aesgcm = AESGCM(file_key)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(output_path.name + ".mbcopy")

    copied = 0
    with tmp_path.open("wb") as dst:
        for index in range(manifest["chunk_count"]):
            ciphertext = _chunk_path(dest_dir, path_id, index).read_bytes()
            nonce = _nonce_for_chunk(nonce_prefix, index)
            plaintext = aesgcm.decrypt(nonce, ciphertext, None)
            dst.write(plaintext)
            copied += len(plaintext)
            if progress_cb:
                progress_cb(copied, total_size)

    tmp_path.replace(output_path)
    return copied


def backup_exists(dest_dir: Path, path_id: str) -> bool:
    return _manifest_path(dest_dir, path_id).is_file()
