from pathlib import Path

import pytest
from cryptography.exceptions import InvalidTag

from app.encryption import (
    backup_exists,
    decrypt_file,
    derive_file_key,
    derive_master_key,
    derive_path_id,
    encrypt_file,
)


def test_master_key_is_deterministic_for_same_password_and_salt():
    salt = b"fixed-salt-16by."
    assert derive_master_key("hunter2", salt) == derive_master_key("hunter2", salt)


def test_master_key_differs_for_different_passwords():
    salt = b"fixed-salt-16by."
    assert derive_master_key("hunter2", salt) != derive_master_key("different", salt)


def test_master_key_differs_for_different_salts():
    assert derive_master_key("hunter2", b"salt-one........") != derive_master_key("hunter2", b"salt-two........")


def test_file_key_is_deterministic_and_unique_per_uuid():
    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    key_a = derive_file_key(master_key, "uuid-a")
    key_a_again = derive_file_key(master_key, "uuid-a")
    key_b = derive_file_key(master_key, "uuid-b")

    assert key_a == key_a_again
    assert key_a != key_b


def test_file_key_never_equals_master_key():
    # The derived per-file key must not just be the master key reused verbatim.
    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    assert derive_file_key(master_key, "some-uuid") != master_key


def test_path_id_does_not_reveal_the_key():
    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    path_id = derive_path_id(master_key, "some-uuid")
    file_key = derive_file_key(master_key, "some-uuid")
    assert path_id != file_key.hex()
    assert file_key.hex() not in path_id


def test_path_id_is_deterministic_and_unique_per_uuid():
    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    assert derive_path_id(master_key, "uuid-a") == derive_path_id(master_key, "uuid-a")
    assert derive_path_id(master_key, "uuid-a") != derive_path_id(master_key, "uuid-b")


def test_encrypt_decrypt_round_trip(tmp_path: Path):
    source = tmp_path / "source.mp4"
    original = b"some movie bytes, could be anything" * 1000
    source.write_bytes(original)

    dest_dir = tmp_path / "backup"
    output = tmp_path / "restored.mp4"

    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    file_key = derive_file_key(master_key, "file-uuid-1")
    path_id = derive_path_id(master_key, "file-uuid-1")

    encrypt_file(source, dest_dir, file_key, path_id)
    assert backup_exists(dest_dir, path_id)

    decrypt_file(dest_dir, file_key, path_id, output)
    assert output.read_bytes() == original


def test_round_trip_spanning_multiple_chunks(tmp_path: Path, monkeypatch):
    import app.encryption as encryption_module

    monkeypatch.setattr(encryption_module, "CHUNK_SIZE", 100)  # force many small chunks

    source = tmp_path / "source.bin"
    original = bytes(range(256)) * 10  # 2560 bytes -> 26 chunks at CHUNK_SIZE=100
    source.write_bytes(original)

    dest_dir = tmp_path / "backup"
    output = tmp_path / "restored.bin"
    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    file_key = derive_file_key(master_key, "file-uuid-2")
    path_id = derive_path_id(master_key, "file-uuid-2")

    manifest = encrypt_file(source, dest_dir, file_key, path_id)
    assert manifest["chunk_count"] == 26

    decrypt_file(dest_dir, file_key, path_id, output)
    assert output.read_bytes() == original


def test_chunk_filenames_do_not_contain_original_filename_or_key(tmp_path: Path):
    source = tmp_path / "Star_Wars_Episode_IV.mp4"
    source.write_bytes(b"secret movie content")
    dest_dir = tmp_path / "backup"

    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    file_key = derive_file_key(master_key, "file-uuid-3")
    path_id = derive_path_id(master_key, "file-uuid-3")

    encrypt_file(source, dest_dir, file_key, path_id)

    names = [p.name for p in dest_dir.iterdir()]
    assert names, "expected chunk/manifest files to be written"
    for name in names:
        assert "Star_Wars" not in name
        assert file_key.hex() not in name


def test_chunk_files_do_not_contain_plaintext_content(tmp_path: Path):
    source = tmp_path / "source.txt"
    secret_marker = b"THIS_IS_THE_SECRET_MOVIE_CONTENT"
    source.write_bytes(secret_marker * 100)
    dest_dir = tmp_path / "backup"

    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    file_key = derive_file_key(master_key, "file-uuid-4")
    path_id = derive_path_id(master_key, "file-uuid-4")

    encrypt_file(source, dest_dir, file_key, path_id)

    for chunk_file in dest_dir.glob("*.chunk"):
        assert secret_marker not in chunk_file.read_bytes()


def test_decrypt_with_wrong_password_raises_invalid_tag(tmp_path: Path):
    source = tmp_path / "source.bin"
    source.write_bytes(b"some content")
    dest_dir = tmp_path / "backup"
    output = tmp_path / "restored.bin"

    master_key = derive_master_key("correct-password", b"fixed-salt-16by.")
    wrong_master_key = derive_master_key("wrong-password", b"fixed-salt-16by.")

    file_key = derive_file_key(master_key, "file-uuid-5")
    path_id = derive_path_id(master_key, "file-uuid-5")
    encrypt_file(source, dest_dir, file_key, path_id)

    wrong_file_key = derive_file_key(wrong_master_key, "file-uuid-5")
    # path_id would also differ with the wrong key in practice (so the backup
    # wouldn't even be found), but even given the right location, decrypting
    # with the wrong key must fail loudly rather than produce silently wrong bytes.
    with pytest.raises(InvalidTag):
        decrypt_file(dest_dir, wrong_file_key, path_id, output)


def test_decrypt_detects_tampered_chunk(tmp_path: Path):
    source = tmp_path / "source.bin"
    source.write_bytes(b"some content that will be tampered with")
    dest_dir = tmp_path / "backup"
    output = tmp_path / "restored.bin"

    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    file_key = derive_file_key(master_key, "file-uuid-6")
    path_id = derive_path_id(master_key, "file-uuid-6")
    encrypt_file(source, dest_dir, file_key, path_id)

    chunk_file = next(dest_dir.glob("*.chunk"))
    tampered = bytearray(chunk_file.read_bytes())
    tampered[0] ^= 0xFF
    chunk_file.write_bytes(bytes(tampered))

    with pytest.raises(InvalidTag):
        decrypt_file(dest_dir, file_key, path_id, output)


def test_backup_exists_false_when_no_manifest(tmp_path: Path):
    assert backup_exists(tmp_path, "nonexistent-path-id") is False


def test_progress_callback_reports_final_total(tmp_path: Path):
    source = tmp_path / "source.bin"
    data = b"x" * 5000
    source.write_bytes(data)
    dest_dir = tmp_path / "backup"

    master_key = derive_master_key("hunter2", b"fixed-salt-16by.")
    file_key = derive_file_key(master_key, "file-uuid-7")
    path_id = derive_path_id(master_key, "file-uuid-7")

    calls = []
    encrypt_file(source, dest_dir, file_key, path_id, progress_cb=lambda copied, total: calls.append((copied, total)))

    assert calls
    assert calls[-1] == (len(data), len(data))
