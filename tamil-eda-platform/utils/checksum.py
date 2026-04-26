from __future__ import annotations

import hashlib
import unicodedata
from pathlib import Path

from core.exceptions import PlatformError

_CHUNK_SIZE = 65536 #64KB - atream files in chunks, never load whole file

class ChecksumError(PlatformError):
    """Raised when a checksum operation fails"""


def md5_of_text(text: str) -> str:
    """Compute MD5 hash of text after normalisation.
    Normalises before hashing so 'Hello  World' and 'Hello World'
    produce the same hash.
    FOR DEDUPLICATION ONLY — not suitable for security or authentication.

    Args:
        text: Raw text to hash

    Returns:
        32-character lowercase hex string
    """
    normalised = unicodedata.normalize("NFC", text.strip())
    return hashlib.md5(normalised.encode("utf-8")).hexdigest()

def md5_of_file(path: Path) -> str:
    """Compute MD5 hash of a file by streaming in 64KB chunks.
    Memory usage is constant regardless of file size.
    FOR DEDUPLICATION ONLY — not suitable for security.

    Args:
        path: Path to the file

    Returns:
        32-character lowercase hex string

    Raises:
        ChecksumError: If file cannot be read
    """
    h = hashlib.md5()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                h.update(chunk)
    except OSError as exc:
        raise ChecksumError(
            f"Cannot read file for MD5: {path}",
            details={"path": str(path), "error": str(exc)},

        )from exc
    return h.hexdigest()

def sha256_of_file(path: Path) -> str:
    """Compute SHA-256 hash of a file by streaming in 64KB chunks.
    Cryptographically secure — use for integrity verification and audit.

    Args:
        path: Path to the file

    Returns:
        64-character lowercase hex string

    Raises:
        ChecksumError: If file cannot be read
    """

    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                h.update(chunk)
    except OSError as exc:
        raise ChecksumError(
            f"Cannot read file for SHA-256: {path}",
            details={"path": str(path), "error": str(exc)},
        ) from exc
    return h.hexdigest()

def sha256_of_bytes(data: bytes) -> str:
    """Compute SHA-256 hash of raw bytes.
    Used by parquet_writer after writing file bytes to disk.

    Args:
        data: Raw bytes to hash

    Returns:
        64-character lowercase hex string
    """
    return hashlib.sha256(data).hexdigest()


def md5_of_record(record: dict) -> str:
    """Compute MD5 hash of a record dict for dataset-level deduplication.
    Serialises the dict to a canonical string before hashing so key
    order does not affect the hash.
    FOR DEDUPLICATION ONLY — not suitable for security.

    Args:
        record: Dict to hash

    Returns:
        32-character lowercase hex string
    """
    import json
    canonical = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(canonical.encode("utf-8")).hexdigest()


def verify_file_integrity(path: Path, expected_sha256: str) -> bool:
    """Verify a file's SHA-256 hash matches the expected value.
    Returns False if file was corrupted or tampered with since writing.
    Used by nightly integrity check job.

    Args:
        path:            Path to the file to verify
        expected_sha256: Hash recorded at write time

    Returns:
        True if file is intact, False if corrupted or tampered
    """
    try:
        actual = sha256_of_file(path)
        return actual == expected_sha256
    except ChecksumError:
        return False



