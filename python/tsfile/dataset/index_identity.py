# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Content and generation identities for trusted dataset indexes.

Expensive content hashing belongs to the parent attestation pass.  Workers use
the validation helpers, which compare canonical paths and stat metadata only.
The encodings below are explicitly versioned and length-prefixed so they do not
depend on Python object identity, mapping order, locale, or pickle.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
import struct
from typing import BinaryIO, Iterable


_INDEX_SCHEMA_VERSION = 1
_DATA_MANIFEST_DOMAIN = b"tsfile-data-manifest-v1\0"
_FILE_GENERATION_DOMAIN = b"tsfile-file-generation-v1\0"
_DIGEST_HEX_LENGTH = 64
_INDEX_FINGERPRINT_HEX_LENGTH = 16
_DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class AttestationMismatchError(RuntimeError):
    """Raised before reading when trusted metadata no longer matches."""


@dataclass(frozen=True)
class IndexAttestation:
    schema_version: int
    canonical_index_path: str
    index_identity_sha256: str
    index_size: int
    st_dev: int
    st_ino: int
    st_size: int
    st_mtime_ns: int


@dataclass(frozen=True)
class DataFileAttestation:
    file_id: int
    canonical_path: str
    st_dev: int
    st_ino: int
    st_size: int
    st_mtime_ns: int
    existing_index_fingerprint: str | None = None


def _canonical_path(path: os.PathLike[str] | str) -> str:
    return os.path.realpath(os.path.abspath(os.fspath(path)))


def _stat_tuple(stat_result: os.stat_result) -> tuple[int, int, int, int]:
    return (
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_size,
        stat_result.st_mtime_ns,
    )


def _digest_bytes(value: str) -> bytes:
    if (
        len(value) != _DIGEST_HEX_LENGTH
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("identity must be a lowercase SHA-256 hex digest")
    return bytes.fromhex(value)


def _optional_index_fingerprint_bytes(value: str | None) -> bytes:
    if value is None:
        return b""
    if (
        not isinstance(value, str)
        or len(value) != _INDEX_FINGERPRINT_HEX_LENGTH
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(
            "existing_index_fingerprint must be a 16-character lowercase hex value"
        )
    return value.encode("ascii")


def _u64(value: int, field: str) -> bytes:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 2**64:
        raise ValueError(f"{field} must fit uint64")
    return struct.pack("<Q", value)


def _field(value: bytes) -> bytes:
    return _u64(len(value), "field length") + value


def _update_stream(digest: "hashlib._Hash", stream: BinaryIO, chunk_size: int) -> None:
    while chunk := stream.read(chunk_size):
        digest.update(chunk)


def attest_index(
    path: os.PathLike[str] | str, *, chunk_size: int = _DEFAULT_CHUNK_SIZE
) -> IndexAttestation:
    """Hash finalized index bytes once in the trusted parent attestation pass."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    canonical_path = _canonical_path(path)
    digest = hashlib.sha256()
    with open(canonical_path, "rb") as stream:
        before_hash = os.fstat(stream.fileno())
        _update_stream(digest, stream, chunk_size)
        after_hash = os.fstat(stream.fileno())
    if _stat_tuple(before_hash) != _stat_tuple(after_hash):
        raise AttestationMismatchError("index changed while hashing")
    return IndexAttestation(
        schema_version=_INDEX_SCHEMA_VERSION,
        canonical_index_path=canonical_path,
        index_identity_sha256=digest.hexdigest(),
        index_size=after_hash.st_size,
        st_dev=after_hash.st_dev,
        st_ino=after_hash.st_ino,
        st_size=after_hash.st_size,
        st_mtime_ns=after_hash.st_mtime_ns,
    )


def validate_index_attestation(
    path: os.PathLike[str] | str, attestation: IndexAttestation
) -> None:
    """Validate with one metadata lookup; never scan or re-hash index bytes."""
    canonical_path = _canonical_path(path)
    if attestation.schema_version != _INDEX_SCHEMA_VERSION:
        raise AttestationMismatchError("unsupported index attestation schema")
    _digest_bytes(attestation.index_identity_sha256)
    if canonical_path != attestation.canonical_index_path:
        raise AttestationMismatchError("index path does not match attestation")
    stat_result = os.stat(canonical_path)
    expected = (
        attestation.st_dev,
        attestation.st_ino,
        attestation.st_size,
        attestation.st_mtime_ns,
    )
    if (
        attestation.index_size != attestation.st_size
        or _stat_tuple(stat_result) != expected
    ):
        raise AttestationMismatchError("index stat does not match attestation")


def attest_data_file(
    file_id: int,
    path: os.PathLike[str] | str,
    *,
    existing_index_fingerprint: str | None = None,
) -> DataFileAttestation:
    canonical_path = _canonical_path(path)
    _u64(file_id, "file_id")
    _optional_index_fingerprint_bytes(existing_index_fingerprint)
    with open(canonical_path, "rb") as stream:
        stat_result = os.fstat(stream.fileno())
    return DataFileAttestation(
        file_id=file_id,
        canonical_path=canonical_path,
        st_dev=stat_result.st_dev,
        st_ino=stat_result.st_ino,
        st_size=stat_result.st_size,
        st_mtime_ns=stat_result.st_mtime_ns,
        existing_index_fingerprint=existing_index_fingerprint,
    )


def validate_data_file_attestation(
    path: os.PathLike[str] | str, attestation: DataFileAttestation
) -> None:
    canonical_path = _canonical_path(path)
    if canonical_path != attestation.canonical_path:
        raise AttestationMismatchError("data-file path does not match attestation")
    stat_result = os.stat(canonical_path)
    expected = (
        attestation.st_dev,
        attestation.st_ino,
        attestation.st_size,
        attestation.st_mtime_ns,
    )
    if _stat_tuple(stat_result) != expected:
        raise AttestationMismatchError("data-file stat does not match attestation")


def _canonical_data_file(attestation: DataFileAttestation) -> bytes:
    encoded_fingerprint = _optional_index_fingerprint_bytes(
        attestation.existing_index_fingerprint
    )
    return b"".join(
        (
            _field(attestation.canonical_path.encode("utf-8")),
            _u64(attestation.st_dev, "st_dev"),
            _u64(attestation.st_ino, "st_ino"),
            _u64(attestation.st_size, "st_size"),
            _u64(attestation.st_mtime_ns, "st_mtime_ns"),
            _field(encoded_fingerprint),
        )
    )


def data_manifest_identity(
    attestations: Iterable[DataFileAttestation], *, require_sorted: bool = False
) -> str:
    files = tuple(attestations)
    if require_sorted and any(
        left.file_id >= right.file_id for left, right in zip(files, files[1:])
    ):
        raise ValueError("data-file IDs must be strictly increasing")
    digest = hashlib.sha256(_DATA_MANIFEST_DOMAIN)
    digest.update(_u64(len(files), "file count"))
    for attestation in files:
        digest.update(_u64(attestation.file_id, "file_id"))
        digest.update(_field(_canonical_data_file(attestation)))
    return digest.hexdigest()


def file_generation_token(
    index_identity_sha256: str,
    data_manifest_identity_sha256: str,
    attestation: DataFileAttestation,
) -> str:
    digest = hashlib.sha256(_FILE_GENERATION_DOMAIN)
    digest.update(_digest_bytes(index_identity_sha256))
    digest.update(_digest_bytes(data_manifest_identity_sha256))
    digest.update(_u64(attestation.file_id, "file_id"))
    digest.update(_field(_canonical_data_file(attestation)))
    return digest.hexdigest()
