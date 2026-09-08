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

import dataclasses
import hashlib
import os
from pathlib import Path
import runpy
from types import SimpleNamespace

import pytest

import tsfile.dataset.index_identity as identity


def test_index_identity_module_imports_with_python39_dataclass_api(monkeypatch):
    """The public dataset package supports Python 3.9, which has no slots= API."""

    real_dataclass = dataclasses.dataclass

    def python39_dataclass(*args, **kwargs):
        if "slots" in kwargs:
            raise TypeError("dataclass() got an unexpected keyword argument 'slots'")
        return real_dataclass(*args, **kwargs)

    monkeypatch.setattr(dataclasses, "dataclass", python39_dataclass)
    runpy.run_path(identity.__file__, run_name="_tsfile_index_identity_py39_probe")


def test_index_attestation_hashes_exact_bytes_and_worker_validation_is_metadata_only(
    tmp_path, monkeypatch
):
    first = tmp_path / "first.tsidx"
    first.write_bytes(b"final-index\x00bytes")
    attestation = identity.attest_index(first, chunk_size=3)
    assert (
        attestation.index_identity_sha256
        == hashlib.sha256(first.read_bytes()).hexdigest()
    )

    relocated = tmp_path / "relocated.tsidx"
    relocated.write_bytes(first.read_bytes())
    relocated_attestation = identity.attest_index(relocated)
    assert relocated_attestation.index_identity_sha256 == (
        attestation.index_identity_sha256
    )
    relocated.write_bytes(b"Final-index\x00bytes")
    assert identity.attest_index(relocated).index_identity_sha256 != (
        attestation.index_identity_sha256
    )

    hash_calls = 0

    def forbidden_hash(*_args, **_kwargs):
        nonlocal hash_calls
        hash_calls += 1
        raise AssertionError("worker validation must not hash index contents")

    monkeypatch.setattr(identity, "attest_index", forbidden_hash)
    for _ in range(32):
        identity.validate_index_attestation(first, attestation)
    assert hash_calls == 0

    first.write_bytes(first.read_bytes() + b"x")
    with pytest.raises(identity.AttestationMismatchError, match="index stat"):
        identity.validate_index_attestation(first, attestation)


def test_index_attestation_fails_if_open_file_changes_while_hashing(
    tmp_path, monkeypatch
):
    path = tmp_path / "changing.tsidx"
    path.write_bytes(b"stable-index-bytes")
    real_fstat = identity.os.fstat
    fstat_calls = 0

    def changing_fstat(file_descriptor):
        nonlocal fstat_calls
        stat_result = real_fstat(file_descriptor)
        fstat_calls += 1
        if fstat_calls == 1:
            return stat_result
        return SimpleNamespace(
            st_dev=stat_result.st_dev,
            st_ino=stat_result.st_ino,
            st_size=stat_result.st_size,
            st_mtime_ns=stat_result.st_mtime_ns + 1,
        )

    monkeypatch.setattr(identity.os, "fstat", changing_fstat)
    with pytest.raises(
        identity.AttestationMismatchError, match="changed while hashing"
    ):
        identity.attest_index(path, chunk_size=3)
    assert fstat_calls == 2


def test_manifest_and_generation_are_ordered_versioned_and_fail_closed(tmp_path):
    paths = [tmp_path / "a.tsfile", tmp_path / "b.tsfile"]
    for ordinal, path in enumerate(paths):
        path.write_bytes(bytes([ordinal]) * (ordinal + 1))
    files = tuple(identity.attest_data_file(i, path) for i, path in enumerate(paths))

    manifest = identity.data_manifest_identity(files)
    assert len(manifest) == 64 and manifest == manifest.lower()
    assert identity.data_manifest_identity(tuple(reversed(files))) != manifest
    with pytest.raises(ValueError, match="strictly increasing"):
        identity.data_manifest_identity(tuple(reversed(files)), require_sorted=True)

    token = identity.file_generation_token("11" * 32, manifest, files[0])
    assert token == identity.file_generation_token("11" * 32, manifest, files[0])
    assert token != identity.file_generation_token("11" * 32, manifest, files[1])
    identity.validate_data_file_attestation(paths[0], files[0])
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement")
    os.replace(replacement, paths[0])
    with pytest.raises(identity.AttestationMismatchError, match="data-file stat"):
        identity.validate_data_file_attestation(paths[0], files[0])


@pytest.mark.parametrize(
    "fingerprint",
    ["", "0" * 15, "0" * 17, "ABCDEF0123456789", "0123456789abcdeg"],
)
def test_data_file_attestation_rejects_noncanonical_index_fingerprint(
    tmp_path, fingerprint
):
    path = tmp_path / "data.tsfile"
    path.write_bytes(b"data")
    with pytest.raises(ValueError, match="16-character lowercase hex"):
        identity.attest_data_file(0, path, existing_index_fingerprint=fingerprint)


def test_data_file_fingerprint_has_unique_canonical_identity(tmp_path):
    path = tmp_path / "data.tsfile"
    path.write_bytes(b"data")
    without_fingerprint = identity.attest_data_file(0, path)
    with_fingerprint = identity.attest_data_file(
        0, path, existing_index_fingerprint="0123456789abcdef"
    )
    manifest_without = identity.data_manifest_identity((without_fingerprint,))
    manifest_with = identity.data_manifest_identity((with_fingerprint,))
    assert manifest_without != manifest_with
    assert identity.file_generation_token(
        "11" * 32, manifest_without, without_fingerprint
    ) != identity.file_generation_token("11" * 32, manifest_with, with_fingerprint)


def test_data_file_attestation_stats_the_opened_object(tmp_path, monkeypatch):
    path = tmp_path / "data.tsfile"
    path.write_bytes(b"data")
    real_fstat = identity.os.fstat
    fstat_calls = 0

    def recording_fstat(file_descriptor):
        nonlocal fstat_calls
        fstat_calls += 1
        return real_fstat(file_descriptor)

    monkeypatch.setattr(identity.os, "fstat", recording_fstat)
    attestation = identity.attest_data_file(7, path)
    assert attestation.file_id == 7
    assert attestation.st_size == 4
    assert fstat_calls == 1


@pytest.mark.parametrize("value", ["", "A" * 64, "0" * 63, "gg" * 32])
def test_generation_token_digest_validation_is_strict(value):
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        identity.file_generation_token(value, "44" * 32, object())


def test_attestations_are_immutable_and_canonical_paths_are_absolute(tmp_path):
    path = tmp_path / "index.tsidx"
    path.write_bytes(b"index")
    attestation = identity.attest_index(path)
    assert Path(attestation.canonical_index_path).is_absolute()
    with pytest.raises((AttributeError, TypeError)):
        attestation.index_size = 1


def test_dataset_identity_public_api_is_generic_and_explicit():
    import tsfile.dataset as dataset_api

    expected = (
        "AttestationMismatchError",
        "DataFileAttestation",
        "IndexAttestation",
        "attest_data_file",
        "attest_index",
        "data_manifest_identity",
        "file_generation_token",
        "validate_data_file_attestation",
        "validate_index_attestation",
    )
    for name in expected:
        assert getattr(dataset_api, name) is getattr(identity, name)
        assert name in dataset_api.__all__
    assert "read_plan_identity" not in dataset_api.__all__
