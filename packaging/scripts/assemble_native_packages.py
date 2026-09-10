#!/usr/bin/env python3
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

"""Assemble native package artifacts into a checksummed publishable bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Sequence

DEB_PROPERTIES = {
    "deb.distribution": ["jammy", "noble"],
    "deb.component": ["dev"],
    "deb.architecture": ["amd64"],
}
REQUIRED_VERSIONS = {"archive_version", "homebrew_version"}
REQUIRED_SOURCE = {"commit", "repository"}


def _validate_metadata(values: dict[str, str], required: set[str], name: str) -> None:
    if not isinstance(values, dict) or not required.issubset(values):
        raise ValueError(f"{name} must contain {sorted(required)}")
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in values.items()
    ):
        raise ValueError(f"{name} must map strings to strings")


def _input_files(input_dir: Path) -> list[Path]:
    if not input_dir.is_dir() or input_dir.is_symlink():
        raise ValueError(f"input directory is not a real directory: {input_dir}")
    files: list[Path] = []
    for directory, directories, names in os.walk(input_dir, followlinks=False):
        directory_path = Path(directory)
        for child in directories:
            path = directory_path / child
            if path.is_symlink():
                raise ValueError(f"symbolic link input is not allowed: {path}")
        for name in names:
            path = directory_path / name
            if path.is_symlink():
                raise ValueError(f"symbolic link input is not allowed: {path}")
            if not path.is_file():
                raise ValueError(f"undeclared file type: {path}")
            if path.stat().st_size == 0:
                raise ValueError(f"empty file input is not allowed: {path}")
            files.append(path)
    return sorted(files, key=lambda path: path.relative_to(input_dir).as_posix())


def _artifact_description(
    input_dir: Path, source_path: Path, versions: dict[str, str]
) -> tuple[Path, dict[str, object]]:
    relative_path = source_path.relative_to(input_dir)
    parts = relative_path.parts
    filename = source_path.name
    if len(parts) >= 2 and parts[0] == "deb" and filename.endswith(".deb"):
        target = Path("deb/ubuntu22.04-amd64") / filename
        return target, {
            "family": "deb",
            "platform": "ubuntu22.04-amd64",
            "targetRepository": "tsfile-debian",
            "targetPath": f"pool/dev/ubuntu22.04-amd64/{filename}",
            "properties": DEB_PROPERTIES,
        }
    if len(parts) >= 2 and parts[0] == "rpm" and filename.endswith(".rpm"):
        target = Path("rpm/almalinux9-x86_64") / filename
        return target, {
            "family": "rpm",
            "platform": "almalinux9-x86_64",
            "targetRepository": "tsfile-rpm",
            "targetPath": f"dev/el9/x86_64/{filename}",
            "properties": {},
        }
    if (
        len(parts) == 3
        and parts[:2] == ("homebrew", "Formula")
        and filename == "tsfile-dev.rb"
    ):
        target = Path(*parts)
        return target, {
            "family": "homebrew",
            "platform": "homebrew",
            "targetRepository": "tsfile",
            "targetPath": f"homebrew/dev/versions/{versions['homebrew_version']}/{target.as_posix()[len('homebrew/'):]}",
            "properties": {},
        }
    if (
        len(parts) == 3
        and parts[:2] == ("homebrew", "bottles")
        and (filename.endswith(".tar.gz") or filename.endswith(".bottle.json"))
    ):
        target = Path(*parts)
        return target, {
            "family": "homebrew",
            "platform": "homebrew",
            "targetRepository": "tsfile",
            "targetPath": f"homebrew/dev/versions/{versions['homebrew_version']}/{target.as_posix()[len('homebrew/'):]}",
            "properties": {},
        }
    if len(parts) >= 2 and parts[0] == "windows" and filename.endswith(".zip"):
        target = Path("windows") / filename
        return target, {
            "family": "windows",
            "platform": "windows-msvc-x86_64",
            "targetRepository": "tsfile",
            "targetPath": f"windows/dev/versions/{versions['archive_version']}/{filename}",
            "properties": {},
        }
    raise ValueError(f"undeclared file type: {relative_path.as_posix()}")


def _require_complete_families(
    planned: list[tuple[Path, Path, dict[str, object]]],
) -> None:
    families = [metadata["family"] for _, _, metadata in planned]
    if not any(family == "deb" for family in families):
        raise ValueError("missing DEB package input")
    if not any(family == "rpm" for family in families):
        raise ValueError("missing RPM package input")
    if not any(family == "windows" for family in families):
        raise ValueError("missing Windows ZIP input")
    homebrew_paths = {
        target.as_posix()
        for _, target, metadata in planned
        if metadata["family"] == "homebrew"
    }
    if "homebrew/Formula/tsfile-dev.rb" not in homebrew_paths:
        raise ValueError("missing Homebrew Formula input")
    if not any(path.endswith(".tar.gz") for path in homebrew_paths):
        raise ValueError("missing Homebrew Bottle input")
    if not any(path.endswith(".bottle.json") for path in homebrew_paths):
        raise ValueError("missing Homebrew Bottle metadata input")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def assemble(
    input_dir: Path, output_dir: Path, versions: dict[str, str], source: dict[str, str]
) -> dict[str, object]:
    """Copy approved artifacts and return their deterministic publication manifest."""
    _validate_metadata(versions, REQUIRED_VERSIONS, "versions")
    _validate_metadata(source, REQUIRED_SOURCE, "source")
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    files = _input_files(input_dir)
    planned: list[tuple[Path, Path, dict[str, object]]] = []
    target_paths: set[Path] = set()
    for source_path in files:
        target, metadata = _artifact_description(input_dir, source_path, versions)
        if target in target_paths:
            raise ValueError(f"duplicate target path: {target.as_posix()}")
        target_paths.add(target)
        planned.append((source_path, target, metadata))
    _require_complete_families(planned)
    if output_dir.is_symlink():
        raise ValueError(f"output directory must not be a symbolic link: {output_dir}")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(f"output directory must be empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    artifacts: list[dict[str, object]] = []
    for source_path, target, metadata in sorted(
        planned, key=lambda item: item[1].as_posix()
    ):
        destination = output_dir / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, destination)
        artifact = {
            **metadata,
            "filename": destination.name,
            "path": target.as_posix(),
            "size": destination.stat().st_size,
            "sha256": _sha256(destination),
        }
        artifacts.append(artifact)

    checksums = "".join(
        f"{artifact['sha256']}  {artifact['path']}\n" for artifact in artifacts
    )
    (output_dir / "SHA256SUMS").write_text(checksums, encoding="utf-8")
    manifest: dict[str, object] = {
        "artifacts": artifacts,
        "source": source,
        "versions": versions,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--versions-json", type=Path, required=True)
    parser.add_argument("--source-repository", required=True)
    parser.add_argument("--source-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Assemble a bundle from an Actions artifact download directory."""
    arguments = _argument_parser().parse_args(argv)
    versions = json.loads(arguments.versions_json.read_text(encoding="utf-8"))
    assemble(
        arguments.input_dir,
        arguments.output_dir,
        versions,
        {"repository": arguments.source_repository, "commit": arguments.source_commit},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
