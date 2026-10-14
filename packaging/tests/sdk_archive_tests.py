#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Download-contract tests for a relocated SDK archive, without product sources."""

import argparse
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[2]


def extract_sdk(archive_path, destination):
    destination = destination.resolve()
    if destination.exists() and any(destination.iterdir()):
        raise ValueError(f"SDK extraction directory must be empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            path = PurePosixPath(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or "\\" in member.name
                or ":" in member.name
            ):
                raise ValueError(f"Unsafe SDK archive path: {member.name}")
            if not (
                member.isfile() or member.isdir() or member.issym() or member.islnk()
            ):
                raise ValueError(f"Unsupported SDK archive entry: {member.name}")
            if member.issym() or member.islnk():
                if "\\" in member.linkname or ":" in member.linkname:
                    raise ValueError(f"Unsafe SDK archive link: {member.name}")
                base = (
                    (destination / member.name).parent
                    if member.issym()
                    else destination
                )
                target = (base / member.linkname).resolve()
                if target != destination and destination not in target.parents:
                    raise ValueError(
                        f"SDK archive link escapes extraction directory: {member.name}"
                    )
        if hasattr(tarfile, "data_filter"):
            archive.extractall(destination, members=members, filter="data")
        else:
            archive.extractall(destination, members=members)
    headers = list(destination.rglob("include/tsfile/cwrapper/tsfile_cwrapper.h"))
    if len(headers) != 1:
        raise ValueError(f"Expected one SDK installation prefix in {archive_path}")
    return headers[0].parents[3]


def run(command):
    print(
        "+ " + subprocess.list2cmdline([str(item) for item in command]),
        flush=True,
    )
    subprocess.run(command, check=True, timeout=1800)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archives", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--goarch", choices=("amd64", "arm64"), required=True)
    parser.add_argument("--llvm-bin", type=Path)
    parser.add_argument("--gtest-prefix", type=Path)
    arguments = parser.parse_args()
    archives = list(arguments.archives.glob("*.tar.gz"))
    if len(archives) != 1:
        raise ValueError(f"Expected one downloaded SDK archive: {archives}")
    prefix = extract_sdk(archives[0], arguments.destination)
    cli = (
        prefix / "bin" / ("tsfile-cli.exe" if sys.platform == "win32" else "tsfile-cli")
    )
    if not cli.is_file():
        raise ValueError(f"Missing CLI in SDK archive: {cli}")
    metadata = json.loads(
        (prefix / "share/tsfile/tsfile-build-config.json").read_text()
    )
    if metadata.get("schema_version") != 1 or not metadata.get("runtime_library"):
        raise ValueError("Missing shared-library build configuration in SDK")
    if sys.platform == "win32" and metadata.get("msvc_runtime") != "MultiThreaded":
        raise ValueError("Portable Windows SDK must record its static Release CRT")
    print(
        f"Testing downloaded SDK {archives[0]} relocated to {prefix}",
        flush=True,
    )
    if sys.platform == "darwin":
        run(
            [
                sys.executable,
                ROOT / "packaging/scripts/verify_macos_runtime.py",
                prefix,
            ]
        )
    run(
        [
            sys.executable,
            ROOT / "packaging/tests/cli_package_smoke.py",
            "--cli",
            cli,
            "--resources",
            arguments.resources,
        ]
    )
    cpp = [
        sys.executable,
        ROOT / "packaging/tests/cpp_installed_tests.py",
        "--prefix",
        prefix,
        "--build-dir",
        arguments.build_dir,
    ]
    if arguments.gtest_prefix is not None:
        cpp += ["--gtest-prefix", arguments.gtest_prefix]
    run(cpp)
    go = [
        sys.executable,
        ROOT / "packaging/tests/go_installed_tests.py",
        "--prefix",
        prefix,
        "--goarch",
        arguments.goarch,
        "--build-dir",
        arguments.build_dir / "go",
    ]
    if arguments.llvm_bin is not None:
        go += ["--llvm-bin", arguments.llvm_bin]
    run(go)


if __name__ == "__main__":
    main()
