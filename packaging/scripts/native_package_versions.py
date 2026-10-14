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

"""Build release versions for native TsFile packages."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Sequence

_SOURCE_VERSION_RE = re.compile(
    r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$"
)
_CMAKE_VERSION_RE = re.compile(
    r"^\s*set\(\s*TsFile_CPP_VERSION\s+(?P<version>[^\s)]+)\s*\)\s*(?:#.*)?$",
    re.MULTILINE,
)


def read_cpp_version(path: Path) -> str:
    """Read the release version declared by CMake."""
    match = _CMAKE_VERSION_RE.search(path.read_text(encoding="utf-8"))
    if match is None or _SOURCE_VERSION_RE.fullmatch(match.group("version")) is None:
        raise ValueError(
            f"{path} must declare a release TsFile_CPP_VERSION as MAJOR.MINOR.PATCH"
        )
    return match.group("version")


def build_versions(source_version: str) -> dict[str, str]:
    """Create matching archive, Python, DEB, RPM, and Homebrew release versions."""
    if _SOURCE_VERSION_RE.fullmatch(source_version) is None:
        raise ValueError("source_version must be a release version MAJOR.MINOR.PATCH")
    return {
        "base_version": source_version,
        "python_version": source_version,
        "logical_version": source_version,
        "deb_version": f"{source_version}-1",
        "rpm_version": source_version,
        "rpm_release": "1.el9",
        "archive_version": source_version,
        "homebrew_version": source_version,
    }


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cmake-file", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Write release versions to JSON and, optionally, GitHub outputs."""
    arguments = _argument_parser().parse_args(argv)
    versions = build_versions(read_cpp_version(arguments.cmake_file))
    arguments.json_out.write_text(
        json.dumps(versions, indent=2) + "\n", encoding="utf-8"
    )
    if arguments.github_output is not None:
        with arguments.github_output.open("a+b") as github_output:
            if github_output.tell():
                github_output.seek(-1, 2)
                if github_output.read(1) != b"\n":
                    github_output.write(b"\n")
            github_output.write(
                (
                    "\n".join(f"{key}={value}" for key, value in versions.items())
                    + "\n"
                ).encode("utf-8")
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
