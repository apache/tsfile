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

"""Build deterministic development versions for native TsFile packages."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Sequence


_SOURCE_VERSION_RE = re.compile(
    r"^(?P<base>(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))\.dev$"
)
_CMAKE_VERSION_RE = re.compile(
    r"^\s*set\(\s*TsFile_CPP_VERSION\s+(?P<version>[^\s)]+)\s*\)\s*(?:#.*)?$",
    re.MULTILINE,
)
_BUILD_DATE_RE = re.compile(r"^[0-9]{8}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,}$")


def read_cpp_version(path: Path) -> str:
    """Read and validate the development version declared by CMake."""
    match = _CMAKE_VERSION_RE.search(path.read_text(encoding="utf-8"))
    if match is None or _SOURCE_VERSION_RE.fullmatch(match.group("version")) is None:
        raise ValueError(f"{path} must declare TsFile_CPP_VERSION as MAJOR.MINOR.PATCH.dev")
    return match.group("version")


def _positive_integer(value: int, name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return str(value)


def build_versions(
    source_version: str,
    build_date: str,
    run_number: int,
    run_attempt: int,
    git_sha: str,
) -> dict[str, str]:
    """Create archive, DEB, RPM, and Homebrew versions from one build identity."""
    source_match = _SOURCE_VERSION_RE.fullmatch(source_version)
    if source_match is None:
        raise ValueError("source_version must be MAJOR.MINOR.PATCH.dev")
    if _BUILD_DATE_RE.fullmatch(build_date) is None:
        raise ValueError("build_date must be YYYYMMDD")
    datetime.strptime(build_date, "%Y%m%d")
    if _GIT_SHA_RE.fullmatch(git_sha) is None:
        raise ValueError("git_sha must contain at least seven hexadecimal characters")

    run_number_text = _positive_integer(run_number, "run_number")
    run_attempt_text = _positive_integer(run_attempt, "run_attempt")
    base_version = source_match.group("base")
    short_sha = git_sha[:7].lower()
    build_identity = f"{build_date}.{run_number_text}.{run_attempt_text}.g{short_sha}"

    return {
        "base_version": base_version,
        "logical_version": f"{base_version}.dev0+{build_identity}",
        "deb_version": f"{base_version}~dev0+{build_identity}-1",
        "rpm_version": base_version,
        "rpm_release": f"0.dev0.{build_identity}.el9",
        "archive_version": f"{base_version}-dev0.{build_identity}",
        "homebrew_version": f"{base_version}.dev0.{build_identity}",
    }


def _parse_positive_integer(value: str) -> int:
    if not re.fullmatch(r"[1-9][0-9]*", value):
        raise argparse.ArgumentTypeError("must be a positive integer")
    return int(value)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic development versions for native TsFile packages."
    )
    parser.add_argument("--cmake-file", type=Path, required=True)
    parser.add_argument("--build-date", required=True)
    parser.add_argument("--run-number", type=_parse_positive_integer, required=True)
    parser.add_argument("--run-attempt", type=_parse_positive_integer, required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Write the generated versions to JSON and, optionally, GitHub outputs."""
    arguments = _argument_parser().parse_args(argv)
    versions = build_versions(
        read_cpp_version(arguments.cmake_file),
        arguments.build_date,
        arguments.run_number,
        arguments.run_attempt,
        arguments.git_sha,
    )
    arguments.json_out.write_text(json.dumps(versions, indent=2) + "\n", encoding="utf-8")
    if arguments.github_output is not None:
        with arguments.github_output.open("a", encoding="utf-8") as github_output:
            github_output.write(
                "\n".join(f"{key}={value}" for key, value in versions.items()) + "\n"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
