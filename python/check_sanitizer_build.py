#!/usr/bin/env python3
#
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
#

"""Verify that Python native extensions and libtsfile are all instrumented.

Used by the sanitizer CI job after the build so the log proves that ASan
covers the complete Python -> Cython -> C wrapper -> C++ library chain. The
same check runs inside setup.py; this entry point is for standalone runs.

Usage:
    check_sanitizer_build.py <package-dir> [--expect-clean]

<package-dir> is the directory containing the built tsfile package, for
example ``python/tsfile``. With ``--expect-clean`` every binary must be free
of ASan references (useful to keep sanitizer artifacts out of release builds).
"""

from __future__ import annotations

from pathlib import Path
import sys

from _build_support import native_binaries, verify_sanitizer_instrumentation


def main(argv: list[str]) -> int:
    arguments = [item for item in argv[1:] if item != "--expect-clean"]
    expect_clean = "--expect-clean" in argv[1:]
    if len(arguments) != 1:
        print(
            f"usage: {Path(argv[0]).name} <package-dir> [--expect-clean]",
            file=sys.stderr,
        )
        return 2

    package = Path(arguments[0]).expanduser().resolve()
    if not package.is_dir():
        print(f"error: package directory not found: {package}", file=sys.stderr)
        return 2

    binaries = native_binaries(package)
    if not binaries:
        print(f"error: no native binaries found under {package}", file=sys.stderr)
        return 1

    try:
        verify_sanitizer_instrumentation(binaries, expect_instrumented=not expect_clean)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    state = "clean" if expect_clean else "instrumented"
    print(f"sanitizer check: {len(binaries)} native binaries are {state}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
