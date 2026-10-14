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

"""Validate that the C++ build tree required by the Python module exists.

The Python module links against the native library produced by the ``cpp``
module. When the two are built together (``-Pwith-python``) the artifacts are
always present. When the Python module is built on its own
(``-Pwith-python-only``) they must already exist, so this check turns a late and
opaque failure inside ``setup.py`` into an early, actionable one.
"""

from __future__ import annotations

import sys
from pathlib import Path


def missing_parts(build_root: Path) -> list[Path]:
    """Return the required subdirectories of *build_root* that are absent."""
    return [
        part
        for part in (build_root / "include", build_root / "lib")
        if not part.is_dir()
    ]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {Path(argv[0]).name} <cpp-build-root>", file=sys.stderr)
        return 2

    build_root = Path(argv[1]).expanduser().resolve()
    missing = missing_parts(build_root)
    if not missing:
        return 0

    print(
        f"error: TsFile C++ build not found at {build_root}",
        file=sys.stderr,
    )
    for part in missing:
        print(f"       missing directory: {part}", file=sys.stderr)
    print(
        "       Run './mvnw -Pwith-cpp package' first, "
        "or set -Dtsfile.cpp.build=<path>.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
