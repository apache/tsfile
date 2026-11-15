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

"""Set an explicit Python development wheel version in a build checkout."""

import argparse
from pathlib import Path
import re

DEV_VERSION = re.compile(
    r"(?:0|[1-9][0-9]*)(?:\.(?:0|[1-9][0-9]*)){2}\.dev(?:0|[1-9][0-9]*)"
)
VERSION_ASSIGNMENT = re.compile(r"""(?m)^(version[ \t]*=[ \t]*)(["'])([^"'\r\n]+)\2""")
PROJECT_TABLE = re.compile(r"(?ms)^\[project\][ \t]*\r?\n(?P<body>.*?)(?=^\[|\Z)")


def _replace_version(text, version, path):
    matches = list(VERSION_ASSIGNMENT.finditer(text))
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one version declaration in {path}")
    match = matches[0]
    return text[: match.start(3)] + version + text[match.end(3) :]


def prepare_version(python_root, version):
    if DEV_VERSION.fullmatch(version) is None:
        raise ValueError(
            "Python development version must be MAJOR.MINOR.PATCH.devN "
            "(for example 2.5.1.dev0 or 2.5.1.dev2)"
        )
    setup_path = python_root / "setup.py"
    project_path = python_root / "pyproject.toml"
    setup = _replace_version(
        setup_path.read_text(encoding="utf-8"), version, setup_path
    )
    project = project_path.read_text(encoding="utf-8")
    table = PROJECT_TABLE.search(project)
    if table is None:
        raise ValueError(f"Missing [project] table in {project_path}")
    body = _replace_version(table.group("body"), version, project_path)
    project = project[: table.start("body")] + body + project[table.end("body") :]
    # Validate both declarations before changing either file. Only the Python
    # package metadata changes; the native library keeps its source/ABI version.
    setup_path.write_text(setup, encoding="utf-8")
    project_path.write_text(project, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--python-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "python",
    )
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    prepare_version(args.python_root, args.version)
    if args.github_output is not None:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"\npython_version={args.version}\n")
    print(f"Python wheel version: {args.version}")


if __name__ == "__main__":
    main()
