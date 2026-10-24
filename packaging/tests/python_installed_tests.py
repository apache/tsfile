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

"""Install one downloaded wheel and run the existing pytest suite in isolation."""

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def select_wheel(directory, version, python_tag):
    pattern = re.compile(
        rf"^tsfile-{re.escape(version)}-{python_tag}-{python_tag}-.+\.whl$"
    )
    wheels = [
        wheel.resolve()
        for wheel in directory.glob("*.whl")
        if pattern.fullmatch(wheel.name)
    ]
    if len(wheels) != 1:
        raise ValueError(
            f"Expected one tsfile {version} {python_tag} wheel in {directory}: {wheels}"
        )
    return wheels[0]


def run(command, **kwargs):
    print("+ " + subprocess.list2cmdline([str(item) for item in command]), flush=True)
    subprocess.run(command, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheelhouse", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--results", type=Path, required=True)
    arguments = parser.parse_args()
    python_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
    wheel = select_wheel(arguments.wheelhouse, arguments.version, python_tag)
    run(
        [
            sys.executable,
            str(ROOT / "packaging/scripts/check_python_wheel.py"),
            str(wheel),
        ],
        timeout=60,
    )
    # Keep the report outside the temporary cwd even when older Windows
    # pathlib versions leave a nonexistent path relative after resolve().
    results = Path(os.path.abspath(arguments.results))
    results.parent.mkdir(parents=True, exist_ok=True)
    tests = ROOT / "python/tests"
    if not list(tests.glob("test_*.py")):
        raise ValueError(f"No existing Python tests found in {tests}")
    run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--only-binary=:all:",
            str(wheel),
            "pytest",
            "pytest-timeout",
        ],
        timeout=600,
    )
    with tempfile.TemporaryDirectory(prefix="tsfile-installed-pytest-") as directory:
        # -I excludes the checkout and PYTHONPATH from imports. The wheel must
        # work with its bundled native library on this separate runner.
        check = """import faulthandler
faulthandler.dump_traceback_later(30, repeat=True)
from importlib.metadata import version
from pathlib import Path
import sys
import tsfile
faulthandler.cancel_dump_traceback_later()
module = Path(tsfile.__file__).resolve()
checkout = Path(sys.argv[2]).resolve()
if checkout in module.parents:
    raise RuntimeError(f'Imported source checkout: {module}')
if version('tsfile') != sys.argv[1]:
    raise RuntimeError(f'Unexpected installed version: {version("tsfile")}')
print(f'Installed tsfile {version("tsfile")} from {module}', flush=True)
"""
        run(
            [
                sys.executable,
                "-I",
                "-u",
                "-X",
                "faulthandler",
                "-c",
                check,
                arguments.version,
                str(ROOT),
            ],
            cwd=directory,
            timeout=60,
        )
        run(
            [
                sys.executable,
                "-I",
                "-u",
                "-X",
                "faulthandler",
                "-m",
                "pytest",
                "--import-mode=importlib",
                f"--rootdir={tests}",
                "-o",
                f"cache_dir={Path(directory) / 'pytest-cache'}",
                "-ra",
                "--timeout=120",
                "--timeout-method=thread",
                "-o",
                "faulthandler_timeout=60",
                f"--junitxml={results}",
                str(tests),
            ],
            cwd=directory,
            timeout=1800,
        )


if __name__ == "__main__":
    main()
