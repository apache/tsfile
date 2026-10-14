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

"""Compile existing C++ regression tests against an installed SDK only."""

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def package_config(prefix):
    candidates = sorted(
        {
            *prefix.glob("lib*/cmake/TsFile/TsFileConfig.cmake"),
            *prefix.glob("lib*/*/cmake/TsFile/TsFileConfig.cmake"),
        }
    )
    if len(candidates) != 1:
        raise ValueError(
            f"Expected one installed TsFileConfig.cmake under {prefix}: {candidates}"
        )
    return candidates[0].parent


def run(command, **kwargs):
    print("+ " + subprocess.list2cmdline([str(item) for item in command]), flush=True)
    subprocess.run(command, check=True, timeout=900, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--gtest-prefix", type=Path)
    arguments = parser.parse_args()
    prefix = arguments.prefix.resolve()
    build = arguments.build_dir.resolve()
    config = package_config(prefix)
    dependency_prefix = (
        arguments.gtest_prefix.resolve()
        if arguments.gtest_prefix is not None
        else build / "dependencies"
    )
    options = ["-DCMAKE_BUILD_TYPE=Release"]
    if sys.platform == "win32":
        options += [
            "-G",
            "Ninja Multi-Config",
            "-DCMAKE_POLICY_DEFAULT_CMP0091=NEW",
            "-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded",
        ]
    elif sys.platform == "darwin":
        sdk = subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()
        options += [f"-DCMAKE_OSX_SYSROOT={sdk}"]
    if arguments.gtest_prefix is None:
        run(
            [
                "cmake",
                "-S",
                ROOT / "packaging/cmake/InstalledGTest",
                "-B",
                build / "gtest",
                *options,
                f"-DCMAKE_INSTALL_PREFIX={dependency_prefix}",
            ]
        )
        run(
            [
                "cmake",
                "--build",
                build / "gtest",
                "--config",
                "Release",
                "--parallel",
                "4",
            ]
        )
        run(["cmake", "--install", build / "gtest", "--config", "Release"])
    gtest_configs = list(dependency_prefix.glob("lib*/cmake/GTest/GTestConfig.cmake"))
    if len(gtest_configs) != 1:
        raise ValueError(
            f"Expected one installed GoogleTest config under {dependency_prefix}"
        )
    run(
        [
            "cmake",
            "-S",
            ROOT / "cpp/test",
            "-B",
            build / "tests",
            *options,
            f"-DTsFile_DIR={config}",
            f"-DGTest_DIR={gtest_configs[0].parent}",
            f"-DCMAKE_PREFIX_PATH={dependency_prefix}",
            "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF",
            "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF",
        ]
    )
    run(["cmake", "--build", build / "tests", "--config", "Release", "--parallel", "4"])
    environment = os.environ.copy()
    if sys.platform == "win32":
        environment["PATH"] = (
            str(prefix / "bin") + os.pathsep + environment.get("PATH", "")
        )
    run(
        [
            "ctest",
            "--test-dir",
            build / "tests",
            "-C",
            "Release",
            "--output-on-failure",
            "--no-tests=error",
            "--output-junit",
            build / "cpp-results.xml",
        ],
        env=environment,
    )


if __name__ == "__main__":
    main()
