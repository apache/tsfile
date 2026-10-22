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

"""Exercise generated SDK runtime metadata and runtime-only wheel staging."""

import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "tsfile_build_support", ROOT / "python/_build_support.py"
)
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)


class SDKBuildConfigTest(unittest.TestCase):
    def test_isolated_antlr_lookup_inherits_the_selected_compiler(self):
        compiler = shutil.which("c++") or shutil.which("g++") or shutil.which("clang++")
        with tempfile.TemporaryDirectory() as temporary:
            subprocess.run(
                [
                    "cmake",
                    f"-DTEST_BINARY_ROOT={temporary}",
                    "-DTEST_CMAKE_GENERATOR=Unix Makefiles",
                    f"-DTEST_CMAKE_MAKE_PROGRAM={shutil.which('make')}",
                    f"-DTEST_CMAKE_CXX_COMPILER={compiler}",
                    "-P",
                    str(ROOT / "cpp/cmake/tests/ANTLR4DependencyTest.cmake"),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=120,
            )

    def test_cmake_exports_resolved_crt_per_configuration_and_after_install(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "probe.cc").write_text("int probe() { return 0; }\n")
            (root / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.15)\n"
                "project(SDKMetadata LANGUAGES CXX)\n"
                "include(GNUInstallDirs)\n"
                "add_library(tsfile SHARED probe.cc)\n"
                "set_target_properties(tsfile PROPERTIES VERSION 2.5.0 SOVERSION 1)\n"
                "set(TSFILE_BUILD_SHARED ON)\n"
                "set(TSFILE_PACKAGE_VERSION 2.5.0)\n"
                # Test the metadata producer with MSVC's standard runtime
                # property while using the host compiler only for configure.
                "set(MSVC TRUE)\n"
                "set(CMAKE_CXX_COMPILER_ID MSVC)\n"
                'set_property(TARGET tsfile PROPERTY MSVC_RUNTIME_LIBRARY "${TEST_RUNTIME}")\n'
                f'include("{ROOT / "cpp/cmake/TsFileBuildConfig.cmake"}")\n'
            )
            for runtime, configuration, flag in [
                ("", "Release", "/MD"),
                ("", "Debug", "/MDd"),
                ("MultiThreaded$<$<CONFIG:Debug>:Debug>", "Release", "/MT"),
                ("MultiThreaded$<$<CONFIG:Debug>:Debug>", "Debug", "/MTd"),
                ("MultiThreaded$<$<CONFIG:Debug>:Debug>DLL", "Release", "/MD"),
                ("MultiThreaded$<$<CONFIG:Debug>:Debug>DLL", "Debug", "/MDd"),
            ]:
                with self.subTest(runtime=runtime, configuration=configuration):
                    build = root / (flag[1:] + "-build")
                    subprocess.run(
                        [
                            "cmake",
                            "-S",
                            str(root),
                            "-B",
                            str(build),
                            f"-DCMAKE_BUILD_TYPE={configuration}",
                            f"-DTEST_RUNTIME={runtime}",
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    generated = list(build.rglob("tsfile-build-config.json"))
                    self.assertEqual(len(generated), 1)
                    config = support.read_sdk_config(
                        build, generated[0].parent / "library"
                    )
                    self.assertEqual(support.msvc_runtime_flag(config), flag)
                    self.assertEqual(config["configuration"], configuration)
                    prefix = root / (flag[1:] + "-sdk")
                    subprocess.run(
                        [
                            "cmake",
                            "--install",
                            str(build),
                            "--prefix",
                            str(prefix),
                            "--config",
                            configuration,
                        ],
                        check=True,
                        capture_output=True,
                    )
                    moved = root / (flag[1:] + "-relocated")
                    shutil.move(prefix, moved)
                    self.assertEqual(
                        support.read_sdk_config(moved, moved / "lib/library"), config
                    )

    def test_missing_or_invalid_runtime_metadata_fails_instead_of_guessing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(FileNotFoundError, r"Rebuild/install C\+\+"):
                support.read_sdk_config(root, root / "library")
            for config in [
                {"compiler": "MSVC", "msvc_runtime": ""},
                {"compiler": "GNU", "msvc_runtime": "MultiThreaded"},
            ]:
                with self.subTest(config=config), self.assertRaises(ValueError):
                    support.msvc_runtime_flag(config)

    def test_staging_one_soname_removes_stale_alias_copies(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = root / "tsfile"
            package.mkdir()
            source = root / "libtsfile.so.2.5.0"
            source.write_bytes(b"native-library")
            for name in ("libtsfile.so", "libtsfile.so.1", "libtsfile.so.2.5.0"):
                (package / name).write_bytes(b"old-copy")
            target = support.copy_runtime_library(
                package, source, {"runtime_library": "libtsfile.so.1"}
            )
            self.assertEqual([p.name for p in package.iterdir()], ["libtsfile.so.1"])
            self.assertEqual(target.read_bytes(), source.read_bytes())

    def test_runtime_filename_cannot_escape_package(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            info = root / "share/tsfile"
            info.mkdir(parents=True)
            (info / "tsfile-build-config.json").write_text(
                json.dumps({"schema_version": 1, "runtime_library": "../tsfile.dll"})
            )
            with self.assertRaisesRegex(ValueError, "shared runtime library"):
                support.read_sdk_config(root, root / "lib/tsfile.dll")
