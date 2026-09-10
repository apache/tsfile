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

"""Opt-in native configure/install/link regressions (requires CMake and LZ4)."""

import os
import re
import shlex
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "cpp/cmake/tests/projects"


class StaticANTLRPackageTest(unittest.TestCase):
    """Exercise installed metadata without compiling the ANTLR runtime."""

    def test_installed_system_antlr_requires_compatible_runtime(self):
        with tempfile.TemporaryDirectory(prefix="tsfile-antlr-config-") as temporary:
            directory = Path(temporary)
            producer = directory / "producer"
            (producer / "cmake").mkdir(parents=True)
            shutil.copyfile(
                ROOT / "cpp/cmake/TsFileStaticDependencies.cmake.in",
                producer / "cmake/TsFileStaticDependencies.cmake.in",
            )
            (producer / "CMakeLists.txt").write_text(textwrap.dedent("""\
                    cmake_minimum_required(VERSION 3.11)
                    project(StaticANTLRPackage NONE)
                    include(CMakePackageConfigHelpers)
                    include("${TSFILE_CMAKE_DIR}/DependencySource.cmake")
                    include("${TSFILE_CMAKE_DIR}/ANTLR4Dependency.cmake")
                    set(CMAKE_INSTALL_LIBDIR lib)
                    set(ENABLE_ANTLR4 ON)
                    set(ENABLE_THREADS OFF)
                    set(TSFILE_BUILD_SHARED OFF)
                    add_library(tsfile INTERFACE)
                    add_library(TsFile::ANTLR4 INTERFACE IMPORTED)
                    include("${TSFILE_CMAKE_DIR}/TsFileStaticDependencies.cmake")
                    configure_package_config_file(
                        "${TSFILE_CMAKE_DIR}/TsFileConfig.cmake.in"
                        "${CMAKE_CURRENT_BINARY_DIR}/TsFileConfig.cmake"
                        INSTALL_DESTINATION lib/cmake/TsFile)
                    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/TsFileConfig.cmake"
                        DESTINATION lib/cmake/TsFile)
                    install(TARGETS tsfile EXPORT TsFileTargets)
                    install(EXPORT TsFileTargets NAMESPACE TsFile::
                        DESTINATION lib/cmake/TsFile)
                    """))
            consumer = directory / "consumer"
            consumer.mkdir()
            (consumer / "CMakeLists.txt").write_text(textwrap.dedent("""\
                    cmake_minimum_required(VERSION 3.11)
                    project(StaticANTLRConsumer NONE)
                    find_package(TsFile CONFIG REQUIRED)
                    get_target_property(runtime TsFile::Static_ANTLR4
                        INTERFACE_LINK_LIBRARIES)
                    if (NOT runtime STREQUAL EXPECTED_TARGET OR NOT TARGET ${runtime})
                        message(FATAL_ERROR "Static TsFile lost its system target")
                    endif ()
                    if (NOT TARGET utf8cpp)
                        message(FATAL_ERROR "Static TsFile lost utf8cpp compatibility")
                    endif ()
                    """))

            def configure(source, build, *options):
                return subprocess.run(
                    [
                        "cmake",
                        "-S",
                        str(source),
                        "-B",
                        str(build),
                        "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=FALSE",
                        "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=FALSE",
                        "-DCMAKE_FIND_USE_CMAKE_SYSTEM_PATH=FALSE",
                        "-DCMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH=FALSE",
                        f"-DCMAKE_MAKE_PROGRAM={shutil.which('make')}",
                        "-G",
                        "Unix Makefiles",
                        *options,
                    ],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )

            def package(prefix, version, target, version_field):
                prefix.mkdir(parents=True)
                (prefix / "utf8cppConfig.cmake").write_text(
                    "add_library(utf8cpp INTERFACE IMPORTED)\n"
                )
                if version == "missing-package":
                    return
                content = ""
                if version is not None:
                    content += f'set({version_field} "{version}")\n'
                if target is not None:
                    content += (
                        f"add_library({target} INTERFACE IMPORTED)\n"
                        f"set_property(TARGET {target} PROPERTY "
                        "INTERFACE_LINK_LIBRARIES utf8cpp)\n"
                    )
                (prefix / "antlr4-runtime-config.cmake").write_text(content)

            targets = (
                "antlr4_static",
                "antlr4-runtime::antlr4_static",
                "antlr4_shared",
                "antlr4-runtime::antlr4_shared",
            )
            for index, target in enumerate(targets):
                build = directory / f"producer-build-{index}"
                stage = directory / f"stage-{index}"
                build_package = directory / f"build-package-{index}"
                package(build_package, "4.9.3", target, "ANTLR_VERSION")
                result = configure(
                    producer,
                    build,
                    f"-DTSFILE_CMAKE_DIR={ROOT / 'cpp/cmake'}",
                    "-DTSFILE_DEPENDENCY_SOURCE=SYSTEM",
                    f"-DCMAKE_PREFIX_PATH={build_package}",
                    f"-DCMAKE_INSTALL_PREFIX={stage}",
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                result = subprocess.run(
                    ["cmake", "--install", str(build)],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                relocated = directory / f"relocated-{index}"
                stage.rename(relocated)
                cases = (
                    ("4.9.3", target, True),
                    ("4.12.0", target, True),
                    ("4.13.0", target, False),
                    ("4.13.2", target, False),
                    ("4.9.2", target, False),
                    (None, target, False),
                    ("4.9.3", None, False),
                    ("missing-package", None, False),
                )
                for field in ("antlr4-runtime_VERSION", "ANTLR_VERSION"):
                    for case, (version, consumer_target, succeeds) in enumerate(cases):
                        with self.subTest(
                            target=target,
                            field=field,
                            version=version,
                            consumer_target=consumer_target,
                        ):
                            suffix = f"{index}-{field}-{case}"
                            runtime = directory / f"runtime-{suffix}"
                            package(runtime, version, consumer_target, field)
                            result = configure(
                                consumer,
                                directory / f"consumer-{suffix}",
                                f"-DCMAKE_PREFIX_PATH={relocated};{runtime}",
                                f"-DEXPECTED_TARGET={target}",
                            )
                            if succeeds:
                                self.assertEqual(result.returncode, 0, result.stdout)
                            else:
                                self.assertNotEqual(
                                    result.returncode,
                                    0,
                                    f"Installed static TsFile accepted {version}:\n"
                                    + result.stdout,
                                )
                                diagnostic = " ".join(result.stdout.split())
                                self.assertIn("compatible system ANTLR4", diagnostic)
                                self.assertIn(">=4.9.3", diagnostic)
                                self.assertIn("<4.13.0", diagnostic)


@unittest.skipUnless(
    os.environ.get("TSFILE_RUN_INSTALL_TESTS") == "1", "opt-in native builds"
)
class NativeInstallTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="tsfile-install-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.build = self.directory / "build"
        self.stage = self.directory / "stage"

    def run_command(self, *arguments, env=None):
        result = subprocess.run(
            [str(argument) for argument in arguments],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-14000:])
        return result.stdout.strip()

    def configure(self, *options):
        arguments = [
            "cmake",
            "-S",
            ROOT / "cpp",
            "-B",
            self.build,
            "-DCMAKE_BUILD_TYPE=Release",
            "-DBUILD_TEST=OFF",
            "-DBUILD_TOOLS=OFF",
            "-DTSFILE_DEPENDENCY_SOURCE=SYSTEM",
            "-DTSFILE_ENABLE_NATIVE_ARCH=OFF",
            f"-DCMAKE_INSTALL_PREFIX={self.stage}",
        ]
        arguments.extend(
            f"-DENABLE_{name}=OFF"
            for name in (
                "ANTLR4",
                "SNAPPY",
                "LZ4",
                "LZOKAY",
                "ZLIB",
                "ZSTD",
                "LZMA2",
                "SIMD",
            )
        )
        self.run_command(*arguments, *options)

    def install(self):
        self.run_command("cmake", "--build", self.build, "--parallel", "8")
        self.run_command("cmake", "--install", self.build, "--config", "Release")
        relocated = self.directory / "relocated"
        self.stage.rename(relocated)
        self.stage = relocated
        for path in self.stage.rglob("*.cmake"):
            self.assertNotIn(str(self.build), path.read_text(), str(path))
            self.assertNotIn(str(ROOT / "cpp"), path.read_text(), str(path))

    def consumer(self, fixture, target, env=None, *options):
        build = self.directory / fixture
        self.run_command(
            "cmake",
            "-S",
            FIXTURES / fixture,
            "-B",
            build,
            f"-DCMAKE_PREFIX_PATH={self.stage}",
            *options,
            env=env,
        )
        self.run_command("cmake", "--build", build, "--parallel", "8", env=env)
        self.run_command(build / target, env=env)

    def test_static_system_lz4_install_and_relocated_consumer(self):
        self.configure("-DTSFILE_BUILD_SHARED=OFF", "-DENABLE_LZ4=ON")
        self.install()
        self.consumer("InstalledConsumer", "tsfile_installed_consumer")

    def test_static_bundled_install_and_relocated_consumer(self):
        self.configure(
            "-DTSFILE_BUILD_SHARED=OFF",
            "-DTSFILE_DEPENDENCY_SOURCE=BUNDLED",
            "-DENABLE_ANTLR4=ON",
            "-DENABLE_ZLIB=ON",
            "-DENABLE_LZOKAY=ON",
            "-DTSFILE_DEPENDENCY_OFFLINE=ON",
            f"-DTSFILE_DEPENDENCY_CACHE={os.environ['TSFILE_TEST_DEPENDENCY_CACHE']}",
        )
        self.install()
        self.consumer("InstalledConsumer", "tsfile_installed_consumer")

    def test_multiarch_pkgconfig_install_and_relocated_consumers(self):
        self.configure("-DCMAKE_INSTALL_LIBDIR=lib/x86_64-linux-gnu")
        self.install()
        env = dict(
            os.environ,
            PKG_CONFIG_PATH=str(self.stage / "lib/x86_64-linux-gnu/pkgconfig"),
        )
        prefix = self.run_command(
            "pkg-config", "--variable=prefix", "libtsfile", env=env
        )
        self.assertEqual(Path(prefix).resolve(), self.stage.resolve())
        self.consumer("PkgConfigConsumer", "tsfile_pkgconfig_consumer", env)
        self.run_command(
            self.directory / "PkgConfigConsumer/tsfile_pkgconfig_cpp_consumer", env=env
        )

    def test_installed_smoke_requires_a_library_symbol(self):
        self.configure()
        self.install()
        result = subprocess.run(
            [
                *shlex.split(os.environ.get("CXX", "c++")),
                "-std=c++11",
                f"-I{self.stage / 'include'}",
                f"-I{self.stage / 'include/tsfile'}",
                str(FIXTURES / "InstalledConsumer/main.cc"),
                "-o",
                str(self.directory / "unlinked"),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.assertNotEqual(
            result.returncode, 0, "Smoke consumer linked successfully without libtsfile"
        )
        self.assertIn("get_global_time_encoding", result.stdout)
        self.consumer("InstalledConsumer", "tsfile_installed_consumer")
        for formula in ("tsfile.rb", "tsfile-dev.rb.in"):
            source = re.search(
                r"write <<~CPP\n(.*?)\n\s+CPP",
                (ROOT / "packaging/homebrew" / formula).read_text(),
                re.S,
            ).group(1)
            source_path = self.directory / f"{formula}.cc"
            source_path.write_text(textwrap.dedent(source))
            command = [
                *shlex.split(os.environ.get("CXX", "c++")),
                "-std=c++11",
                f"-I{self.stage / 'include'}",
                str(source_path),
                "-o",
                str(self.directory / "brew-consumer"),
            ]
            unlinked = subprocess.run(
                command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            self.assertNotEqual(unlinked.returncode, 0, formula)
            self.assertIn("get_global_time_encoding", unlinked.stdout)
            self.run_command(
                *command,
                f"-L{self.stage / 'lib'}",
                f"-Wl,-rpath,{self.stage / 'lib'}",
                "-ltsfile",
            )
            self.run_command(self.directory / "brew-consumer")

    def test_static_msvc_runtime_reaches_dependency_and_project_targets(self):
        self.configure(
            "-DTSFILE_MSVC_STATIC_RUNTIME=ON",
            "-DTSFILE_DEPENDENCY_SOURCE=BUNDLED",
            "-DENABLE_ZLIB=ON",
            "-DTSFILE_DEPENDENCY_OFFLINE=ON",
            f"-DTSFILE_DEPENDENCY_CACHE={os.environ['TSFILE_TEST_DEPENDENCY_CACHE']}",
            f"-DCMAKE_PROJECT_TsFile_CPP_INCLUDE={ROOT / 'cpp/cmake/tests/CheckStaticMSVCRuntime.cmake'}",
        )


if __name__ == "__main__":
    unittest.main()
