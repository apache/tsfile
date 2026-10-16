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

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPOSITORY_ROOT / "packaging/scripts/native_package_versions.py"
RELEASE_VERSION = "2.5.0"


def write_release_cmake(root):
    # Successful CLI cases must also run in a development-version checkout.
    path = root / "CMakeLists.txt"
    path.write_text(f"set(TsFile_CPP_VERSION {RELEASE_VERSION})\n", encoding="utf-8")
    return path


class NativePackageVersionsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "native_package_versions", MODULE_PATH
        )
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    def test_reads_release_version(self):
        with tempfile.TemporaryDirectory() as directory:
            cmake_file = write_release_cmake(Path(directory))
            self.assertEqual(self.module.read_cpp_version(cmake_file), RELEASE_VERSION)

    def test_builds_exact_release_package_versions(self):
        self.assertEqual(
            self.module.build_versions("2.5.0"),
            {
                "base_version": "2.5.0",
                "python_version": "2.5.0",
                "logical_version": "2.5.0",
                "deb_version": "2.5.0-1",
                "rpm_version": "2.5.0",
                "rpm_release": "1.el9",
                "archive_version": "2.5.0",
                "homebrew_version": "2.5.0",
            },
        )
        self.assertEqual(self.module.build_versions("3.0.0")["deb_version"], "3.0.0-1")

    def test_rejects_development_prerelease_and_invalid_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            cmake_file = Path(directory) / "CMakeLists.txt"
            for version in (
                "2.5.0.dev",
                "2.5.0.dev0",
                "2.5.0rc1",
                "2.5.dev",
                "2.5",
                "02.5.0",
                "2.5.0+build",
                "2.5.0-release",
                "",
            ):
                with self.subTest(version=version):
                    with self.assertRaisesRegex(ValueError, "release"):
                        self.module.build_versions(version)
                    cmake_file.write_text(f"set(TsFile_CPP_VERSION {version})\n")
                    with self.assertRaisesRegex(ValueError, "release"):
                        self.module.read_cpp_version(cmake_file)

    def test_cli_rejects_development_checkout_before_writing_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cmake_file = root / "CMakeLists.txt"
            cmake_file.write_text("set(TsFile_CPP_VERSION 2.5.0.dev)\n")
            json_out = root / "versions.json"
            github_out = root / "github-output.txt"
            result = subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--cmake-file",
                    str(cmake_file),
                    "--json-out",
                    str(json_out),
                    "--github-output",
                    str(github_out),
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("release TsFile_CPP_VERSION", result.stderr)
            self.assertFalse(json_out.exists())
            self.assertFalse(github_out.exists())

    def test_cli_writes_json_and_github_outputs(self):
        expected = self.module.build_versions(RELEASE_VERSION)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cmake_file = write_release_cmake(root)
            json_out = root / "versions.json"
            github_out = root / "github-output.txt"
            github_out.write_text("existing=value\n")
            subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--cmake-file",
                    str(cmake_file),
                    "--json-out",
                    str(json_out),
                    "--github-output",
                    str(github_out),
                ],
                check=True,
            )
            self.assertEqual(json.loads(json_out.read_text()), expected)
            self.assertEqual(
                github_out.read_text().splitlines(),
                [
                    "existing=value",
                    *[f"{key}={value}" for key, value in expected.items()],
                ],
            )

    def test_cli_preserves_existing_github_output_bytes(self):
        version = RELEASE_VERSION
        for existing in (
            b"",
            b"existing=value",
            b"existing=value\n",
            b"existing=value\r\n",
        ):
            with self.subTest(
                existing=existing
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cmake_file = write_release_cmake(root)
                github_out = root / "github-output.txt"
                github_out.write_bytes(existing)
                subprocess.run(
                    [
                        sys.executable,
                        str(MODULE_PATH),
                        "--cmake-file",
                        str(cmake_file),
                        "--json-out",
                        str(root / "versions.json"),
                        "--github-output",
                        str(github_out),
                    ],
                    check=True,
                )
                separator = b"\n" if existing and not existing.endswith(b"\n") else b""
                self.assertTrue(
                    github_out.read_bytes().startswith(
                        existing + separator + f"base_version={version}\n".encode()
                    )
                )


if __name__ == "__main__":
    unittest.main()
