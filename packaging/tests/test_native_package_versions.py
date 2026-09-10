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
CPP_CMAKE_FILE = REPOSITORY_ROOT / "cpp/CMakeLists.txt"


def load_version_module():
    if not MODULE_PATH.is_file():
        return None
    spec = importlib.util.spec_from_file_location("native_package_versions", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load native package version helper from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NativePackageVersionsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_version_module()

    def require_module(self):
        self.assertIsNotNone(
            self.module, "native_package_versions helper must exist for native package builds"
        )
        return self.module

    def test_reads_development_version_from_cpp_cmake(self):
        module = self.require_module()

        self.assertEqual(module.read_cpp_version(CPP_CMAKE_FILE), "2.5.0.dev")

    def test_builds_exact_development_package_versions(self):
        module = self.require_module()

        self.assertEqual(
            module.build_versions("2.5.0.dev", "20260910", 123, 1, "abcdef123456"),
            {
                "base_version": "2.5.0",
                "logical_version": "2.5.0.dev0+20260910.123.1.gabcdef1",
                "deb_version": "2.5.0~dev0+20260910.123.1.gabcdef1-1",
                "rpm_version": "2.5.0",
                "rpm_release": "0.dev0.20260910.123.1.gabcdef1.el9",
                "archive_version": "2.5.0-dev0.20260910.123.1.gabcdef1",
                "homebrew_version": "2.5.0.dev0.20260910.123.1.gabcdef1",
            },
        )

    def test_rejects_invalid_source_versions(self):
        module = self.require_module()

        for source_version in ("2.5.0", "2.5.dev"):
            with self.subTest(source_version=source_version):
                with self.assertRaises(ValueError):
                    module.build_versions(source_version, "20260910", 123, 1, "abcdef123456")

    def test_rejects_invalid_build_identity_fields(self):
        module = self.require_module()

        invalid_cases = (
            ("2026-09-10", 123, 1, "abcdef123456"),
            ("2026091", 123, 1, "abcdef123456"),
            ("20261340", 123, 1, "abcdef123456"),
            ("20260910", "run", 1, "abcdef123456"),
            ("20260910", 123, "attempt", "abcdef123456"),
            ("20260910", 123, 1, "abcdef"),
            ("20260910", 123, 1, "abcdefg123456"),
        )
        for build_date, run_number, run_attempt, git_sha in invalid_cases:
            with self.subTest(
                build_date=build_date,
                run_number=run_number,
                run_attempt=run_attempt,
                git_sha=git_sha,
            ):
                with self.assertRaises(ValueError):
                    module.build_versions(
                        "2.5.0.dev", build_date, run_number, run_attempt, git_sha
                    )

    def test_cli_writes_json_and_github_outputs(self):
        module = self.require_module()
        expected = module.build_versions("2.5.0.dev", "20260910", 123, 1, "abcdef123456")

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            json_output = temporary_path / "versions.json"
            github_output = temporary_path / "github-output.txt"
            github_output.write_text("existing=value\n", encoding="utf-8")
            subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--cmake-file",
                    str(CPP_CMAKE_FILE),
                    "--build-date",
                    "20260910",
                    "--run-number",
                    "123",
                    "--run-attempt",
                    "1",
                    "--git-sha",
                    "abcdef123456",
                    "--json-out",
                    str(json_output),
                    "--github-output",
                    str(github_output),
                ],
                check=True,
            )

            self.assertEqual(json.loads(json_output.read_text(encoding="utf-8")), expected)
            self.assertEqual(
                github_output.read_text(encoding="utf-8").splitlines(),
                ["existing=value", *[f"{key}={value}" for key, value in expected.items()]],
            )

    def test_cli_separates_unterminated_github_output_without_changing_bytes(self):
        for existing in (
            b"",
            b"existing=value",
            b"existing=value\n",
            b"existing=value\r\n",
        ):
            with self.subTest(
                existing=existing
            ), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "github-output.txt"
                output.write_bytes(existing)
                subprocess.run(
                    [
                        sys.executable,
                        str(MODULE_PATH),
                        "--cmake-file",
                        str(CPP_CMAKE_FILE),
                        "--build-date",
                        "20260910",
                        "--run-number",
                        "123",
                        "--run-attempt",
                        "1",
                        "--git-sha",
                        "abcdef123456",
                        "--json-out",
                        str(Path(directory) / "versions.json"),
                        "--github-output",
                        str(output),
                    ],
                    check=True,
                )
                separator = b"\n" if existing and not existing.endswith(b"\n") else b""
                self.assertTrue(
                    output.read_bytes().startswith(
                        existing + separator + b"base_version=2.5.0\n"
                    ),
                    output.read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
