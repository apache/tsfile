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

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPOSITORY_ROOT / "packaging/scripts/assemble_native_packages.py"
VERSIONS = {
    "archive_version": "2.5.0-dev0.20260910.123.1.gabcdef1",
    "homebrew_version": "2.5.0.dev0.20260910.123.1.gabcdef1",
    "logical_version": "2.5.0.dev0+20260910.123.1.gabcdef1",
}
SOURCE = {
    "commit": "abcdef1234567890abcdef1234567890abcdef12",
    "repository": "apache/tsfile",
}


def load_assembler_module():
    if not MODULE_PATH.is_file():
        return None
    spec = importlib.util.spec_from_file_location(
        "assemble_native_packages", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load native package assembler from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AssembleNativePackagesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_assembler_module()

    def require_module(self):
        self.assertIsNotNone(
            self.module, "native package assembler must exist for final bundle assembly"
        )
        return self.module

    def write_fixture(self, directory):
        files = {
            "deb/tsfile_2.5.0-dev_amd64.deb": b"deb artifact\n",
            "rpm/tsfile-2.5.0-dev.x86_64.rpm": b"rpm artifact\n",
            "homebrew/Formula/tsfile-dev.rb": b"formula artifact\n",
            "homebrew/bottles/tsfile-dev.bottle.tar.gz": b"bottle artifact\n",
            "homebrew/bottles/tsfile-dev.bottle.json": b"bottle metadata\n",
            "windows/tsfile-2.5.0-dev-windows-x86_64.zip": b"windows artifact\n",
            "sdk/ubuntu22.04-amd64/tsfile-sdk-ubuntu22.04-amd64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz": b"sdk ubuntu\n",
            "sdk/almalinux9-x86_64/tsfile-sdk-almalinux9-x86_64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz": b"sdk almalinux\n",
            "sdk/macos-arm64/tsfile-sdk-macos-arm64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz": b"sdk macos arm64\n",
            "sdk/macos-x86_64/tsfile-sdk-macos-x86_64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz": b"sdk macos x86_64\n",
            "sdk/windows-msvc-x86_64/tsfile-sdk-windows-msvc-x86_64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz": b"sdk windows\n",
            "python/ubuntu22.04-x86_64/tsfile-2.5.0.dev0.20260910.123.1.gabcdef1-cp311-cp311-linux_x86_64.whl": b"python wheel\n",
        }
        for relative_path, contents in files.items():
            path = directory / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(contents)
        return files

    def test_assembles_publishable_bundle_with_literal_metadata(self):
        module = self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            input_directory = Path(temporary_directory) / "input"
            output_directory = Path(temporary_directory) / "bundle"
            self.write_fixture(input_directory)

            manifest = module.assemble(
                input_directory, output_directory, VERSIONS, SOURCE
            )

            expected_paths = [
                "deb/ubuntu22.04-amd64/tsfile_2.5.0-dev_amd64.deb",
                "homebrew/Formula/tsfile-dev.rb",
                "homebrew/bottles/tsfile-dev.bottle.json",
                "homebrew/bottles/tsfile-dev.bottle.tar.gz",
                "python/wheels/ubuntu22.04-x86_64/tsfile-2.5.0.dev0.20260910.123.1.gabcdef1-cp311-cp311-linux_x86_64.whl",
                "rpm/almalinux9-x86_64/tsfile-2.5.0-dev.x86_64.rpm",
                "sdk/almalinux9-x86_64/tsfile-sdk-almalinux9-x86_64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz",
                "sdk/macos-arm64/tsfile-sdk-macos-arm64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz",
                "sdk/macos-x86_64/tsfile-sdk-macos-x86_64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz",
                "sdk/ubuntu22.04-amd64/tsfile-sdk-ubuntu22.04-amd64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz",
                "sdk/windows-msvc-x86_64/tsfile-sdk-windows-msvc-x86_64-2.5.0-dev0.20260910.123.1.gabcdef1.tar.gz",
                "windows/tsfile-2.5.0-dev-windows-x86_64.zip",
            ]
            self.assertEqual(
                sorted(
                    path.relative_to(output_directory).as_posix()
                    for path in output_directory.rglob("*")
                    if path.is_file()
                    and path.name not in {"SHA256SUMS", "manifest.json"}
                ),
                expected_paths,
            )
            self.assertEqual(
                json.loads((output_directory / "manifest.json").read_text()), manifest
            )
            self.assertEqual(manifest["source"], SOURCE)
            self.assertEqual(manifest["versions"], VERSIONS)
            self.assertEqual(
                {artifact["family"] for artifact in manifest["artifacts"]},
                {"deb", "homebrew", "python", "rpm", "sdk", "windows"},
            )
            self.assertEqual(
                {artifact["platform"] for artifact in manifest["artifacts"]},
                {
                    "almalinux9-x86_64",
                    "homebrew",
                    "macos-arm64",
                    "macos-x86_64",
                    "ubuntu22.04-amd64",
                    "ubuntu22.04-x86_64",
                    "windows-msvc-x86_64",
                },
            )
            for artifact in manifest["artifacts"]:
                output_path = output_directory / artifact["path"]
                input_path = next(
                    path
                    for path in input_directory.rglob(artifact["filename"])
                    if path.is_file()
                )
                self.assertEqual(artifact["size"], input_path.stat().st_size)
                self.assertEqual(
                    artifact["sha256"],
                    hashlib.sha256(input_path.read_bytes()).hexdigest(),
                )

    def test_verifies_assembler_checksum_lines_sorted_by_path(self):
        module = self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            input_directory = Path(temporary_directory) / "input"
            output_directory = Path(temporary_directory) / "bundle"
            self.write_fixture(input_directory)
            module.assemble(input_directory, output_directory, VERSIONS, SOURCE)

            checksum_lines = (
                (output_directory / "SHA256SUMS")
                .read_text(encoding="utf-8")
                .splitlines()
            )
            self.assertNotEqual(
                checksum_lines,
                sorted(checksum_lines),
                "fixture must distinguish path ordering from hash ordering",
            )
            self.assertIsNone(module.verify_bundle(output_directory))

    def test_rejects_unsafe_versions_and_empty_source_identity(self):
        module = self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            input_directory = Path(temporary_directory) / "input"
            self.write_fixture(input_directory)
            for index, (field, value) in enumerate(
                (
                    ("archive_version", "../../latest"),
                    ("homebrew_version", "../latest"),
                    ("archive_version", ""),
                    ("homebrew_version", "version\\latest"),
                )
            ):
                with self.subTest(field=field, value=value):
                    versions = {**VERSIONS, field: value}
                    with self.assertRaisesRegex(ValueError, "safe path component"):
                        module.assemble(
                            input_directory,
                            Path(temporary_directory) / f"invalid-{index}",
                            versions,
                            SOURCE,
                        )
            for index, (field, value) in enumerate(
                (("commit", ""), ("repository", "   "))
            ):
                with self.subTest(field=field, value=value):
                    source = {**SOURCE, field: value}
                    with self.assertRaisesRegex(ValueError, "nonempty"):
                        module.assemble(
                            input_directory,
                            Path(temporary_directory) / f"empty-{index}",
                            VERSIONS,
                            source,
                        )

    def test_rejects_unknown_files(self):
        module = self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            input_directory = Path(temporary_directory) / "input"
            self.write_fixture(input_directory)
            (input_directory / "deb/README.txt").write_text(
                "not a package\n", encoding="utf-8"
            )

            with self.assertRaisesRegex(ValueError, "undeclared file type"):
                module.assemble(
                    input_directory,
                    Path(temporary_directory) / "bundle",
                    VERSIONS,
                    SOURCE,
                )

    def test_rejects_empty_and_symbolic_link_inputs(self):
        module = self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            input_directory = Path(temporary_directory) / "input"
            self.write_fixture(input_directory)
            (input_directory / "rpm/tsfile-2.5.0-dev.x86_64.rpm").write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "empty file"):
                module.assemble(
                    input_directory,
                    Path(temporary_directory) / "empty",
                    VERSIONS,
                    SOURCE,
                )

            self.write_fixture(input_directory)
            os.symlink(
                input_directory / "deb/tsfile_2.5.0-dev_amd64.deb",
                input_directory / "deb/linked.deb",
            )
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                module.assemble(
                    input_directory,
                    Path(temporary_directory) / "linked",
                    VERSIONS,
                    SOURCE,
                )

    def test_rejects_duplicate_destination_names(self):
        module = self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            input_directory = Path(temporary_directory) / "input"
            self.write_fixture(input_directory)
            duplicate = input_directory / "deb/duplicate/tsfile_2.5.0-dev_amd64.deb"
            duplicate.parent.mkdir()
            duplicate.write_bytes(b"another deb artifact\n")

            with self.assertRaisesRegex(ValueError, "duplicate target path"):
                module.assemble(
                    input_directory,
                    Path(temporary_directory) / "bundle",
                    VERSIONS,
                    SOURCE,
                )

    def test_cli_assembles_from_versions_json(self):
        self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            input_directory = temporary_path / "input"
            self.write_fixture(input_directory)
            versions_path = temporary_path / "versions.json"
            versions_path.write_text(json.dumps(VERSIONS), encoding="utf-8")
            output_directory = temporary_path / "bundle"

            subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--input-dir",
                    str(input_directory),
                    "--output-dir",
                    str(output_directory),
                    "--versions-json",
                    str(versions_path),
                    "--source-repository",
                    SOURCE["repository"],
                    "--source-commit",
                    SOURCE["commit"],
                ],
                check=True,
            )
            manifest_path = output_directory / "manifest.json"
            self.assertTrue(manifest_path.is_file(), "CLI must write manifest.json")
            self.assertEqual(
                json.loads(manifest_path.read_text(encoding="utf-8"))["source"], SOURCE
            )


if __name__ == "__main__":
    unittest.main()
