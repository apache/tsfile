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

"""Exercise explicit development versions without building the native library."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prepare_python_dev_version.py"
SPEC = importlib.util.spec_from_file_location("python_dev_version", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

SETUP = 'version = "2.5.1.dev"  # package version\nname = "tsfile"\n'
PROJECT = (
    '[project]\nname = "tsfile"\nversion = "2.5.1.dev"\n'
    '[tool.example]\nversion = "leave-this-alone"\n'
)


class PythonDevVersionTest(unittest.TestCase):
    def write_fixture(self, root):
        (root / "setup.py").write_text(SETUP)
        (root / "pyproject.toml").write_text(PROJECT)
        (root / "CMakeLists.txt").write_text("set(TsFile_CPP_VERSION 2.5.1.dev)\n")

    def test_sets_dev0_and_dev2_in_both_python_declarations(self):
        for version in ("2.5.1.dev0", "2.5.1.dev2"):
            with self.subTest(
                version=version
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write_fixture(root)
                MODULE.prepare_version(root, version)
                self.assertEqual(
                    (root / "setup.py").read_text(), SETUP.replace("2.5.1.dev", version)
                )
                self.assertEqual(
                    (root / "pyproject.toml").read_text(),
                    PROJECT.replace("2.5.1.dev", version),
                )
                self.assertEqual(
                    (root / "CMakeLists.txt").read_text(),
                    "set(TsFile_CPP_VERSION 2.5.1.dev)\n",
                )
                # A rerun must keep the explicit version instead of appending a suffix.
                MODULE.prepare_version(root, version)
                self.assertEqual(
                    (root / "setup.py").read_text(), SETUP.replace("2.5.1.dev", version)
                )

    def test_rejects_noncanonical_versions_without_changing_sources(self):
        for version in (
            "2.5.1",
            "2.5.1.dev",
            "2.5.1.dev-1",
            "2.5.1.dev02",
            "2.5.1rc1",
            "02.5.1.dev0",
            "2.5.1.dev0+local",
            '2.5.1.dev0"\nother=value',
            "",
        ):
            with self.subTest(
                version=version
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write_fixture(root)
                with self.assertRaisesRegex(ValueError, "MAJOR.MINOR.PATCH.devN"):
                    MODULE.prepare_version(root, version)
                self.assertEqual((root / "setup.py").read_text(), SETUP)
                self.assertEqual((root / "pyproject.toml").read_text(), PROJECT)

    def test_validates_all_source_declarations_before_writing(self):
        cases = (
            (SETUP + 'version = "duplicate"\n', PROJECT),
            (SETUP.replace("version =", "other ="), PROJECT),
            (SETUP, PROJECT.replace("[project]", "[other]")),
            (SETUP, PROJECT.replace('version = "2.5.1.dev"', 'other = "2.5.1.dev"')),
            (
                SETUP,
                PROJECT.replace(
                    'version = "2.5.1.dev"',
                    'version = "2.5.1.dev"\nversion = "duplicate"',
                ),
            ),
        )
        for setup, project in cases:
            with self.subTest(
                setup=setup, project=project
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "setup.py").write_text(setup)
                (root / "pyproject.toml").write_text(project)
                with self.assertRaises(ValueError):
                    MODULE.prepare_version(root, "2.5.1.dev2")
                self.assertEqual((root / "setup.py").read_text(), setup)
                self.assertEqual((root / "pyproject.toml").read_text(), project)

    def test_cli_preserves_existing_github_outputs_and_sets_requested_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_fixture(root)
            output = root / "github-output"
            output.write_text("existing=value")
            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--python-root",
                    str(root),
                    "--version",
                    "2.5.1.dev2",
                    "--github-output",
                    str(output),
                ],
                check=True,
            )
            self.assertEqual(
                output.read_text(), "existing=value\npython_version=2.5.1.dev2\n"
            )
            self.assertIn('version = "2.5.1.dev2"', (root / "setup.py").read_text())


if __name__ == "__main__":
    unittest.main()
