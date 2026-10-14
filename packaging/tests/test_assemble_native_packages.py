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
    "archive_version": "2.5.0",
    "homebrew_version": "2.5.0",
    "logical_version": "2.5.0",
}
SOURCE = {
    "commit": "abcdef1234567890abcdef1234567890abcdef12",
    "repository": "apache/tsfile",
}


PYTHON_TAGS = ("cp39", "cp310", "cp311", "cp312", "cp313", "cp314")
WHEEL_VERSION = "2.5.0"
WHEEL_PLATFORMS = {
    "manylinux_2_28-x86_64": "manylinux_2_28_x86_64",
    "manylinux_2_28-aarch64": "manylinux_2_28_aarch64",
    "macos12-x86_64": "macosx_12_0_x86_64",
    "macos12-arm64": "macosx_12_0_arm64",
    "windows-msvc-x86_64": "win_amd64",
}
PYTHON_SDK_PLATFORMS = tuple(
    platform for platform in WHEEL_PLATFORMS if platform != "windows-msvc-x86_64"
)


def wheel_filename(platform, tag="cp311", version=WHEEL_VERSION):
    return f"tsfile-{version}-{tag}-{tag}-{WHEEL_PLATFORMS[platform]}.whl"


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
            "deb/tsfile_2.5.0-1_amd64.deb": b"deb artifact\n",
            "deb/tsfile_2.5.0-1_arm64.deb": b"arm64 deb artifact\n",
            "rpm/tsfile-2.5.0-1.el9.x86_64.rpm": b"rpm artifact\n",
            "rpm/tsfile-2.5.0-1.el9.aarch64.rpm": b"aarch64 rpm artifact\n",
            "homebrew/Formula/tsfile.rb": b"formula artifact\n",
            "homebrew/bottles/tsfile.bottle.tar.gz": b"bottle artifact\n",
            "homebrew/bottles/tsfile.bottle.json": b"bottle metadata\n",
            "windows/tsfile-2.5.0-windows-x86_64.zip": b"windows artifact\n",
            "windows/tsfile-2.5.0-windows-arm64.zip": b"windows arm64 artifact\n",
            "sdk/ubuntu22.04-amd64/tsfile-sdk-ubuntu22.04-amd64-2.5.0.tar.gz": b"sdk ubuntu\n",
            "sdk/ubuntu22.04-arm64/tsfile-sdk-ubuntu22.04-arm64-2.5.0.tar.gz": b"sdk ubuntu arm64\n",
            "sdk/almalinux9-x86_64/tsfile-sdk-almalinux9-x86_64-2.5.0.tar.gz": b"sdk almalinux\n",
            "sdk/almalinux9-aarch64/tsfile-sdk-almalinux9-aarch64-2.5.0.tar.gz": b"sdk almalinux arm64\n",
            "sdk/macos-arm64/tsfile-sdk-macos-arm64-2.5.0.tar.gz": b"sdk macos arm64\n",
            "sdk/macos-x86_64/tsfile-sdk-macos-x86_64-2.5.0.tar.gz": b"sdk macos x86_64\n",
            "sdk/windows-msvc-x86_64/tsfile-sdk-windows-msvc-x86_64-2.5.0.tar.gz": b"sdk windows\n",
            "sdk/windows-msvc-arm64/tsfile-sdk-windows-msvc-arm64-2.5.0.tar.gz": b"sdk windows arm64\n",
        }
        for platform in PYTHON_SDK_PLATFORMS:
            files[
                f"sdk/{platform}/tsfile-sdk-{platform}-{VERSIONS['archive_version']}.tar.gz"
            ] = b"Python compatibility SDK\n"
        for platform in WHEEL_PLATFORMS:
            for tag in PYTHON_TAGS:
                files[f"python/{platform}/{wheel_filename(platform, tag)}"] = (
                    b"Python wheel\n"
                )
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
                "deb/ubuntu22.04-amd64/tsfile_2.5.0-1_amd64.deb",
                "deb/ubuntu22.04-arm64/tsfile_2.5.0-1_arm64.deb",
                "homebrew/Formula/tsfile.rb",
                "homebrew/bottles/tsfile.bottle.json",
                "homebrew/bottles/tsfile.bottle.tar.gz",
                "rpm/almalinux9-aarch64/tsfile-2.5.0-1.el9.aarch64.rpm",
                "rpm/almalinux9-x86_64/tsfile-2.5.0-1.el9.x86_64.rpm",
                "sdk/almalinux9-aarch64/tsfile-sdk-almalinux9-aarch64-2.5.0.tar.gz",
                "sdk/almalinux9-x86_64/tsfile-sdk-almalinux9-x86_64-2.5.0.tar.gz",
                "sdk/macos-arm64/tsfile-sdk-macos-arm64-2.5.0.tar.gz",
                "sdk/macos-x86_64/tsfile-sdk-macos-x86_64-2.5.0.tar.gz",
                "sdk/ubuntu22.04-amd64/tsfile-sdk-ubuntu22.04-amd64-2.5.0.tar.gz",
                "sdk/ubuntu22.04-arm64/tsfile-sdk-ubuntu22.04-arm64-2.5.0.tar.gz",
                "sdk/windows-msvc-arm64/tsfile-sdk-windows-msvc-arm64-2.5.0.tar.gz",
                "sdk/windows-msvc-x86_64/tsfile-sdk-windows-msvc-x86_64-2.5.0.tar.gz",
                "windows/tsfile-2.5.0-windows-arm64.zip",
                "windows/tsfile-2.5.0-windows-x86_64.zip",
            ]
            expected_paths.extend(
                f"sdk/{platform}/tsfile-sdk-{platform}-{VERSIONS['archive_version']}.tar.gz"
                for platform in PYTHON_SDK_PLATFORMS
            )
            expected_paths.extend(
                f"python/wheels/{platform}/{wheel_filename(platform, tag)}"
                for platform in WHEEL_PLATFORMS
                for tag in PYTHON_TAGS
            )
            expected_paths.sort()
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
                    "almalinux9-aarch64",
                    "almalinux9-x86_64",
                    "homebrew",
                    "macos-arm64",
                    "macos-x86_64",
                    "ubuntu22.04-amd64",
                    "ubuntu22.04-arm64",
                    "manylinux_2_28-x86_64",
                    "manylinux_2_28-aarch64",
                    "macos12-x86_64",
                    "macos12-arm64",
                    "windows-msvc-arm64",
                    "windows-msvc-x86_64",
                },
            )
            wheels = [
                artifact
                for artifact in manifest["artifacts"]
                if artifact["family"] == "python"
            ]
            self.assertEqual(len(wheels), 30)
            for wheel in wheels:
                self.assertEqual(wheel["sdkPlatform"], wheel["platform"])
                self.assertEqual(wheel["wheelVersion"], WHEEL_VERSION)
                self.assertEqual(wheel["abiTag"], wheel["pythonTag"])
                for key in ("targetRepository", "targetPath", "properties"):
                    self.assertNotIn(key, wheel)
            for artifact in manifest["artifacts"]:
                if artifact["family"] == "python":
                    continue
                self.assertIn(
                    artifact["targetRepository"],
                    {"tsfile", "tsfile-debian", "tsfile-rpm"},
                )
                self.assertIn("release/", artifact["targetPath"])
                if artifact["family"] == "deb":
                    self.assertEqual(
                        artifact["properties"]["deb.component"], ["release"]
                    )
            artifact_platforms = {
                artifact["filename"]: artifact["platform"]
                for artifact in manifest["artifacts"]
            }
            self.assertEqual(
                artifact_platforms["tsfile_2.5.0-1_arm64.deb"],
                "ubuntu22.04-arm64",
            )
            self.assertEqual(
                artifact_platforms["tsfile-2.5.0-1.el9.aarch64.rpm"],
                "almalinux9-aarch64",
            )
            self.assertEqual(
                artifact_platforms["tsfile-2.5.0-windows-arm64.zip"],
                "windows-msvc-arm64",
            )
            deb_arm64 = next(
                artifact
                for artifact in manifest["artifacts"]
                if artifact["filename"] == "tsfile_2.5.0-1_arm64.deb"
            )
            self.assertEqual(deb_arm64["properties"]["deb.architecture"], ["arm64"])
            rpm_arm64 = next(
                artifact
                for artifact in manifest["artifacts"]
                if artifact["filename"] == "tsfile-2.5.0-1.el9.aarch64.rpm"
            )
            self.assertEqual(
                rpm_arm64["targetPath"],
                "release/el9/aarch64/tsfile-2.5.0-1.el9.aarch64.rpm",
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

    def test_requires_every_python_abi_on_every_platform(self):
        module = self.require_module()
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for platform in WHEEL_PLATFORMS:
                for tag in PYTHON_TAGS:
                    with self.subTest(platform=platform, tag=tag):
                        source = root / platform / tag
                        self.write_fixture(source)
                        (
                            source / "python" / platform / wheel_filename(platform, tag)
                        ).unlink()
                        with self.assertRaisesRegex(
                            ValueError, "missing Python wheel ABIs"
                        ):
                            module.assemble(source, root / "bundle", VERSIONS, SOURCE)

    def test_rejects_incompatible_wheel_tags_and_duplicate_abis(self):
        module = self.require_module()
        invalid = [
            ("manylinux_2_28-x86_64", "linux_x86_64", "incompatible Python wheel tags"),
            (
                "manylinux_2_28-aarch64",
                "manylinux_2_28_x86_64",
                "incompatible Python wheel tags",
            ),
            ("macos12-arm64", "macosx_13_0_arm64", "incompatible Python wheel tags"),
            ("windows-msvc-x86_64", "win_arm64", "incompatible Python wheel tags"),
        ]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for index, (platform, tag, error) in enumerate(invalid):
                with self.subTest(platform=platform, tag=tag):
                    source = root / f"invalid-{index}"
                    self.write_fixture(source)
                    original = source / "python" / platform / wheel_filename(platform)
                    original.rename(
                        original.with_name(
                            f"tsfile-{WHEEL_VERSION}-cp311-cp311-{tag}.whl"
                        )
                    )
                    with self.assertRaisesRegex(ValueError, error):
                        module.assemble(source, root / "bundle", VERSIONS, SOURCE)
            source = root / "duplicate"
            self.write_fixture(source)
            wheel = (
                source
                / "python/manylinux_2_28-x86_64"
                / wheel_filename("manylinux_2_28-x86_64", version="9.0.0")
            )
            wheel.write_bytes(b"duplicate ABI with a different version")
            with self.assertRaisesRegex(ValueError, "duplicate Python wheel ABI"):
                module.assemble(source, root / "bundle", VERSIONS, SOURCE)

    def test_rejects_different_versions_across_wheel_platforms(self):
        module = self.require_module()
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "input"
            self.write_fixture(source)
            original = (
                source
                / "python/windows-msvc-x86_64"
                / wheel_filename("windows-msvc-x86_64")
            )
            original.rename(
                original.with_name(
                    wheel_filename("windows-msvc-x86_64", version="9.0.0")
                )
            )
            with self.assertRaisesRegex(ValueError, "must all have the same version"):
                module.assemble(
                    source, Path(temporary_directory) / "bundle", VERSIONS, SOURCE
                )

    def test_release_bundle_requires_matching_wheel_version(self):
        module = self.require_module()
        release_versions = {
            "base_version": "2.5.0",
            "logical_version": "2.5.0",
            "archive_version": "2.5.0",
            "homebrew_version": "2.5.0",
        }
        for wheel_version in ("2.5.0", "2.3.2.dev0"):
            with self.subTest(
                wheel_version=wheel_version
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "input"
                output = root / "bundle"
                self.write_fixture(source)
                for wheel in source.rglob("*.whl"):
                    wheel.rename(
                        wheel.with_name(
                            wheel.name.replace(WHEEL_VERSION, wheel_version)
                        )
                    )
                if wheel_version == "2.5.0":
                    manifest = module.assemble(source, output, release_versions, SOURCE)
                    self.assertEqual(manifest["versions"], release_versions)
                    module.verify_bundle(output)
                else:
                    with self.assertRaisesRegex(
                        ValueError, "must match release version 2.5.0"
                    ):
                        module.assemble(source, output, release_versions, SOURCE)
                    self.assertFalse(output.exists())

    def test_rejects_development_or_mismatched_release_versions(self):
        module = self.require_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input"
            self.write_fixture(source)
            for field, value in (
                ("archive_version", "2.5.0.dev"),
                ("homebrew_version", "2.5.0.dev0"),
                ("homebrew_version", "2.6.0"),
                ("logical_version", "2.5.0.dev0+build"),
            ):
                with self.subTest(field=field, value=value):
                    output = root / "bundle"
                    with self.assertRaisesRegex(ValueError, "release version"):
                        module.assemble(
                            source, output, {**VERSIONS, field: value}, SOURCE
                        )
                    self.assertFalse(output.exists())

    def test_rejects_bundles_missing_arm_architectures(self):
        module = self.require_module()

        with tempfile.TemporaryDirectory() as temporary_directory:
            missing_artifacts = (
                (
                    "deb/tsfile_2.5.0-1_arm64.deb",
                    "missing DEB inputs for: ubuntu22.04-arm64",
                ),
                (
                    "rpm/tsfile-2.5.0-1.el9.aarch64.rpm",
                    "missing RPM inputs for: almalinux9-aarch64",
                ),
                (
                    "windows/tsfile-2.5.0-windows-arm64.zip",
                    "missing WINDOWS inputs for: windows-msvc-arm64",
                ),
                (
                    "sdk/ubuntu22.04-arm64/tsfile-sdk-ubuntu22.04-arm64-2.5.0.tar.gz",
                    "missing SDK inputs for: ubuntu22.04-arm64",
                ),
                (
                    "sdk/almalinux9-aarch64/tsfile-sdk-almalinux9-aarch64-2.5.0.tar.gz",
                    "missing SDK inputs for: almalinux9-aarch64",
                ),
                (
                    "sdk/windows-msvc-arm64/tsfile-sdk-windows-msvc-arm64-2.5.0.tar.gz",
                    "missing SDK inputs for: windows-msvc-arm64",
                ),
            )
            for index, (missing_path, error_message) in enumerate(missing_artifacts):
                with self.subTest(missing=missing_path):
                    input_directory = Path(temporary_directory) / f"input-{index}"
                    self.write_fixture(input_directory)
                    (input_directory / missing_path).unlink()

                    with self.assertRaisesRegex(ValueError, error_message):
                        module.assemble(
                            input_directory,
                            Path(temporary_directory) / f"incomplete-{index}",
                            VERSIONS,
                            SOURCE,
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
            (input_directory / "rpm/tsfile-2.5.0-1.el9.x86_64.rpm").write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "empty file"):
                module.assemble(
                    input_directory,
                    Path(temporary_directory) / "empty",
                    VERSIONS,
                    SOURCE,
                )

            self.write_fixture(input_directory)
            os.symlink(
                input_directory / "deb/tsfile_2.5.0-1_amd64.deb",
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
            duplicate = input_directory / "deb/duplicate/tsfile_2.5.0-1_amd64.deb"
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
