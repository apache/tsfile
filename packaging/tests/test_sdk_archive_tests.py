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
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


def load_runner():
    spec = importlib.util.spec_from_file_location(
        "sdk_archive_tests", HERE / "sdk_archive_tests.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SDKArchiveTestsTest(unittest.TestCase):
    def archive(self, path, entries):
        with tarfile.open(path, "w:gz") as archive:
            for name, data, link in entries:
                member = tarfile.TarInfo(name)
                if link is not None:
                    member.type = tarfile.SYMTYPE
                    member.linkname = link
                    archive.addfile(member)
                else:
                    member.size = len(data)
                    member.mode = 0o755 if "/bin/" in name else 0o644
                    archive.addfile(member, io.BytesIO(data))

    def test_extracts_relocated_sdk_and_preserves_library_symlink(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sdk.tar.gz"
            self.archive(
                source,
                [
                    ("sdk/include/tsfile/cwrapper/tsfile_cwrapper.h", b"header", None),
                    ("sdk/lib/libtsfile.so.2.5.0", b"library", None),
                    ("sdk/lib/libtsfile.so", b"", "libtsfile.so.2.5.0"),
                    ("sdk/bin/tsfile-cli", b"executable", None),
                ],
            )
            prefix = runner.extract_sdk(source, root / "relocated")
            self.assertEqual(prefix, (root / "relocated/sdk").resolve())
            self.assertEqual((prefix / "lib/libtsfile.so").read_bytes(), b"library")
            self.assertTrue((prefix / "bin/tsfile-cli").stat().st_mode & 0o111)

    def test_rejects_archive_paths_and_links_outside_extraction_directory(self):
        runner = load_runner()
        for name, link in (
            ("../escape", None),
            ("/escape", None),
            ("C:/escape", None),
            ("sdk/link", "../../escape"),
        ):
            with self.subTest(
                name=name, link=link
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "sdk.tar.gz"
                self.archive(source, [(name, b"payload", link)])
                with self.assertRaises(ValueError):
                    runner.extract_sdk(source, root / "extracted")
                self.assertFalse((root / "escape").exists())

    def test_rejects_missing_or_duplicate_sdk_prefix(self):
        runner = load_runner()
        for entries in (
            [],
            [
                ("one/include/tsfile/cwrapper/tsfile_cwrapper.h", b"header", None),
                ("two/include/tsfile/cwrapper/tsfile_cwrapper.h", b"header", None),
            ],
        ):
            with self.subTest(
                entries=entries
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "sdk.tar.gz"
                self.archive(source, entries)
                with self.assertRaisesRegex(
                    ValueError, "Expected one SDK installation prefix"
                ):
                    runner.extract_sdk(source, root / "extracted")

    def test_preserves_existing_extraction_directory(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            destination = root / "existing"
            destination.mkdir()
            sentinel = destination / "keep.txt"
            sentinel.write_text("keep")
            with self.assertRaisesRegex(ValueError, "must be empty"):
                runner.extract_sdk(root / "unused.tar.gz", destination)
            self.assertEqual(sentinel.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
