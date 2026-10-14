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

import importlib.util
from pathlib import Path
import tempfile
import unittest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts/verify_macos_runtime.py"
spec = importlib.util.spec_from_file_location("verify_macos_runtime", MODULE_PATH)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class MacOSRuntimeTest(unittest.TestCase):
    def test_reads_all_dylib_load_commands_but_not_library_identity(self):
        output = """library.dylib:
Load command 0
          cmd LC_ID_DYLIB
      cmdsize 96
         name /opt/homebrew/Cellar/tsfile/2.5.0/lib/libtsfile.1.dylib (offset 24)
Load command 1
          cmd LC_LOAD_DYLIB
      cmdsize 56
         name /usr/lib/libSystem.B.dylib (offset 24)
"""
        for index, command in enumerate(sorted(checker.LOAD_COMMANDS), 2):
            output += f"Load command {index}\n cmd {command}\n name @rpath/libtsfile.1.dylib (offset 24)\n"
        self.assertEqual(
            checker.parse_dependencies(output),
            {"/usr/lib/libSystem.B.dylib", "@rpath/libtsfile.1.dylib"},
        )

    def test_allows_system_libraries_and_packaged_tsfile(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory)
            (prefix / "lib").mkdir()
            (prefix / "bin").mkdir()
            library = prefix / "lib/libtsfile.1.dylib"
            library.touch()
            checker.validate_dependencies(
                prefix / "bin/tsfile-cli",
                {
                    "/usr/lib/libc++.1.dylib",
                    "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation",
                    "@rpath/libtsfile.1.dylib",
                    "@loader_path/../lib/libtsfile.1.dylib",
                    str(library),
                },
                prefix,
            )

    def test_rejects_codec_dylibs_even_when_they_exist(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory)
            (prefix / "lib").mkdir()
            codec = prefix / "lib/liblz4.1.dylib"
            codec.touch()
            for name in (
                "/opt/homebrew/opt/snappy/lib/libsnappy.1.dylib",
                "/usr/local/opt/lz4/lib/liblz4.1.dylib",
                "@rpath/libsnappy.1.dylib",
                str(codec),
            ):
                with self.subTest(name=name), self.assertRaisesRegex(
                    ValueError, "external third-party dylib"
                ):
                    checker.validate_dependencies(
                        prefix / "bin/tsfile-cli", {name}, prefix
                    )

    def test_rejects_missing_or_external_tsfile_even_through_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prefix = root / "sdk"
            (prefix / "lib").mkdir(parents=True)
            external = root / "libtsfile.1.dylib"
            external.touch()
            (prefix / "lib/libtsfile.1.dylib").symlink_to(external)
            for name in (
                "@rpath/libtsfile.2.dylib",
                "@rpath/libtsfile.1.dylib",
                str(external),
                "@loader_path/../../libtsfile.1.dylib",
            ):
                with self.subTest(name=name), self.assertRaisesRegex(
                    ValueError, "unshipped TsFile dylib"
                ):
                    checker.validate_dependencies(
                        prefix / "bin/tsfile-cli", {name}, prefix
                    )

    def test_rejects_empty_or_malformed_inspector_output(self):
        for output in (
            "not a Mach-O file",
            "Load command 0\n cmd LC_LOAD_DYLIB\n",
        ):
            with self.subTest(output=output), self.assertRaises(ValueError):
                checker.parse_dependencies(output)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "No Mach-O"):
                checker.verify_prefix(directory)


if __name__ == "__main__":
    unittest.main()
