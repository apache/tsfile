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

"""Validate real wheel ZIP layouts and ELF debug-section rejection."""

import importlib.util
from pathlib import Path
import struct
import tempfile
import unittest
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "wheel_contents", ROOT / "packaging/scripts/check_python_wheel.py"
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class WheelContentsTest(unittest.TestCase):
    def check(self, files):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "tsfile.whl"
            with ZipFile(path, "w") as wheel:
                for name, data in files.items():
                    wheel.writestr(name, data)
            checker.check_wheel(path)

    def test_runtime_only_wheel_is_accepted(self):
        self.check(
            {
                "tsfile/libtsfile.so.1": b"native",
                "tsfile/reader.so": b"binding",
                "tsfile/__init__.py": b"",
            }
        )

    def test_sdk_headers_and_import_libraries_are_rejected(self):
        for name in [
            "tsfile/include/tsfile/cwrapper/tsfile_cwrapper.h",
            "tsfile/tsfile.lib",
            "tsfile/tsfile_cpp.pxd",
        ]:
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "Build-only"
            ):
                self.check({"tsfile/tsfile.dll": b"native", name: b"build-input"})

    def test_library_alias_copies_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "one TsFile runtime library"):
            self.check(
                {"tsfile/libtsfile.so": b"native", "tsfile/libtsfile.so.1": b"native"}
            )

    def test_elf_debug_sections_are_rejected(self):
        # ELF64 fixture: null section, section-name string table, debug section.
        strings = b"\0.shstrtab\0.debug_info\0"
        data = bytearray(64 + 3 * 64 + len(strings) + 1)
        data[:6] = b"\x7fELF\x02\x01"
        struct.pack_into("<Q", data, 40, 64)
        struct.pack_into("<HHH", data, 58, 64, 3, 1)
        struct.pack_into(
            "<IIQQQQIIQQ", data, 128, 1, 3, 0, 0, 256, len(strings), 0, 0, 1, 0
        )
        struct.pack_into(
            "<IIQQQQIIQQ", data, 192, 11, 1, 0, 0, 256 + len(strings), 1, 0, 0, 1, 0
        )
        data[256 : 256 + len(strings)] = strings
        with self.assertRaisesRegex(ValueError, "ELF debug sections"):
            self.check({"tsfile/libtsfile.so.1": bytes(data)})
