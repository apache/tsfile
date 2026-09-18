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
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts/verify_windows_runtime.py"


class WindowsRuntimeTest(unittest.TestCase):
    def checker(self):
        self.assertTrue(
            MODULE_PATH.is_file(), "Windows artifacts need a runtime dependency check"
        )
        spec = importlib.util.spec_from_file_location(
            "verify_windows_runtime", MODULE_PATH
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_allows_only_inbox_or_colocated_package_dlls(self):
        module = self.checker()
        imports = module.parse_dependents(
            "File Type: EXECUTABLE IMAGE\n\n  Image has the following dependencies:\n\n"
            "    tsfile.dll\n    KERNEL32.dll\n    api-ms-win-core-synch-l1-2-0.dll\n\n"
            "  Summary\n        1000 .data\n"
        )
        module.validate_dependents("tsfile-cli.exe", imports, {"tsfile.dll"})
        self.assertEqual(len(imports), 3)

    def test_rejects_dynamic_crt_even_when_runner_or_package_provides_it(self):
        module = self.checker()
        for dll in (
            "VCRUNTIME140.dll",
            "MSVCP140.dll",
            "ucrtbase.dll",
            "api-ms-win-crt-runtime-l1-1-0.dll",
        ):
            with self.subTest(dll=dll), self.assertRaisesRegex(
                ValueError, "static MSVC runtime"
            ):
                module.validate_dependents("tsfile.dll", {dll.lower()}, {dll.lower()})

    def test_rejects_unshipped_codec_dll(self):
        module = self.checker()
        with self.assertRaisesRegex(ValueError, "snappy.dll"):
            module.validate_dependents("tsfile.dll", {"snappy.dll"}, {"tsfile.dll"})

    def test_rejects_unrecognized_dumpbin_output(self):
        with self.assertRaisesRegex(ValueError, "DLL imports"):
            self.checker().parse_dependents(
                "fatal error LNK1107: invalid or corrupt file"
            )


if __name__ == "__main__":
    unittest.main()
