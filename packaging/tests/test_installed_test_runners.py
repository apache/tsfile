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
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


def load_runner(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class InstalledTestRunnersTest(unittest.TestCase):
    def test_selects_downloaded_wheel_for_exact_version_and_abi(self):
        runner = load_runner("python_installed_tests")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            selected = root / "tsfile-2.5.0-cp312-cp312-manylinux_2_28_x86_64.whl"
            selected.touch()
            (root / "tsfile-2.3.2.dev0-cp312-cp312-manylinux_2_28_x86_64.whl").touch()
            (root / "tsfile-2.5.0-cp311-cp311-manylinux_2_28_x86_64.whl").touch()
            self.assertEqual(
                runner.select_wheel(root, "2.5.0", "cp312"), selected.resolve()
            )
            with self.assertRaisesRegex(ValueError, "Expected one"):
                runner.select_wheel(root, "2.5.0", "cp314")

    def test_rejects_ambiguous_downloaded_wheels(self):
        runner = load_runner("python_installed_tests")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for platform in ("macosx_12_0_arm64", "macosx_12_0_x86_64"):
                (root / f"tsfile-2.5.0-cp312-cp312-{platform}.whl").touch()
            with self.assertRaisesRegex(ValueError, "Expected one"):
                runner.select_wheel(root, "2.5.0", "cp312")

    def test_cpp_config_is_required_under_the_installed_prefix(self):
        runner = load_runner("cpp_installed_tests")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "Expected one installed"):
                runner.package_config(root)
            config = root / "lib/x86_64-linux-gnu/cmake/TsFile/TsFileConfig.cmake"
            config.parent.mkdir(parents=True)
            config.touch()
            self.assertEqual(runner.package_config(root), config.parent)

    def test_go_pkgconfig_supports_multiarch_and_lib64_installations(self):
        runner = load_runner("go_installed_tests")
        for libdir in ("lib", "lib64", "lib/x86_64-linux-gnu"):
            with self.subTest(
                libdir=libdir
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                metadata = root / libdir / "pkgconfig/libtsfile.pc"
                metadata.parent.mkdir(parents=True)
                metadata.touch()
                self.assertEqual(runner.pkgconfig_file(root), metadata)

    def test_windows_go_import_library_uses_only_c_exports(self):
        runner = load_runner("go_installed_tests")
        output = """File: tsfile.dll
Export {
  Ordinal: 1
  Name: tsfile_open
}
Export {
  Ordinal: 2
  Name: ?query@TsFileReader@storage@@QEAAXXZ
}
Export {
  Name: free_tsfile
}
"""
        self.assertEqual(
            runner.exported_c_symbols(output), ["free_tsfile", "tsfile_open"]
        )
        with self.assertRaisesRegex(ValueError, "No C exports"):
            runner.exported_c_symbols("invalid PE input")

    def test_race_runs_on_supported_targets_only(self):
        runner = load_runner("go_installed_tests")
        for goos in ("linux", "darwin", "windows"):
            self.assertTrue(runner.race_supported(goos, "amd64"))
        self.assertTrue(runner.race_supported("linux", "arm64"))
        self.assertTrue(runner.race_supported("darwin", "arm64"))
        self.assertFalse(runner.race_supported("windows", "arm64"))


if __name__ == "__main__":
    unittest.main()
