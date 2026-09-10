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
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "packaging/scripts/render_homebrew_formula.py"
TEMPLATE_PATH = ROOT / "packaging/homebrew/tsfile-dev.rb.in"
VALUES = {
    "SOURCE_REPOSITORY": "ColinLeeo/tsfile",
    "GIT_SHA": "abcdef1234567890abcdef1234567890abcdef12",
    "HOMEBREW_VERSION": "2.5.0.dev0.20260910.123.1.gabcdef1",
    "SOURCE_SHA256": "0123456789abcdef" * 4,
}
TEMPLATE = """class TsfileDev < Formula
  url "https://github.com/@SOURCE_REPOSITORY@/archive/@GIT_SHA@.tar.gz"
  version "@HOMEBREW_VERSION@"
  sha256 "@SOURCE_SHA256@"
@BOTTLE_BLOCK@
end
"""
BOTTLE_BLOCK = """  bottle do
    root_url "https://packages.apache.org/artifactory/tsfile/homebrew/dev/versions/2.5.0.dev0.20260910.123.1.gabcdef1/bottles"
    sha256 cellar: :any, arm64_sequoia: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    sha256 cellar: :any, sequoia: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  end"""


class RenderHomebrewFormulaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = None
        if MODULE_PATH.is_file():
            spec = importlib.util.spec_from_file_location(
                "render_homebrew_formula", MODULE_PATH
            )
            cls.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cls.module)

    def renderer(self):
        self.assertIsNotNone(self.module, "Homebrew Formula renderer must exist")
        return self.module.render_formula

    def test_renders_exact_immutable_source_and_both_bottle_tags(self):
        rendered = self.renderer()(TEMPLATE, VALUES, BOTTLE_BLOCK)
        self.assertEqual(
            rendered,
            """class TsfileDev < Formula
  url "https://github.com/ColinLeeo/tsfile/archive/abcdef1234567890abcdef1234567890abcdef12.tar.gz"
  version "2.5.0.dev0.20260910.123.1.gabcdef1"
  sha256 "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"""
            + BOTTLE_BLOCK
            + "\nend\n",
        )

    def test_pre_bottle_formula_has_no_bottle_block(self):
        rendered = self.renderer()(TEMPLATE, VALUES)
        self.assertNotIn("bottle do", rendered)
        self.assertNotRegex(rendered, r"@[A-Z0-9_]+@")

    def test_rejects_missing_and_undeclared_placeholders(self):
        render = self.renderer()
        with self.assertRaises(ValueError):
            render(
                TEMPLATE,
                {key: value for key, value in VALUES.items() if key != "GIT_SHA"},
            )
        with self.assertRaises(ValueError):
            render(TEMPLATE + "@UNKNOWN@", {**VALUES, "UNKNOWN": "anything"})

    def test_rejects_template_tokens_in_bottle_block(self):
        with self.assertRaises(ValueError):
            self.renderer()(TEMPLATE, VALUES, BOTTLE_BLOCK.replace("sequoia", "@TAG@"))

    def test_rejects_duplicate_bottle_blocks(self):
        render = self.renderer()
        for template, block in (
            (TEMPLATE, BOTTLE_BLOCK + "\n" + BOTTLE_BLOCK),
            (TEMPLATE + "\nbottle do\nend\n", BOTTLE_BLOCK),
            (
                TEMPLATE.replace("@BOTTLE_BLOCK@", "@BOTTLE_BLOCK@\n@BOTTLE_BLOCK@"),
                BOTTLE_BLOCK,
            ),
        ):
            with self.subTest(template=template, block=block):
                with self.assertRaises(ValueError):
                    render(template, VALUES, block)

    def test_rejects_empty_or_non_immutable_source_identity(self):
        render = self.renderer()
        for key, value in (
            ("SOURCE_SHA256", ""),
            ("SOURCE_SHA256", "bad"),
            ("GIT_SHA", ""),
            ("GIT_SHA", "abcdef1"),
            ("GIT_SHA", "develop"),
            ("SOURCE_REPOSITORY", 'ColinLeeo/tsfile"'),
            ("HOMEBREW_VERSION", 'bad"\nversion "injected'),
        ):
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError):
                    render(TEMPLATE, {**VALUES, key: value})

    def test_preserves_ruby_interpolation_and_other_at_signs(self):
        extra = "\n# maintainer@example.org\n# #{rpath}\n"
        self.assertTrue(self.renderer()(TEMPLATE + extra, VALUES).endswith(extra))

    def test_cli_renders_real_template_with_optional_bottle_file(self):
        self.renderer()
        template = TEMPLATE_PATH.read_text(encoding="utf-8")
        self.assertIn(
            'keg_only "development snapshots install their dependency header closure"',
            template,
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            output = directory / "tsfile-dev.rb"
            block = directory / "bottle.rb"
            block.write_text(BOTTLE_BLOCK, encoding="utf-8")
            command = [
                sys.executable,
                str(MODULE_PATH),
                "--template",
                str(TEMPLATE_PATH),
                "--repository",
                VALUES["SOURCE_REPOSITORY"],
                "--git-sha",
                VALUES["GIT_SHA"],
                "--version",
                VALUES["HOMEBREW_VERSION"],
                "--source-sha256",
                VALUES["SOURCE_SHA256"],
                "--output",
                str(output),
            ]
            for bottle_args, expected_block in (
                ([], ""),
                (["--bottle-block", str(block)], BOTTLE_BLOCK),
            ):
                subprocess.run(command + bottle_args, check=True)
                rendered = output.read_text(encoding="utf-8")
                self.assertIn("class TsfileDev < Formula", rendered)
                self.assertIn(
                    'url "https://github.com/ColinLeeo/tsfile/archive/abcdef1234567890abcdef1234567890abcdef12.tar.gz"',
                    rendered,
                )
                self.assertIn('version "2.5.0.dev0.20260910.123.1.gabcdef1"', rendered)
                self.assertIn(
                    'sha256 "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"',
                    rendered,
                )
                self.assertIn("#{rpath}", rendered)
                self.assertNotRegex(rendered, r"@[A-Z0-9_]+@")
                self.assertNotIn("\n\n\n", rendered)
                if expected_block:
                    self.assertIn(BOTTLE_BLOCK, rendered)
                else:
                    self.assertNotIn("bottle do", rendered)


if __name__ == "__main__":
    unittest.main()
