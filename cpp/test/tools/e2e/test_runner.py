# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Exercise runner failures with real child processes, without a TsFile build."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import run_cli_e2e


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "empty.txt").write_bytes(b"")
        self.step = {
            "args": ["-c", "pass"],
            "exit_code": 0,
            "stdout": "empty.txt",
            "stderr": "empty.txt",
            "files": {},
        }

    def run_suite(self, steps=None, **options):
        suite = {
            "schema_version": 1,
            "cases": [{"id": "example", "steps": steps or [self.step]}],
        }
        (self.root / "cases.json").write_text(json.dumps(suite))
        command = [
            sys.executable,
            str(Path(__file__).with_name("run_cli_e2e.py")),
            "--cli",
            sys.executable,
            "--suite",
            str(self.root / "cases.json"),
            "--output-dir",
            str(self.root / "results"),
        ]
        for key, value in options.items():
            command.extend(["--" + key.replace("_", "-"), str(value)])
        result = subprocess.run(command, capture_output=True, timeout=15)
        reports = list((self.root / "results").glob("run-*/report.json"))
        report = json.loads(reports[-1].read_text()) if reports else None
        return result, report

    def test_wrong_exit_code_fails_even_with_identical_empty_files(self):
        self.step["args"] = ["-c", "raise SystemExit(7)"]
        result, report = self.run_suite()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(report["cases"][0]["steps"][0]["actual_exit_code"], 7)
        self.assertIn(
            "expected 0, actual 7",
            next((self.root / "results").rglob("*.diff")).read_text(),
        )

    def test_expected_nonzero_exit_code_passes(self):
        self.step.update(args=["-c", "raise SystemExit(2)"], exit_code=2)
        result, report = self.run_suite()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(report["failed"], 0)

    def test_stdout_and_stderr_are_checked_independently(self):
        for stream in ("stdout", "stderr"):
            with self.subTest(stream=stream):
                self.step["args"] = [
                    "-c",
                    f"import sys; sys.{stream}.write('unexpected')",
                ]
                result, report = self.run_suite()
                self.assertEqual(result.returncode, 1)
                self.assertEqual(report["failed"], 1)

    def test_export_content_is_checked_and_golden_is_unchanged(self):
        (self.root / "expected.txt").write_bytes(b"expected\n")
        self.step["args"] = [
            "-c",
            "from pathlib import Path; Path('export.csv').write_bytes(b'wrong\\n')",
        ]
        self.step["files"] = {"export.csv": {"type": "file", "source": "expected.txt"}}
        result, _ = self.run_suite()
        self.assertEqual(result.returncode, 1)
        self.assertEqual((self.root / "expected.txt").read_bytes(), b"expected\n")

    def test_missing_or_unexpected_files_fail(self):
        self.step["files"] = {"missing.csv": {"type": "file"}}
        result, _ = self.run_suite()
        self.assertEqual(result.returncode, 1)
        self.step["files"] = {}
        self.step["args"] = ["-c", "from pathlib import Path; Path('extra').touch()"]
        result, _ = self.run_suite()
        self.assertEqual(result.returncode, 1)

    def test_steps_share_generated_files_and_stdin_is_preserved(self):
        (self.root / "input.txt").write_bytes(b"a\r\nb\n")
        self.step.update(
            args=[
                "-c",
                "import sys; from pathlib import Path; Path('data').write_bytes(sys.stdin.buffer.read())",
            ],
            stdin="input.txt",
            files={"data": {"type": "file", "source": "input.txt"}},
        )
        second = dict(
            self.step,
            args=[
                "-c",
                "import sys; from pathlib import Path; sys.stdout.buffer.write(Path('data').read_bytes())",
            ],
            stdout="input.txt",
        )
        second.pop("stdin")
        result, _ = self.run_suite([self.step, second])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_timeout_fails_without_running_dependent_steps(self):
        self.step["args"] = ["-c", "import time; time.sleep(10)"]
        result, report = self.run_suite([self.step, dict(self.step)], timeout=0.2)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(report["cases"][0]["steps"]), 1)

    def test_unknown_case_is_an_error(self):
        result, report = self.run_suite(case="typo")
        self.assertEqual(result.returncode, 2)
        self.assertIsNone(report)

    def test_expected_paths_cannot_escape_the_suite(self):
        self.step["stdout"] = "../outside.txt"
        result, report = self.run_suite()
        self.assertEqual(result.returncode, 2)
        self.assertIsNone(report)

    def test_skipped_posix_step_preserves_ordinary_setup_for_later_steps(self):
        (self.root / "expected.txt").write_bytes(b"hello\n")
        first = dict(
            self.step,
            posix_only="raw byte arguments",
            prepare=[
                {"path": "input", "type": "file", "source": "expected.txt"},
                {"path": "pipe", "type": "fifo"},
            ],
        )
        second = dict(
            self.step,
            args=[
                "-c",
                "import sys; from pathlib import Path; sys.stdout.buffer.write(Path('input').read_bytes())",
            ],
            stdout="expected.txt",
        )
        destination = self.root / "direct-results"
        destination.mkdir()
        with mock.patch.object(run_cli_e2e, "HAS_POSIX", False):
            result = run_cli_e2e.run_case(
                {"id": "skip", "steps": [first, second]},
                self.root,
                destination,
                Path(sys.executable),
                5,
                None,
            )
        self.assertTrue(result["passed"], result)
        self.assertIn("skipped", result["steps"][0])
        self.assertNotIn("skipped", result["steps"][1])

    def test_unlisted_input_changes_fail(self):
        self.step.update(
            prepare=[{"path": "input", "type": "file", "source": "empty.txt"}],
            args=[
                "-c",
                "from pathlib import Path; Path('input').write_bytes(b'changed')",
            ],
        )
        result, _ = self.run_suite()
        self.assertEqual(result.returncode, 1)

    @unittest.skipIf(os.name == "nt", "POSIX signals")
    def test_signal_exit_fails_even_with_empty_outputs(self):
        self.step["args"] = [
            "-c",
            "import os, signal; os.kill(os.getpid(), signal.SIGTERM)",
        ]
        result, report = self.run_suite()
        self.assertEqual(result.returncode, 1)
        self.assertLess(report["cases"][0]["steps"][0]["actual_exit_code"], 0)

    def test_concrete_commit_requirement_is_preserved(self):
        data = (
            b"tsfile-cli 2.5.0 tsfile=2.5.0 commit=unknown built=2026-10-13T00:00:00Z\n"
        )
        with self.assertRaises(ValueError):
            run_cli_e2e.version_output(data, "2.5.0", require_concrete_commit=True)

    def test_runner_can_be_repeated_without_overwriting_previous_results(self):
        for _ in range(2):
            result, _ = self.run_suite()
            self.assertEqual(result.returncode, 0)
        self.assertEqual(
            len(list((self.root / "results").glob("run-*/report.json"))), 2
        )


if __name__ == "__main__":
    unittest.main()
