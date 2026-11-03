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
"""Run native ownership scenarios under AddressSanitizer/LeakSanitizer.

Enabled only for a Linux sanitizer build (``TSFILE_SANITIZER`` is set). Every
scenario runs in a fresh interpreter that exits normally, which is required for
LeakSanitizer to report at exit. The pytest runner itself disables leak
checking to avoid interpreter noise; each scenario process forces it back on.
"""

import os
from pathlib import Path
import subprocess
import sys

import pytest

SANITIZER = os.environ.get("TSFILE_SANITIZER", "").strip()
PYTHON_DIR = Path(__file__).resolve().parents[1]
SUPPRESSIONS = Path(__file__).resolve().parent / "sanitizer" / "lsan_suppressions.txt"

pytestmark = pytest.mark.skipif(
    not SANITIZER or sys.platform != "linux",
    reason=(
        "native sanitizer scenarios require a Linux build with "
        "TSFILE_SANITIZER set (see python/SANITIZER.md)"
    ),
)

SCENARIOS = [
    "tree_full",
    "table_full",
    "metadata_reads",
    "file_object_read",
    "dataset_read",
    "arrow_full",
    "arrow_early_release",
    "early_termination",
    "exception_cleanup",
    "repeated_open_close",
    "lifetime_interactions",
    "import_only",
    "verify_runtime",
]


def _set_option(options, key, value):
    items = [item for item in options.split(":") if item]
    items = [item for item in items if not item.startswith(f"{key}=")]
    items.append(f"{key}={value}")
    return ":".join(items)


def _scenario_env():
    env = os.environ.copy()
    asan = env.get("ASAN_OPTIONS", "")
    asan = _set_option(asan, "detect_leaks", "1")
    asan = _set_option(asan, "exitcode", "23")
    asan = _set_option(asan, "abort_on_error", "1")
    asan = _set_option(asan, "halt_on_error", "1")
    env["ASAN_OPTIONS"] = asan
    lsan = env.get("LSAN_OPTIONS", "")
    lsan = _set_option(lsan, "suppressions", str(SUPPRESSIONS))
    lsan = _set_option(lsan, "print_suppressions", "1")
    env["LSAN_OPTIONS"] = lsan
    env.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
    return env


def _run_scenario(scenario, work_dir, timeout=300):
    return subprocess.run(
        [
            sys.executable,
            "-X",
            "faulthandler",
            "-m",
            "tests.sanitizer_scenarios",
            scenario,
            str(work_dir),
        ],
        cwd=PYTHON_DIR,
        env=_scenario_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _combined_output(completed):
    return completed.stdout + completed.stderr


# A leak is attributable to this project when the first meaningful frame of
# its allocation stack (after allocator/runtime frames) is the native library
# or one of the Cython extensions. Interpreter and third-party frames found
# first mean the allocation belongs to them, even if a TsFile frame appears
# much further down (for example, our Cython module importing pandas).
TSFILE_FRAME_TOKENS = (
    "libtsfile",
    "tsfile_py_cpp",
    "tsfile_reader",
    "tsfile_writer",
    "dataset/_merge",
    "dataset/_index",
)
INTERPRETER_FRAME_TOKENS = ("python3.", "libpython", "site-packages")


def _leak_blocks(output):
    blocks = []
    current = []
    for line in output.splitlines():
        starts = line.startswith("Direct leak of") or line.startswith(
            "Indirect leak of"
        )
        if starts:
            if current:
                blocks.append(current)
            current = [line]
        elif current:
            if line.strip():
                current.append(line)
            else:
                blocks.append(current)
                current = []
    if current:
        blocks.append(current)
    return blocks


def _is_tsfile_leak(block):
    for line in block:
        if not line.lstrip().startswith("#"):
            continue
        if any(token in line for token in TSFILE_FRAME_TOKENS):
            return True
        if any(token in line for token in INTERPRETER_FRAME_TOKENS):
            return False
    return False


def _tsfile_leak_blocks(output):
    return [block for block in _leak_blocks(output) if _is_tsfile_leak(block)]


def _assert_no_sanitizer_report(completed):
    output = _combined_output(completed)
    assert ": ok" in completed.stdout, output
    assert "ERROR: AddressSanitizer" not in output, output

    if "ERROR: LeakSanitizer" not in output:
        assert completed.returncode == 0, output
        return

    # CPython does not release every import-time object before the sanitizer's
    # exit check, and third-party wheels keep process-lifetime caches. Those
    # blocks carry no TsFile frame; a leak from libtsfile or an extension does.
    tsfile_blocks = _tsfile_leak_blocks(output)
    assert not tsfile_blocks, "TsFile-attributable leaks:\n" + "\n".join(
        "\n".join(block) for block in tsfile_blocks
    )
    ignored = len(_leak_blocks(output)) - len(tsfile_blocks)
    print(f"ignored {ignored} interpreter/third-party leak block(s)")


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory):
    work_dir = tmp_path_factory.mktemp("sanitizer_fixtures")
    completed = _run_scenario("build_fixtures", work_dir)
    _assert_no_sanitizer_report(completed)
    return work_dir


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_ownership_scenario_reports_no_defects(scenario, fixture_dir):
    _assert_no_sanitizer_report(_run_scenario(scenario, fixture_dir))


@pytest.mark.parametrize(
    "scenario, marker",
    [
        ("positive_control_leak", "LeakSanitizer"),
        ("positive_control_uaf", "AddressSanitizer"),
    ],
)
def test_positive_control_fails_the_run(scenario, marker, tmp_path):
    """The harness must fail when a real defect exists.

    These controls use ctypes allocations, not TsFile code, so they also prove
    that the suppression file does not silently hide leaks.
    """
    completed = _run_scenario(scenario, tmp_path)
    output = _combined_output(completed)
    assert completed.returncode != 0, output
    assert marker in output, output
