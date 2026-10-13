<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# CLI end-to-end tests

This directory is a standalone test bundle. Copy it to a machine with Python
3.9+ and a built or installed `tsfile-cli`; no TsFile headers, C++ compiler,
Google Test, Python TsFile package, or source checkout are required to run it.
The CLI itself still needs its native runtime dependencies.

```sh
python3 run_cli_e2e.py --cli /path/to/tsfile-cli --output-dir /tmp/cli-results
```

On Windows, use `python` and the path to `tsfile-cli.exe`. The executable path
is resolved before entering the isolated case directories. Use
`--expected-version <version>` to check the product version, `--case <id>` to
select a case, and `--timeout <seconds>` to change the per-command timeout.

Each invocation creates a new `run-*` directory below `--output-dir` and prints
its path. It retains a `report.json` and one directory per case. The report
records SHA-256 identities for the executable, command suite, driver, and assets. Each executed
step writes `NNN.stdout`, `NNN.stderr`, and `NNN.exit_code`; a failing step also
writes `NNN.diff`. Generated files remain in the case's `work/` directory.
The runner exits 0 when the selected cases pass, 1 on test failure, and 2 on an
invalid invocation or suite. These runner statuses are separate from the CLI
exit codes asserted by each step.

## Cases and expected results

`cases.json` contains all command arguments and expected exit codes. The
`golden/` directory contains expected stdout, stderr, and exported text;
`fixtures/` contains fixed TsFiles and other test inputs. The `_license` field
is license metadata, and `baseline_commit` identifies the source revision
against which the migrated cases were validated.

A case is a sequence of commands sharing one initially empty workspace:

```json
{
  "id": "MissingInput",
  "steps": [
    {
      "args": ["ls", "missing.tsfile"],
      "exit_code": 2,
      "stdout": "golden/empty.txt",
      "stderr": "golden/missing_input.txt",
      "files": {}
    }
  ]
}
```

This is a schema example; add the named expected files when adding such a case.
Arguments are passed as an array, without a shell. Exit codes are compared
independently of output files: a matching output with the wrong exit code is a
failure. Timeouts, launch failures, and process crashes also fail the case.
A failing step stops dependent steps in that case, while other cases still run.

Optional step fields:

- `prepare`: ordered filesystem setup actions. Each action has a relative
  `path` and a `type`: `file` (copy bytes from `source` in this bundle),
  `directory`, `absent`, `symlink` (with a relative `target`), or `fifo`.
  Setup actions describe changes originally made by the C++ fixture helpers.
- `stdin`: a fixture file whose bytes are sent to the command's stdin. Stdin is
  otherwise empty.
- `files`: expected changes to the workspace after the command. Each entry
  specifies its `type`; use `absent` for a removed path. A regular file may
  specify `source` for an exact content comparison. Symlinks specify `target`.
  All paths not listed must remain unchanged, including their bytes. Missing
  and unexpected files fail the step, including partial files left behind by
  failed writes.
- `posix_only`: a reason to skip a step on Windows, used for FIFOs, symlinks,
  or arguments containing raw non-UTF-8 bytes. Skipped steps are recorded in
  the report.
- `stdout_format: "version"`: require a complete, consistent version line and
  compare its template with the golden. Product version, commit, and build
  timestamp vary by the executable being tested; `--expected-version` can pin
  the product version. `require_concrete_commit: true` preserves the existing
  concrete-build-metadata case, which rejects an `unknown` commit. Raw stdout is still retained in the results.

Output comparisons preserve bytes, whitespace, ordering, and CSV embedded
line endings. Only stderr's native Windows CRT newline translation is accounted
for. Generated TsFiles are checked for existence and read back with CLI commands
against text goldens; their serialized bytes are not a cross-version contract.
Untouched input fixtures and preexisting targets are compared byte for byte.

The original 141 golden files and fixed tree/table fixtures are preserved.
Additional fixtures and expected outputs migrate the executable-level cases
from `command_e2e_test.cc`, `model_format_e2e_test.cc`,
`utf8_collision_e2e_test.cc`, and the command cases formerly in
`cli_args_test.cc` and `cli_requirements_test.cc`. The original 170 tests were
run with their existing assertions before migration; parser and injected
metadata/stream failures remain C++ unit tests. Successful writes additionally
have a CLI read-back step, and stdin has a process-level round-trip case.
Content-addressed fixture names allow cases to share identical binary inputs.

Goldens are reviewed test inputs. The runner never updates them. When changing
an expected result, inspect the retained actual file and diff before editing
its golden. When adding cases, use the functional contract of the product
version being tested; a newer test revision may independently validate an older
release candidate without modifying that candidate's source or installed files.

## Build integration

When C++ tools and tests are enabled, CTest runs `CliE2E` against the built CLI
and `CliE2ERunnerTest` against small child processes that exercise the runner's
failure handling. Python 3.9+ is required for these tests.

```sh
ctest --test-dir <cpp-build>/test -R '^CliE2E' --output-on-failure
python3 -m unittest discover -s . -p test_runner.py -v
```
