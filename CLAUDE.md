<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# CLAUDE.md

This file provides repository-wide guidance for coding agents. `AGENTS.md` is a symbolic link to this file; edit this file to update both entry points.

## Project Overview

Apache TsFile is a columnar storage file format designed for time series data. The project provides implementations in four languages, each in its own top-level directory. The main branch is `develop`.

TsFile uses a hierarchical storage model: **Page → Chunk → ChunkGroup → File**. Data is organized by device, with each device's measurements stored as individual time series. Two write models exist: **aligned** (all measurements share timestamps) and **non-aligned** (independent timestamps per measurement).

## Repository Structure

| Directory | Language | Build System | Details |
|-----------|----------|--------------|---------|
| `java/`   | Java     | Maven        | [java/CLAUDE.md](java/CLAUDE.md) |
| `cpp/`    | C++      | CMake        | [cpp/CLAUDE.md](cpp/CLAUDE.md) |
| `python/` | Python   | setuptools + Cython | [python/CLAUDE.md](python/CLAUDE.md) |
| `go/`     | Go       | Go toolchain (cgo) | [go/README.md](go/README.md) |

The root `pom.xml` orchestrates Java, C++, and Python via Maven profiles:

```bash
./mvnw clean verify -P with-java       # Java only
./mvnw clean verify -P with-cpp        # C++ only
./mvnw clean verify -P with-python     # Python (requires C++ built first)
```

The Go module is not part of the Maven build. It is a cgo binding to the C++
library and is built and tested with the Go toolchain in `go/` (`make test`,
`make race`, `make vet`), against a C++ library already built under
`cpp/target/build`.

### Local Iteration vs. Pre-Push Verification

These two entry points serve different purposes and are not interchangeable.

**While iterating**, run the already-built test artifacts directly for fast feedback:

```bash
./cpp/build/Release/lib/TsFile_Test                      # C++ test binary
./cpp/build/Release/lib/TsFile_Test --gtest_filter=Foo.*  # narrow to relevant cases
cd python && pytest tests/                                # Python tests
cd go && make test                                        # Go tests (C++ already built)
```

**Before committing or pushing**, run the Maven entry point that CI uses, from the repository root:

```bash
./mvnw -P with-cpp verify      # C++ changes
./mvnw -P with-python verify   # Python changes (also builds C++)
./mvnw -P with-java verify     # Java changes
```

These are smoke checks for a branch in flight. The full pre-push requirements — including the `clean` full-suite build, the C++ ASan run, Python binding sanitizer coverage and `spotless:check` — are defined under [Required Checks Before Pushing](#required-checks-before-pushing) and are not replaced by the commands above.

CI compiles, links, and packages through Maven, so running the test binary alone does not reproduce that pipeline. For example, a `constexpr` constant without an out-of-class definition links fine on macOS with the directly built binary but fails under Linux/LTO in CI.

## Cross-Module Dependencies

- **Python depends on C++**: The Python module uses Cython to bind to the C++ shared library (`libtsfile`). C++ must be built before Python (`-P with-python` implies C++ build).
- **Go depends on C++**: The Go module uses cgo against the C++ headers and shared library under `cpp/target/build`. C++ must be built before the Go tests.
- **Java is independent**: The Java implementation shares no build-time dependency with C++ or Python.
- All implementations read/write the same TsFile binary format.

## Codec and File-Format Compatibility

- When adding or modifying encoders, decoders, or compression/decompression implementations, use the corresponding Java implementation in the current source version as the reference. Keep codec behavior and binary representation consistent with Java.
- The on-disk TsFile format must also remain consistent with Java for the same file-format version, including encoded/compressed payload structure, page/chunk headers, and metadata. Do not introduce language-specific file-format changes.
- For changes affecting codecs or the on-disk format, add or update regression tests covering codec behavior, boundary values, and block/page boundaries. Verify cross-language compatibility in both directions: Java must read files produced by the changed implementation, and the changed implementation must read Java-generated files.

## CLI and Skill Synchronization

- When SDK functionality is added or changed, review the corresponding CLI integration (`cpp/tools/` and `java/tools/`, as applicable) and synchronize commands, options, defaults, supported types/codecs, and errors in the same change. Reuse SDK capability information where available and add regression tests. If no CLI change is applicable, explain why in the change description.
- When a code change alters behavior, update the affected skill documentation in the same change. This includes API and CLI behavior, defaults, errors, build workflows, and encoding/compression or file-format behavior.
- Review `skills/tsfile/` for SDK and format changes, and `skills/tsfile-cli/` plus `cpp/tools/skills/tsfile-cli/` for CLI changes. Update the relevant `SKILL.md`, references, examples, and helper scripts as needed, keeping duplicated CLI documentation consistent.
- A behavior change is incomplete until the skill instructions and examples describe the resulting behavior accurately.

## Code Formatting

```bash
./mvnw spotless:apply    # Format Java, C++, and Python (Google Java Format, clang-format, Black)
./mvnw spotless:check    # Check formatting without modifying
cd go && make fmt        # Format Go sources with gofmt
```

## Required Checks Before Pushing

Before pushing code, executable scripts, or build/test configuration changes to a remote, run the full Maven test suite from the repository root and verify that all required checks pass for the changes being pushed.

Pure documentation-only changes are exempt from full Maven builds, tests, and sanitizer runs. For these changes, review the documentation for accuracy and consistency, verify referenced paths and commands, and run `git diff --check`. A change that also modifies code, executable scripts, or build/test configuration does not qualify for this exemption.

For changes requiring full verification, run:

```bash
# Full Java, C++, and Python build, unit tests, and integration tests.
# with-python includes C++; enable LZMA2 as in the C++ CI jobs (CMake 3.20+).
./mvnw clean verify -P with-java,with-python -Denable.lzma2=ON

# C++ AddressSanitizer run, in addition to the ordinary full test suite.
./mvnw clean verify -P with-cpp -Denable.asan=ON \
  -Dbuild.type=RelWithDebInfo -Denable.lzma2=ON

# Explicit formatting check across the Java, C++, and Python modules.
./mvnw spotless:check -P with-java,with-python

# Go module: build C++ first, then run vet, tests, and the race detector.
cd go && make vet && make test && make race
```

- Do not substitute focused tests, a build-only command, or an earlier revision's results for the full suite. Do not bypass tests, formatting, or other validation with skip flags in these verification runs.
- Inspect the applicable workflows in `.github/workflows/` and verify their additional checks, including Python binding ASan/LSan coverage as documented in `python/SANITIZER.md`, compatibility tests, and static analysis. A successful ordinary Maven run does not establish that sanitizer checks passed; the Python sanitizer build also requires its separate instrumentation check and ownership test run.
- Check the actual test reports and exit statuses for Maven, ASan, formatting, and other required checks. Record the commands and results before pushing. If a required check fails, is skipped, or cannot run in the current environment, report the blocker and do not push until it has passed on a supported environment.

## CI Path Filters

The GitHub Actions workflows under `.github/workflows/` use per-language `paths` whitelists so that documentation-only changes and unrelated language directories do not trigger builds. When adding a directory or shared build input that a workflow consumes, update that workflow's `paths` list in the same change. Markdown (`**.md`), `docs/**`, `skills/**`, `cpp/docs/**`, and other languages' source directories are intentionally excluded from the language test workflows.

## Internationalization

Java logs and exception messages use a runtime `ResourceBundle` approach so the same JAR can emit English or Simplified Chinese based on a JVM startup property. See `java/CLAUDE.md` for the convention. Switch language with `-Dtsfile.locale=zh`.

## License Header

Use the complete ASF source header in new TsFile-authored files that support comments.
Match the comment syntax to the file format. Use `<!-- -->` for Markdown, `/* */`
for Java and C++, `//` for Go, and `#` for Python. Preserve third-party license and copyright
notices. [ASF source header policy](https://www.apache.org/legal/src-headers.html)

Keep license headers intact during bulk replacements and refactors. When adding
or restoring a standard header, check the full text including the disclaimer.
For generated sources, update the template that emits the header. ATR checks the
header wording separately from Maven RAT. [ATR license checks](https://releases.apache.org/docs/license-checks#lightweight-license-check)

## Git Commit

- Do NOT add `Co-Authored-By` trailer to commit messages.
