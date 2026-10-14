<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# TsFile C++ Packages

The C++ installation contract is shared by all package formats. It installs
the library, the current compatibility header closure, `TsFileConfig.cmake`,
pkg-config metadata, the CLI, and Apache license files below one prefix.

The header closure mirrors the include relationships used by the current C++
implementation. It is intentionally broader than the long-term stable public
API; a separate API cleanup will narrow it in a future version.

## Manual artifact workflow

`Build native package artifacts` (`.github/workflows/native-packages.yml`) is
the authoritative native packaging workflow. In the Apache TsFile repository's GitHub Actions tab,
select this workflow, choose **Run workflow**, select the branch containing the
commit to build, and dispatch it manually. It runs only on `workflow_dispatch`,
with read-only repository permissions; pushes and pull requests do not trigger
native packaging.

A successful run builds the C++ core through Maven, stages one SDK per
platform and compatibility baseline, and then produces Ubuntu amd64 and arm64 DEBs, AlmaLinux
x86_64 and aarch64 RPMs, ARM64 and Intel Homebrew development bottles with a
merged Formula, Windows x86_64 and ARM64 SDK/CLI ZIPs, and Python wheels for Linux and macOS
on both architectures plus Windows x86_64. Every wheel platform covers CPython
3.9 through 3.14. The Go consumer and Python extensions reuse staged SDKs; C++
is built once per compatibility baseline. The job combines these into
`tsfile-native-packages-<archive-version>` with `manifest.json` and
`SHA256SUMS`. Download this final artifact from the workflow
run; both intermediate and final artifacts are retained for 14 days.

Publishing is a separate, manual step. This workflow only builds, tests, and
uploads GitHub Actions artifacts. It has no publishing credentials and does
not upload to JFrog, sign packages, update `latest`, create tags or releases, or
promote a build to an official release.

For a source version of `2.5.0`, the workflow generates release-format
metadata: SDK archives and Python wheels use `2.5.0`, DEBs use `2.5.0-1`,
and RPMs use `2.5.0-1.el9`. The bundle records the exact source repository
and commit in `manifest.json`. A source version ending in `.dev` retains
the date, workflow run, attempt, and commit suffixes for development builds.
Final bundle assembly rejects wheels whose version differs from the release
version.

## Portable archive

Build a relocatable SDK and portable archive through the Maven entry point:

```bash
./mvnw -Pwith-cpp package \
  -Dbuild.test=OFF \
  -Dtsfile.dependency.source=AUTO \
  -Denable.cpack=ON
cmake --install cpp/target/build --prefix cpp/build/sdk-root
cpack --config cpp/target/build/CPackConfig.cmake -G TGZ -B cpp/build/packages
```

The staged `cpp/build/sdk-root` contains a standard prefix layout and can be
unpacked at `/usr/local`, a user directory, or a relocated application prefix.

## Native Linux packages

DEB and RPM packages must be built in the target distribution environment so
CPack can run the native dependency scanner (`dpkg-shlibdeps` or
`rpmbuild`). Build the same Maven-driven tree once, then select the native
generator in the same job:

```bash
./mvnw -Pwith-cpp package \
  -Dbuild.test=OFF \
  -Dtsfile.dependency.source=AUTO \
  -Denable.cpack=ON
cpack -G DEB --config cpp/target/build/CPackConfig.cmake -B cpp/build/packages
# or: cpack -G RPM --config cpp/target/build/CPackConfig.cmake -B cpp/build/packages
```

`SYSTEM` can be used instead of `AUTO` when the build image provides every
compatible dependency, including ANTLR4 >=4.9.3 and <4.13.0. `AUTO` is the
reproducible release default and uses the verified source fallback for
unavailable or incompatible distro versions.

The manual `Build native package artifacts` workflow is the authoritative
native package build. Its Linux jobs create `tsfile`, `tsfile-dev`, and
`tsfile-tools` DEBs on native Ubuntu 22.04 amd64 and arm64 runners, and
`tsfile`, `tsfile-devel`, and `tsfile-tools` RPMs on native AlmaLinux 9
x86_64 and aarch64 runners. Fresh Ubuntu 22.04, Ubuntu 24.04, and AlmaLinux 9
containers install the matching packages and verify both `tsfile-cli` and an
external CMake consumer. The workflow uploads intermediate artifacts for 14
days and does not publish packages.

## Test an installed C/C++ SDK

Configure `cpp/test` directly to compile existing C/C++ API regression tests
against an installed package or extracted SDK. This standalone entry point uses
`find_package(TsFile CONFIG REQUIRED)` and links `TsFile::tsfile`; it does not
configure or compile the TsFile library or CLI. Install a CMake-configured
GoogleTest package for the same architecture and compatible compiler runtime.

```bash
cmake -S cpp/test -B installed-sdk-tests \
  -DCMAKE_PREFIX_PATH=/absolute/path/to/sdk
cmake --build installed-sdk-tests --config Release --parallel
ctest --test-dir installed-sdk-tests -C Release --output-on-failure
```

For a system-installed DEB or RPM development package, omit
`CMAKE_PREFIX_PATH`. If GoogleTest is installed under a separate prefix, pass
its config directory with `-DGTest_DIR=/absolute/path/to/lib/cmake/GTest`.
Only the contents of `cpp/test` are needed on the test machine; neither
`cpp/src` nor `cpp/tools` is required. Run CTest sequentially because some
reused tests write fixed filenames in their working directory.

The standalone suite reuses 15 existing test files covering C bindings,
table/tree reads and writes, row queries, Arrow batches, properties, recovery,
and parallel writes. Existing disabled tests remain disabled. The complete
source-built suite continues to run through `-Pwith-cpp verify`; internal
allocator/fault-injection tests, codec implementation tests and CLI object
unit tests remain there. Every native installation job now runs this suite
through `packaging/tests/cpp_installed_tests.py`, in addition to the small
consumer check. The runner builds a SHA-256-verified GoogleTest 1.12.1 dependency
and links the test executable to the installed `TsFile::tsfile` target. Windows
tests and GoogleTest use the same static MSVC runtime as the packaged DLL.

## CLI installation acceptance

`packaging/tests/cli_package_smoke.py` executes an installed CLI against the
four resources in the current source checkout's `python/tests/resources`:

| Resource | Model | Expected rows |
| --- | --- | ---: |
| `simple_tree.tsfile` | Tree | 105 |
| `simple_table_t1.tsfile` | Table | 60 |
| `simple_table_t2.tsfile` | Table | 40 |
| `table_with_time_column.tsfile` | Table with a named time column | 25 |

The workflow takes these files from the same checked-out commit used for the
build and distributes one `native-cli-test-data` artifact. The script verifies each file's SHA-256
before using it. It checks successful exit status, schema/types, metadata,
row/null counts, selected data sums and BOOLEAN values, head/offset/limit,
exported CSV versus `cat`, and sketch availability. Expected data facts come
from `python/tests/test_load_tsfile_from_iotdb.py`.

```bash
python3 packaging/tests/cli_package_smoke.py \
  --cli /absolute/path/to/installed/bin/tsfile-cli \
  --resources /absolute/path/to/source/python/tests/resources
```

Only Python's standard library is required. Every CLI invocation has a
30-second timeout; generated exports live in a temporary directory. The script
compares data semantics and requires no captured output baselines or CLI test
implementation objects.

Fresh Ubuntu 22.04/24.04 and AlmaLinux 9 containers install DEB/RPM packages
before running the checks. Separate Windows x86_64/ARM64 jobs unpack the built
ZIPs and run with the package's binary directory and Windows system paths.
Separate macOS Intel/ARM64 jobs merge the downloaded bottle metadata into a
trusted temporary tap, seed Homebrew's verified bottle cache, and install by
formula name. This supports Homebrew's package-path restrictions while keeping
SHA-256 verification and the `poured_from_bottle` receipt check. Acceptance jobs check out only
the acceptance scripts, existing C++ tests, and Go module. They compile the
C++ test executable and Go consumers against the installed package; they do not
compile TsFile or the CLI. Final assembly waits for all installation checks.

## macOS

Homebrew is the native macOS distribution path. The formula is
`packaging/homebrew/tsfile.rb`; it builds the same CMake install layout and
tests both a C++ consumer and `tsfile-cli --version` after installation.

```bash
brew install ./packaging/homebrew/tsfile.rb
```

The stable formula URL and checksum must be updated to the ASF source archive
when the first release containing this packaging work is published.

The manual native-package workflow also builds the self-hosted development
Formula `tsfile-dev`, using `packaging/homebrew/tsfile-dev.rb.in`. This is not a
Homebrew/core submission. Each build pins the full workflow commit from
the Apache TsFile source repository, hashes that source archive, and uses the generated immutable
Homebrew development version. The Formula retains the CMake install behavior
and tests the installed C++ consumer and CLI before bottling. `tsfile-dev` is
keg-only because the current SDK intentionally ships its dependency header
closure; linking that closure into Homebrew's shared prefix would collide with
headers owned by dependencies such as `simde`. After installation, invoke the
CLI as `$(brew --prefix tsfile-dev)/bin/tsfile-cli`, or add that Formula's
`bin` directory to `PATH`.

The ARM64 job runs on `macos-latest` and the Intel job on `macos-15-intel`.
Their `native-homebrew-macos-arm64` and `native-homebrew-macos-x86_64`
intermediate artifacts remain separate until merge. Each job also archives its
installed Formula prefix as a standalone SDK artifact,
`native-sdk-macos-arm64` or `native-sdk-macos-x86_64`, so macOS C++ consumers
do not have to depend on Homebrew. The SDK archive is checked with the CLI,
pkg-config, and an installed CMake consumer before upload. The merge job checks that
both source Formula files match, merges both platform JSON files with Homebrew,
and checks both generated tags and checksums in the resulting Formula.

The `native-homebrew` artifact contains `Formula/tsfile-dev.rb` and `bottles/`
with both bottle tarballs and both JSON metadata files. When downloaded into
`homebrew/`, this gives the final `homebrew/Formula/` and `homebrew/bottles/`
layout. The configured future bottle root is
`https://packages.apache.org/artifactory/tsfile/homebrew/dev/versions/<homebrew-version>/bottles`.
Homebrew generates local tarballs with a double dash before the version but
requests a single dash in HTTP URLs. After merge, the workflow renames each
tarball to the JSON `filename` (URL-decoded) and updates its `local_filename`
to match, preserving its checksum. The workflow only uploads GitHub Actions
artifacts for 14 days; it does not publish to that root or update `latest`.

## Windows

The manual workflow builds native x86_64 and ARM64 Release packages with
Visual Studio 2022 and bundled dependencies. Each job stages and tests the CLI
plus an external CMake SDK consumer before producing
`tsfile-<archive-version>-windows-x86_64.zip` or
`tsfile-<archive-version>-windows-arm64.zip`. Each archive is one combined ZIP
rather than one archive per CPack component, and contains the runtime,
development files, and tools under a relocatable prefix. The MSVC outputs are
installed as `bin/tsfile.dll`, `lib/tsfile.lib`, and `bin/tsfile-cli.exe`.

The portable ZIP targets Windows 10 / Windows Server 2022 or newer and uses
the static MSVC runtime (`/MT`) in Release. `TSFILE_MSVC_STATIC_RUNTIME=ON`
requires CMake 3.15+ and `TSFILE_DEPENDENCY_SOURCE=BUNDLED`: all codec archives,
TsFile, and the CLI are compiled with the same runtime selection. The workflow
checks the runtime property on every native target and inspects every shipped
EXE/DLL with `dumpbin`, rejecting dynamic Visual C++ runtime imports and missing
non-system DLLs in both the staged tree and the extracted ZIP. Running the CLI
does not require a separate Visual C++ Redistributable installation. Microsoft
redistributable DLLs are not copied from the runner or included in the ZIP;
runtime security updates require rebuilding the static-runtime package.

C++ SDK consumers should use an MSVC Release toolset compatible with the
compiler used to build that architecture and `/MT`
(`CMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded`, with CMake policy CMP0091 set to
NEW before `project()`). Keep allocation and release paired through the public
APIs; CRT-owned objects such as `FILE*` must not cross DLL boundaries. The
installed-consumer fixture demonstrates a public setting/getter round trip
that requires real symbols from `tsfile.dll`.

The workflow uploads the ZIPs as `native-windows-msvc-x86_64` and
`native-windows-msvc-arm64` for 14 days. Like the Linux jobs, it validates the
license files, CMake package config, staged executable, library, import library,
and public headers without publishing the artifacts. Windows ARM uses the
native ARM64 compiler and produces unsigned binary archives; no MSIX/AppX
signing step is part of this workflow.

## Static SDK and install regressions

`TSFILE_BUILD_SHARED=OFF` remains installable. Its CMake package rediscovers
system codec packages on the consuming machine and installs bundled codec
archives under `<libdir>/tsfile/<configuration>`. Link with `TsFile::tsfile`
to receive the complete static dependency list. Bundled archives and CMake
metadata move with the install prefix; system dependency development packages
must be available on the consuming machine. The shared package retains its
existing runtime/development/tools layout.

The pkg-config prefix is derived from the configured pkg-config installation
directory, including multiarch library directories. The optional Unix native
integration suite builds, installs, relocates, and links CMake and pkg-config
consumers. It requires CMake 3.19+ for the runtime-property test, pkg-config,
system LZ4, and a verified dependency cache containing ANTLR4, utf8cpp, zlib,
and LZOKAY archives:

```bash
TSFILE_RUN_INSTALL_TESTS=1 \
TSFILE_TEST_DEPENDENCY_CACHE=/path/to/dependency-cache \
python3 -m unittest discover -s packaging/tests -p 'test_*.py' -v
```

## Go SDK verification

`packaging/tests/go_installed_tests.py` runs the Go module's own `go vet`,
`go test`, and `go test -race` after native package installation on Linux,
macOS and Windows, on both architectures. The jobs use Go 1.26. Windows ARM64
runs `vet` and normal tests; Go does not support its race detector on that
platform, so the runner explicitly reports that one check as not applicable.

On Unix, the runner obtains both installed include roots, feature definitions,
and the library search path from the package's relocatable `libtsfile.pc`.
On Windows, a SHA-256-verified LLVM-MinGW 20261006 toolchain compiles cgo's C
bridge. The runner reads the installed MSVC DLL's C exports and generates a
MinGW import library, without rebuilding or replacing the DLL. This exercises
the same manually installed native package that Go users consume.

## Python wheels

The native workflow is the single packaging entry point. It replaces the former
`.github/workflows/wheels.yml` workflow; pushing an `rc/**` branch no longer
starts a separate wheel build. Dispatch the native workflow to build the full
package bundle, including all 30 Python wheels:

| Wheel platform | CPython | C++ SDK source |
| --- | --- | --- |
| manylinux_2_28 x86_64 | 3.9–3.14 | One bundled build inside the manylinux container |
| manylinux_2_28 aarch64 | 3.9–3.14 | One bundled build inside the native ARM container |
| macOS 12 x86_64 | 3.9–3.14 | One bundled build with deployment target 12.0 |
| macOS 12 arm64 | 3.9–3.14 | One bundled build with deployment target 12.0 |
| Windows x86_64 | 3.9–3.14 | The existing native MSVC SDK |

`cibuildwheel==3.4.1` builds the Python extensions and handles platform wheel
repair. Its `before-all` hook invokes `packaging/scripts/build_python_sdk.sh`
once per Unix compatibility baseline. Every Python ABI then links against that
SDK through `TSFILE_CPP_BUILD`. The manylinux hook exports the SDK archive
through cibuildwheel's `/host` mount so the subsequent upload runs on the host.
The Windows job downloads the existing MSVC SDK
and copies its DLLs from `bin/` into a Python build view under `lib/`; it does
not compile another C++ core. The native Windows SDK enables LZMA2, matching
the wheel compatibility SDKs.

The wheel SDKs remain distinct from the Ubuntu/AlmaLinux SDKs and Homebrew
SDKs: an Ubuntu SDK does not by itself establish manylinux_2_28 compatibility,
and a Homebrew SDK may require newer macOS or Homebrew libraries. The four
Unix wheel SDKs use bundled dependencies and are included in the final bundle
as `native-sdk-manylinux_2_28-{x86_64,aarch64}` and
`native-sdk-macos12-{x86_64,arm64}`. This also makes them available for direct
download and other consumers of the same compatibility baseline.

Wheel jobs build, repair and upload artifacts. Separate `test-python-unix`
and `test-python-windows` jobs download those artifacts on fresh runners, one
job for each of the 30 platform/CPython combinations. They check out only
`python/tests` and the installation test runner; neither the Python package
implementation nor C++ sources are present.

`packaging/tests/python_installed_tests.py` selects exactly one downloaded
wheel matching the expected package version and interpreter ABI, installs it
with binary-only dependencies, and runs the existing full `pytest` suite.
Imports use Python's isolated mode from a temporary working directory, and the
runner checks the installed module path and distribution version. Individual
tests have a 120-second timeout with stack traces; import and whole-suite
timeouts prevent native hangs from consuming an entire job. Each combination
uploads a JUnit result artifact. Existing test-suite platform skips remain
visible in the report.

Final assembly rejects missing Python ABIs, duplicate ABIs, incompatible
platform tags, and inconsistent wheel versions. Wheel manifest entries include
the Python ABI, wheel version, and the SDK platform used to build the extension.

## Installed package test matrix

| Installed package | Architectures | Separate installation tests |
| --- | --- | --- |
| DEB on Ubuntu 22.04 and 24.04 | x86_64, ARM64 | C++ API regressions, CLI resource checks, Go vet/test/race |
| RPM on AlmaLinux 9 | x86_64, ARM64 | C++ API regressions, CLI resource checks, Go vet/test/race |
| Homebrew bottle on macOS | x86_64, ARM64 | C++ API regressions, CLI resource checks, Go vet/test/race |
| Windows ZIP | x86_64, ARM64 | C++ API regressions, CLI resource checks, Go vet/test; race on x86_64 |
| Python wheel | Linux/macOS x86_64/ARM64, Windows x86_64 | Full installed-wheel pytest for every CPython 3.9–3.14 ABI |
| Standalone SDK archives | All 12 distribution and compatibility SDK variants | Download, extract to a new prefix, then C++ API regressions, CLI resource checks and Go tests |

All native tests consume downloaded, installed packages. CLI acceptance uses
the source checkout's Python resources and does not use CLI golden/E2E files.
The C++ regression suite compiles existing API tests through `find_package`;
internal tests requiring a special test build remain in source CI. The release
bundle is gated on every installation-test matrix completing successfully.

Each native installation job also downloads its separate SDK archive and runs
`packaging/tests/sdk_archive_tests.py` against an extracted prefix. Its C++ test
executable links that prefix's `TsFile::tsfile`; CLI and Go consume the same
extracted files. The existing GoogleTest dependency is reused, while the SDK's
tests are rebuilt and rerun. Separate `test-compatibility-sdk` jobs cover the
four bundled manylinux/macOS 12 SDK archives produced for wheel builds. These
jobs check out consumer tests only and also gate final assembly. SDK archive
test reports are retained alongside native-package test reports.

## Final native package bundle

Only after the Ubuntu amd64 and arm64 DEB installation tests, AlmaLinux x86_64
and aarch64 RPM installation tests, macOS and Windows C++/CLI/Go installation
tests, all standalone and compatibility SDK archive tests, all 30 installed-wheel
pytest jobs, macOS SDK and Homebrew bottle merge,
and Windows x86_64 and ARM64 SDK/CLI builds
all succeed, the workflow assembles
`tsfile-native-packages-<archive-version>`. The final
GitHub Actions artifact retains the platform SDKs, including both Linux ARM
archives, both macOS archives, the Windows ARM64 archive, the Python
wheels and their compatibility SDKs, the DEBs, RPMs, merged Formula and bottles,
and Windows ZIPs in their package-family layouts for 14 days. It also contains a
sorted `SHA256SUMS` and `manifest.json` with the source identity, generated
versions, byte sizes, SHA-256 values, and the JFrog repository, immutable target
path, and properties required for later manual publication.

The final job has no publishing credentials and does not upload to JFrog. A
maintainer can later use the manifest to upload DEBs to `tsfile-debian` with the
recorded Debian coordinates, RPMs to `tsfile-rpm/dev/el9/<architecture>`,
Homebrew formula/bottles to `tsfile/homebrew/dev/versions/<version>`, SDKs to
`tsfile/sdk/dev/versions/<version>`, Windows ZIPs to
`tsfile/windows/dev/versions/<version>`, and Python wheels to the
`tsfile-python` repository recorded in the manifest.
