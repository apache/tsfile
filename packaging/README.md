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
the authoritative native packaging workflow. In the fork's GitHub Actions tab,
select this workflow, choose **Run workflow**, select the branch containing the
commit to build, and dispatch it manually. It runs only on `workflow_dispatch`,
with read-only repository permissions; pushes and pull requests do not trigger
native packaging.

A successful run produces Ubuntu DEBs, AlmaLinux RPMs, an ARM64 and Intel
Homebrew development bottle with a merged Formula, and a Windows x86_64 SDK/CLI
ZIP. It combines these into `tsfile-native-packages-<archive-version>` with
`manifest.json` and `SHA256SUMS`. Download this final artifact from the workflow
run; both intermediate and final artifacts are retained for 14 days.

Publishing is a separate, manual step. This workflow only builds, tests, and
uploads GitHub Actions artifacts. It has no publishing credentials and does
not upload to JFrog, sign packages, update `latest`, create tags or releases, or
implement RC/final release behavior.

## Portable archive

Build a relocatable binary archive on any host with CMake and CPack:

```bash
cmake -S cpp -B cpp/build/package \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TEST=OFF \
  -DBUILD_TOOLS=ON \
  -DTSFILE_ENABLE_CPACK=ON \
  -DTSFILE_DEPENDENCY_SOURCE=AUTO
cmake --build cpp/build/package --parallel
cpack --config cpp/build/package/CPackConfig.cmake -G TGZ
```

The resulting `tsfile-<version>-<platform>.tar.gz` contains a standard
prefix layout and can be unpacked at `/usr/local`, a user directory, or a
relocated application prefix.

## Native Linux packages

DEB and RPM packages must be built in the target distribution environment so
CPack can run the native dependency scanner (`dpkg-shlibdeps` or
`rpmbuild`). On Debian/Ubuntu use `-G DEB`; on Fedora/RHEL use `-G RPM`:

```bash
cmake -S cpp -B cpp/build/package \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TEST=OFF \
  -DBUILD_TOOLS=ON \
  -DTSFILE_ENABLE_CPACK=ON \
  -DTSFILE_DEPENDENCY_SOURCE=AUTO
cmake --build cpp/build/package --parallel
cpack --config cpp/build/package/CPackConfig.cmake -G DEB
# or: cpack --config cpp/build/package/CPackConfig.cmake -G RPM
```

`SYSTEM` can be used instead of `AUTO` when the build image provides every
compatible dependency, including ANTLR4 >=4.9.3 and <4.13.0. `AUTO` is the
reproducible release default and uses the verified source fallback for
unavailable or incompatible distro versions.

The manual `Build native package artifacts` workflow is the authoritative
native package build. Its Linux jobs create `tsfile`, `tsfile-dev`, and
`tsfile-tools` DEBs on Ubuntu 22.04 and `tsfile`, `tsfile-devel`, and
`tsfile-tools` RPMs on AlmaLinux 9. Fresh Ubuntu 22.04, Ubuntu 24.04, and
AlmaLinux 9 containers install the packages and verify both `tsfile-cli` and an
external CMake consumer. The workflow uploads intermediate artifacts for 14
days and does not publish packages.

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
`ColinLeeo/tsfile`, hashes that source archive, and uses the generated immutable
Homebrew development version. The Formula retains the CMake install behavior
and tests the installed C++ consumer and CLI before bottling.

The ARM64 job runs on `macos-latest` and the Intel job on `macos-15-intel`.
Their `native-homebrew-macos-arm64` and `native-homebrew-macos-x86_64`
intermediate artifacts remain separate until merge. The merge job checks that
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

The manual workflow builds a 64-bit Release package with Visual Studio 2022 and
bundled dependencies. It stages and tests the CLI plus an external CMake SDK
consumer before producing
`tsfile-<archive-version>-windows-x86_64.zip`. The archive is deliberately one
combined ZIP rather than one archive per CPack component, and contains the
runtime, development files, and tools under a relocatable prefix. In
particular, the MSVC outputs are installed as `bin/tsfile.dll`,
`lib/tsfile.lib`, and `bin/tsfile-cli.exe`.

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

C++ SDK consumers should use the matching MSVC v143 Release toolset and `/MT`
(`CMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded`, with CMake policy CMP0091 set to
NEW before `project()`). Keep allocation and release paired through the public
APIs; CRT-owned objects such as `FILE*` must not cross DLL boundaries. The
installed-consumer fixture demonstrates a public setting/getter round trip
that requires real symbols from `tsfile.dll`.

The workflow uploads that ZIP as the intermediate artifact
`native-windows-msvc-x86_64` for 14 days. Like the Linux jobs, it validates the
license files, CMake package config, staged executable, library, import library,
and public headers without publishing the artifact.

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

## Final native package bundle

Only after the Ubuntu 22.04 and 24.04 DEB installation tests, AlmaLinux 9 RPM
installation test, Homebrew bottle merge, and Windows SDK/CLI build all succeed,
the workflow assembles `tsfile-native-packages-<archive-version>`. The final
GitHub Actions artifact retains the DEBs, RPMs, merged Formula and bottles, and
Windows ZIP in their package-family layouts for 14 days. It also contains a
sorted `SHA256SUMS` and `manifest.json` with the source identity, generated
versions, byte sizes, SHA-256 values, and the JFrog repository, immutable target
path, and properties required for later manual publication.

The final job has no publishing credentials and does not upload to JFrog. A
maintainer can later use the manifest to upload DEBs to `tsfile-debian` with the
recorded Debian coordinates, RPMs to `tsfile-rpm/dev/el9/x86_64`, and Homebrew
and Windows files to their immutable `tsfile/homebrew/dev/versions/<version>`
and `tsfile/windows/dev/versions/<version>` paths.
