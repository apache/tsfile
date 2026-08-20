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

## Portable archive

Build a relocatable source archive on any host with CMake and CPack:

```bash
cmake -S cpp -B cpp/build/package \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TEST=OFF \
  -DBUILD_TOOLS=ON \
  -DTSFILE_ENABLE_CPACK=ON \
  -DTSFILE_DEPENDENCY_SOURCE=AUTO
cmake --build cpp/build/package --parallel
cmake --install cpp/build/package
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
cmake --install cpp/build/package
cpack --config cpp/build/package/CPackConfig.cmake -G DEB
# or: cpack --config cpp/build/package/CPackConfig.cmake -G RPM
```

`SYSTEM` can be used instead of `AUTO` when the build image provides every
compatible dependency, including ANTLR4 4.9.x. `AUTO` is the reproducible
release default and uses the verified source fallback for unavailable or
incompatible distro versions.

The repository workflow `Cpp-Packaging` builds and uploads native DEB and RPM
artifacts on packaging-related changes. The DEB job runs on Ubuntu and the RPM
job runs in Fedora, so each package is generated with its native dependency
metadata tool.

## macOS

Homebrew is the native macOS distribution path. The formula is
`packaging/homebrew/tsfile.rb`; it builds the same CMake install layout and
tests both a C++ consumer and `tsfile-cli --version` after installation.

```bash
brew install ./packaging/homebrew/tsfile.rb
```

The stable formula URL and checksum must be updated to the ASF source archive
when the first release containing this packaging work is published.
