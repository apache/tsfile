#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# One build per compatibility baseline, reused by every Python ABI.
set -euo pipefail

platform="${1:?usage: build_python_sdk.sh <platform> <archive-version>}"
archive_version="${2:?usage: build_python_sdk.sh <platform> <archive-version>}"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository_root"

case "$platform" in
  manylinux_2_28-x86_64|manylinux_2_28-aarch64)
    test "$(uname -s)" = Linux
    test "$(uname -m)" = "${platform#manylinux_2_28-}"
    # This hook runs inside cibuildwheel's manylinux container.
    yum install -y java-17-openjdk-devel cmake pkgconfig libuuid-devel libblkid-devel
    export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
    export PATH="$JAVA_HOME/bin:$PATH"
    ;;
  macos12-x86_64|macos12-arm64)
    test "$(uname -s)" = Darwin
    test "$(uname -m)" = "${platform#macos12-}"
    export MACOSX_DEPLOYMENT_TARGET=12.0
    export CFLAGS="${CFLAGS:-} -mmacosx-version-min=12.0"
    export CXXFLAGS="${CXXFLAGS:-} -mmacosx-version-min=12.0"
    export LDFLAGS="${LDFLAGS:-} -mmacosx-version-min=12.0"
    ;;
  *)
    printf 'Unsupported Python SDK platform: %s\n' "$platform" >&2
    exit 1
    ;;
esac

python3 packaging/scripts/run_maven_with_retry.py -Pwith-cpp package \
  -Dbuild.type=Release \
  -Dbuild.test=OFF \
  -DskipTests \
  -Dspotless.skip=true \
  -Dtsfile.version.sync.skip=true \
  -Dtsfile.dependency.source=BUNDLED \
  -Denable.lzma2=ON \
  "-Dtsfile.project.include=$repository_root/packaging/cmake/PythonSDK.cmake"

# A fixed path lets every Python build consume this SDK without rebuilding C++.
sdk_root="$repository_root/python-sdk"
test ! -e "$sdk_root"
cmake --install cpp/target/build --prefix "$sdk_root"
test -f "$sdk_root/include/tsfile/cwrapper/tsfile_cwrapper.h"
test -d "$sdk_root/lib"
if [[ "$(uname -s)" == Darwin ]]; then
  python3 packaging/scripts/verify_macos_runtime.py "$sdk_root"
fi
"$sdk_root/bin/tsfile-cli" --version
sdk_output="${TSFILE_PYTHON_SDK_EXPORT:-$repository_root/python-sdk-output}"
mkdir -p "$sdk_output"
tar -czf "$sdk_output/tsfile-sdk-$platform-$archive_version.tar.gz" python-sdk
