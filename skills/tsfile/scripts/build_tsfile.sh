#!/usr/bin/env bash
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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TSFILE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
MVNW="${TSFILE_ROOT}/mvnw"

cd "${TSFILE_ROOT}"

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

check_java() {
    if ! command_exists java; then
        echo "Java not found; JDK 17 is required" >&2
        return 1
    fi

    local specification_version major
    specification_version="$({ java -XshowSettings:properties -version; } 2>&1 \
        | awk -F'= ' '/java.specification.version/ {print $2; exit}')"
    if [[ "${specification_version}" == 1.* ]]; then
        major="${specification_version#1.}"
    else
        major="${specification_version%%.*}"
    fi
    if [[ ! "${major}" =~ ^[0-9]+$ ]] || (( major < 17 )); then
        echo "JDK 17 or newer is required; found ${specification_version:-unknown}" >&2
        return 1
    fi
    echo "Java ${specification_version}"
}

check_maven() {
    if [[ ! -x "${MVNW}" ]]; then
        echo "Maven wrapper not found: ${MVNW}" >&2
        return 1
    fi
    "${MVNW}" --version | sed -n '1p'
}

check_cpp() {
    local missing=()
    local tool
    for tool in cmake make c++ clang-format; do
        if ! command_exists "${tool}"; then
            missing+=("${tool}")
        fi
    done
    if (( ${#missing[@]} > 0 )); then
        echo "Missing C++ build tools: ${missing[*]}" >&2
        return 1
    fi
    echo "C++ build tools found"
}

check_python() {
    if ! command_exists python3; then
        echo "Python 3.9 or newer is required" >&2
        return 1
    fi

    local version major minor
    version="$(python3 --version | awk '{print $2}')"
    IFS='.' read -r major minor _ <<<"${version}"
    if (( major < 3 || (major == 3 && minor < 9) )); then
        echo "Python 3.9 or newer is required; found ${version}" >&2
        return 1
    fi
    echo "Python ${version}"
}

build_tsfile() {
    case "${1}" in
        java)
            "${MVNW}" -P with-java clean package -DskipTests
            ;;
        cpp)
            check_cpp
            "${MVNW}" -P with-cpp clean package -DskipTests
            ;;
        python)
            check_cpp
            check_python
            "${MVNW}" -P with-python clean package -DskipTests
            ;;
        all)
            check_cpp
            check_python
            "${MVNW}" -P with-java,with-python clean package -DskipTests
            ;;
        *)
            echo "Unknown language: ${1}" >&2
            return 1
            ;;
    esac
}

install_tsfile() {
    case "${1}" in
        java)
            "${MVNW}" -P with-java clean install -DskipTests
            ;;
        all)
            check_cpp
            check_python
            "${MVNW}" -P with-java,with-python clean install -DskipTests
            ;;
        *)
            echo "Local install is supported for java or all" >&2
            return 1
            ;;
    esac
}

test_tsfile() {
    case "${1}" in
        java)
            "${MVNW}" -P with-java clean verify
            ;;
        cpp)
            check_cpp
            "${MVNW}" -P with-cpp clean verify
            ;;
        python)
            check_cpp
            check_python
            "${MVNW}" -P with-python clean verify
            ;;
        all)
            check_cpp
            check_python
            "${MVNW}" -P with-java,with-python clean verify
            ;;
        *)
            echo "Unknown test target: ${1}" >&2
            return 1
            ;;
    esac
}

show_usage() {
    cat <<'EOF'
TsFile multi-language build helper

Usage: build_tsfile.sh <command> [language]

Commands:
  check
  validate-assets
  build <java|cpp|python|all>
  install <java|all>
  test <java|cpp|python|all>
  clean

Baseline:
  Java: JDK 17
  Maven: repository ./mvnw (Maven 3.6+)
  C++: CMake 3.11+, C++11 compiler, make, clang-format, UUID headers where required
  Python: Python 3.9+, C++ prerequisites
EOF
}

case "${1:-}" in
    check)
        check_java
        check_maven
        check_cpp
        check_python
        ;;
    validate-assets)
        "${SCRIPT_DIR}/validate-assets.sh" --root "${TSFILE_ROOT}"
        ;;
    build)
        [[ -n "${2:-}" ]] || { show_usage; exit 1; }
        check_java
        check_maven
        build_tsfile "${2}"
        ;;
    install)
        [[ -n "${2:-}" ]] || { show_usage; exit 1; }
        check_java
        check_maven
        install_tsfile "${2}"
        ;;
    test)
        [[ -n "${2:-}" ]] || { show_usage; exit 1; }
        check_java
        check_maven
        test_tsfile "${2}"
        ;;
    clean)
        check_maven
        "${MVNW}" clean
        ;;
    *)
        show_usage
        exit 1
        ;;
esac
