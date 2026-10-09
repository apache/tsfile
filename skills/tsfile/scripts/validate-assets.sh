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
SKILL_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ASSETS_DIR="${SKILL_ROOT}/assets"
DEFAULT_ROOT="${SCRIPT_DIR}/../../.."
TSFILE_ROOT="${DEFAULT_ROOT}"
EXPLICIT_ROOT="false"
CHECKOUT_ROOT=""
TSFILE_VERSION=""
JAVA_JAR=""
CPP_INCLUDE=""
PYTHON_RUNTIME="false"

usage() {
    cat <<'EOF'
Validate TsFile skill output templates.

Usage: validate-assets.sh [--root <checkout>]
                          [--tsfile-version <published-version>]
                          [--java-jar <tsfile-jar>]
                          [--cpp-include <include-directory>]
                          [--python-runtime]

The script always validates XML and Python syntax. It automatically uses a
co-located TsFile checkout when available. Explicit Java artifacts or versions,
C++ headers, and --python-runtime override the corresponding automatically
discovered dependency. A Java version may be resolved through Maven; the script
never guesses one.
EOF
}

fail() {
    echo "validate-assets.sh: $*" >&2
    exit 1
}

command_required() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

is_tsfile_checkout() {
    local root="$1"
    [[ -d "${root}" && -f "${root}/pom.xml" ]] || return 1
    grep -q '<artifactId>tsfile-parent</artifactId>' "${root}/pom.xml"
}

while (( $# > 0 )); do
    case "$1" in
        --root)
            (( $# >= 2 )) || fail "--root requires a path"
            TSFILE_ROOT="$2"
            EXPLICIT_ROOT="true"
            shift 2
            ;;
        --tsfile-version)
            (( $# >= 2 )) || fail "--tsfile-version requires a value"
            TSFILE_VERSION="$2"
            shift 2
            ;;
        --java-jar)
            (( $# >= 2 )) || fail "--java-jar requires a file"
            JAVA_JAR="$2"
            shift 2
            ;;
        --cpp-include)
            (( $# >= 2 )) || fail "--cpp-include requires a directory"
            CPP_INCLUDE="$2"
            shift 2
            ;;
        --python-runtime)
            PYTHON_RUNTIME="true"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument: $1"
            ;;
    esac
done

if is_tsfile_checkout "${TSFILE_ROOT}"; then
    CHECKOUT_ROOT="$(cd "${TSFILE_ROOT}" && pwd)"
elif [[ "${EXPLICIT_ROOT}" == "true" ]]; then
    [[ -d "${TSFILE_ROOT}" ]] || fail "checkout does not exist: ${TSFILE_ROOT}"
    fail "not an Apache TsFile source checkout: ${TSFILE_ROOT}"
fi

command_required python3

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/tsfile-assets.XXXXXX")"
cleanup() {
    case "${temp_dir}" in
        "${TMPDIR:-/tmp}"/tsfile-assets.*) rm -rf -- "${temp_dir}" ;;
        *) echo "validate-assets.sh: refusing to remove unexpected path: ${temp_dir}" >&2 ;;
    esac
}
trap cleanup EXIT

python3 -c 'import sys, xml.etree.ElementTree as ET; ET.parse(sys.argv[1])' \
    "${ASSETS_DIR}/pom.xml"
PYTHONPYCACHEPREFIX="${temp_dir}/pycache" \
    python3 -m py_compile "${ASSETS_DIR}/tsfile_example.py"

pom_template="valid"
python_template="syntax-valid"
java_template="skipped-no-dependency"
cpp_template="skipped-no-headers"

compile_java_with_jar() {
    local jar="$1"
    command_required javac
    mkdir -p "${temp_dir}/java-classes"
    javac -cp "${jar}" -d "${temp_dir}/java-classes" \
        "${ASSETS_DIR}/TsFileExample.java"
    java_template="compiled"
}

compile_cpp_with_include() {
    local include_dir="$1"
    [[ -d "${include_dir}" ]] || fail "C++ include directory not found: ${include_dir}"
    command_required c++
    c++ -std=c++11 -I "${include_dir}" -fsyntax-only \
        "${ASSETS_DIR}/tsfile_example.cpp"
    cpp_template="compiled"
}

run_python_template() {
    local python_path="${1:-}"
    cp "${ASSETS_DIR}/tsfile_example.py" "${temp_dir}/tsfile_example.py"
    if [[ -n "${python_path}" ]]; then
        (
            cd "${temp_dir}"
            PYTHONPATH="${python_path}" python3 tsfile_example.py >/dev/null
        )
    else
        (
            cd "${temp_dir}"
            python3 tsfile_example.py >/dev/null
        )
    fi
    python_template="compiled-and-ran"
}

if [[ -n "${TSFILE_VERSION}" && -n "${JAVA_JAR}" ]]; then
    fail "use only one of --tsfile-version and --java-jar"
elif [[ -n "${JAVA_JAR}" ]]; then
    [[ -f "${JAVA_JAR}" ]] || fail "Java artifact not found: ${JAVA_JAR}"
    compile_java_with_jar "${JAVA_JAR}"
elif [[ -n "${TSFILE_VERSION}" ]]; then
    command_required mvn
    mkdir -p "${temp_dir}/java-template"
    cp "${ASSETS_DIR}/pom.xml" "${ASSETS_DIR}/TsFileExample.java" \
        "${temp_dir}/java-template/"
    mvn -q -f "${temp_dir}/java-template/pom.xml" \
        -Dtsfile.version="${TSFILE_VERSION}" compile
    java_template="compiled"
elif [[ -n "${CHECKOUT_ROOT}" ]]; then
    version_output="$("${SCRIPT_DIR}/resolve-version.sh" --root "${CHECKOUT_ROOT}")"
    java_version="$(
        printf '%s\n' "${version_output}" | \
            awk -F= '$1 == "java_artifact_version" { print $2; exit }'
    )"
    java_jar="${CHECKOUT_ROOT}/java/tsfile/target/tsfile-${java_version}.jar"
    if [[ ! -f "${java_jar}" ]]; then
        [[ -x "${CHECKOUT_ROOT}/mvnw" ]] || fail "Maven wrapper not found"
        "${CHECKOUT_ROOT}/mvnw" -pl java/tsfile -am package -DskipTests
    fi
    [[ -f "${java_jar}" ]] || fail "Java artifact not found after build: ${java_jar}"
    compile_java_with_jar "${java_jar}"
fi

if [[ -n "${CPP_INCLUDE}" ]]; then
    compile_cpp_with_include "${CPP_INCLUDE}"
elif [[ -n "${CHECKOUT_ROOT}" ]]; then
    compile_cpp_with_include "${CHECKOUT_ROOT}/cpp/src"
fi

if [[ "${PYTHON_RUNTIME}" == "true" ]]; then
    run_python_template
elif [[ -n "${CHECKOUT_ROOT}" ]]; then
    if PYTHONPATH="${CHECKOUT_ROOT}/python" \
        python3 -c 'import pandas, tsfile' >/dev/null 2>&1; then
        run_python_template "${CHECKOUT_ROOT}/python"
    else
        python_template="syntax-valid-local-binding-not-built"
    fi
fi

printf 'pom_template=%s\n' "${pom_template}"
printf 'java_template=%s\n' "${java_template}"
printf 'cpp_template=%s\n' "${cpp_template}"
printf 'python_template=%s\n' "${python_template}"
