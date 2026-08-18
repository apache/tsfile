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
DEFAULT_ROOT="${SCRIPT_DIR}/../../.."
TSFILE_ROOT="${DEFAULT_ROOT}"
EXPLICIT_ROOT="false"
DOCS_MAP="${SCRIPT_DIR}/../references/docs-map.yaml"

usage() {
    cat <<'EOF'
Resolve version metadata from an Apache TsFile source checkout when available.

Usage: resolve-version.sh [--root <checkout>]

Output is line-oriented key=value data for Maven/Java, C++, Python, and Git.
If no checkout is discovered, version_source and version fields are reported as
unavailable and the script exits successfully. An invalid explicit --root
remains an error.
EOF
}

fail() {
    echo "resolve-version.sh: $*" >&2
    exit 1
}

extract_offline_reference_metadata() {
    offline_reference_scope="unavailable"
    offline_reference_last_verified="unavailable"

    [[ -f "${DOCS_MAP}" ]] || return 0

    parsed_last_verified="$(
        awk '$1 == "last_verified:" { gsub(/\"/, "", $2); print $2; exit }' \
            "${DOCS_MAP}"
    )"
    [[ -z "${parsed_last_verified}" ]] || \
        offline_reference_last_verified="${parsed_last_verified}"

    parsed_scope="$(
        awk '
            $1 == "-" && $2 == "id:" && $3 == "user-guide-v2-zh" {
                selected = 1
                next
            }
            selected && $1 == "version_scope:" { print $2; exit }
            selected && $1 == "-" && $2 == "id:" { exit }
        ' "${DOCS_MAP}"
    )"
    [[ -z "${parsed_scope}" ]] || offline_reference_scope="${parsed_scope}"
}

print_result() {
    printf 'version_source=%s\n' "${version_source}"
    printf 'tsfile_root=%s\n' "${tsfile_root}"
    printf 'maven_project_version=%s\n' "${project_version}"
    printf 'java_artifact_version=%s\n' "${project_version}"
    printf 'java_release=%s\n' "${java_release}"
    printf 'cpp_sdk_version=%s\n' "${cpp_version}"
    printf 'python_package_version=%s\n' "${python_version}"
    printf 'git_commit=%s\n' "${git_commit}"
    printf 'git_ref=%s\n' "${git_ref}"
    printf 'git_dirty=%s\n' "${git_dirty}"
    printf 'offline_reference_scope=%s\n' "${offline_reference_scope}"
    printf 'offline_reference_last_verified=%s\n' \
        "${offline_reference_last_verified}"
}

print_unresolved_result() {
    version_source="unavailable"
    tsfile_root="unavailable"
    project_version="unavailable"
    java_release="unavailable"
    cpp_version="unavailable"
    python_version="unavailable"
    git_commit="unavailable"
    git_ref="unavailable"
    git_dirty="unavailable"
    print_result
}

is_tsfile_checkout() {
    local root="$1"
    [[ -d "${root}" && -f "${root}/pom.xml" ]] || return 1
    grep -q '<artifactId>tsfile-parent</artifactId>' "${root}/pom.xml"
}

extract_offline_reference_metadata

while (( $# > 0 )); do
    case "$1" in
        --root)
            (( $# >= 2 )) || fail "--root requires a path"
            TSFILE_ROOT="$2"
            EXPLICIT_ROOT="true"
            shift 2
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

if ! is_tsfile_checkout "${TSFILE_ROOT}"; then
    if [[ "${EXPLICIT_ROOT}" == "true" ]]; then
        [[ -d "${TSFILE_ROOT}" ]] || fail "checkout does not exist: ${TSFILE_ROOT}"
        [[ -f "${TSFILE_ROOT}/pom.xml" ]] || \
            fail "root pom.xml not found: ${TSFILE_ROOT}/pom.xml"
        fail "not an Apache TsFile source checkout: ${TSFILE_ROOT}"
    fi
    print_unresolved_result
    exit 0
fi

TSFILE_ROOT="$(cd "${TSFILE_ROOT}" && pwd)"
POM="${TSFILE_ROOT}/pom.xml"

extract_project_version() {
    awk '
        /<\/parent>/ { after_parent = 1; next }
        after_parent && /<version>/ {
            line = $0
            sub(/^.*<version>/, "", line)
            sub(/<\/version>.*$/, "", line)
            print line
            exit
        }
    ' "$1"
}

extract_xml_property() {
    local property="$1"
    awk -v property="${property}" '
        index($0, "<" property ">") {
            line = $0
            sub("^.*<" property ">", "", line)
            sub("</" property ">.*$", "", line)
            print line
            exit
        }
    ' "${POM}"
}

resolve_maven_property() {
    local value="$1" property resolved
    if [[ "${value}" =~ ^\$\{([A-Za-z0-9._-]+)\}$ ]]; then
        property="${BASH_REMATCH[1]}"
        resolved="$(extract_xml_property "${property}")"
        [[ -n "${resolved}" ]] || fail "unresolved Maven version property: ${value}"
        value="${resolved}"
    fi
    printf '%s' "${value}"
}

project_version="$(extract_project_version "${POM}")"
[[ -n "${project_version}" ]] || fail "project version not found in ${POM}"
project_version="$(resolve_maven_property "${project_version}")"

java_release="$(extract_xml_property maven.compiler.release)"
[[ -n "${java_release}" ]] || java_release="unavailable"

cpp_version="unavailable"
if [[ -f "${TSFILE_ROOT}/cpp/CMakeLists.txt" ]]; then
    parsed_cpp_version="$(
        sed -n 's/^[[:space:]]*set(TsFile_CPP_VERSION[[:space:]]*"\{0,1\}\([^"[:space:])]*\)"\{0,1\}[[:space:]]*).*/\1/p' \
            "${TSFILE_ROOT}/cpp/CMakeLists.txt" | sed -n '1p'
    )"
    [[ -z "${parsed_cpp_version}" ]] || cpp_version="${parsed_cpp_version}"
fi

python_version="unavailable"
if [[ -f "${TSFILE_ROOT}/python/pyproject.toml" ]]; then
    parsed_python_version="$(
        sed -n 's/^[[:space:]]*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' \
            "${TSFILE_ROOT}/python/pyproject.toml" | sed -n '1p'
    )"
    [[ -z "${parsed_python_version}" ]] || python_version="${parsed_python_version}"
fi

git_commit="unavailable"
git_ref="unavailable"
git_dirty="unavailable"
if command -v git >/dev/null 2>&1 && git -C "${TSFILE_ROOT}" rev-parse --git-dir >/dev/null 2>&1; then
    git_commit="$(git -C "${TSFILE_ROOT}" rev-parse HEAD)"
    git_ref="$(git -C "${TSFILE_ROOT}" describe --tags --exact-match 2>/dev/null || true)"
    if [[ -z "${git_ref}" ]]; then
        git_ref="$(git -C "${TSFILE_ROOT}" symbolic-ref --short HEAD 2>/dev/null || true)"
    fi
    [[ -n "${git_ref}" ]] || git_ref="detached"
    if [[ -n "$(git -C "${TSFILE_ROOT}" status --porcelain 2>/dev/null)" ]]; then
        git_dirty="true"
    else
        git_dirty="false"
    fi
fi

version_source="checkout_metadata"
tsfile_root="${TSFILE_ROOT}"
print_result
