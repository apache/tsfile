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

"""Read native SDK build metadata and prepare runtime-only wheel files."""

import importlib.machinery
import json
from pathlib import Path
import shutil
import subprocess
import sys

MSVC_RUNTIME_FLAGS = {
    "MultiThreaded": "/MT",
    "MultiThreadedDLL": "/MD",
    "MultiThreadedDebug": "/MTd",
    "MultiThreadedDebugDLL": "/MDd",
}


def read_sdk_config(sdk, library):
    candidates = [
        Path(sdk) / "share/tsfile/tsfile-build-config.json",
        Path(library).parent / "tsfile-build-config.json",
    ]
    for path in candidates:
        if path.is_file():
            config = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(config, dict) or config.get("schema_version") != 1:
                raise ValueError(f"Unsupported TsFile SDK build metadata: {path}")
            name = config.get("runtime_library")
            if (
                not isinstance(name, str)
                or not name
                or name in {".", ".."}
                or "/" in name
                or "\\" in name
                or ":" in name
                or "\0" in name
            ):
                raise ValueError(
                    f"SDK does not describe a shared runtime library: {path}"
                )
            return config
    raise FileNotFoundError(
        "Missing tsfile-build-config.json in the native SDK. "
        "Rebuild/install C++ from this checkout before building Python. "
        f"Checked: {candidates}"
    )


def msvc_runtime_flag(config):
    if config.get("compiler") != "MSVC":
        raise ValueError("An MSVC Python extension requires an MSVC-built SDK")
    runtime = config.get("msvc_runtime")
    if runtime not in MSVC_RUNTIME_FLAGS:
        raise ValueError(f"Unsupported SDK MSVC runtime: {runtime!r}")
    return MSVC_RUNTIME_FLAGS[runtime]


def copy_runtime_library(package, library, config):
    package = Path(package)
    # Repeated ABI builds must not retain aliases from an earlier SDK view.
    for pattern in ("libtsfile.so*", "libtsfile*.dylib", "tsfile.dll", "libtsfile.dll"):
        for stale in package.glob(pattern):
            stale.unlink()
    target = package / config["runtime_library"]
    shutil.copy2(library, target)
    return target


def strip_wheel_binaries(build_lib, platform):
    # Run before auditwheel/delocate rewrite load commands. Only wheel build
    # copies are stripped; SDKs and in-place development builds keep symbols.
    if platform not in {"linux", "darwin"}:
        return
    for path in sorted((Path(build_lib) / "tsfile").rglob("*")):
        if not path.is_file() or not (
            path.name.endswith((".so", ".dylib")) or ".so." in path.name
        ):
            continue
        if platform == "linux":
            subprocess.run(["strip", "--strip-unneeded", str(path)], check=True)
        else:
            subprocess.run(["xcrun", "strip", "-S", "-x", str(path)], check=True)
            subprocess.run(
                ["codesign", "--force", "--sign", "-", str(path)], check=True
            )


SANITIZER_SYMBOL_PREFIX = "__asan_"


def is_runtime_library(path):
    return Path(path).name.startswith(("libtsfile", "tsfile.dll"))


def native_binaries(root):
    """Return the binaries built for the running interpreter under *root*.

    Only the current platform/ABI extension suffix is matched, so stale
    artifacts from another OS or Python version in a reused tree are ignored.
    The copied TsFile runtime library is included explicitly.
    """
    extension_suffix = importlib.machinery.EXTENSION_SUFFIXES[0]
    root = Path(root)
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and (path.name.endswith(extension_suffix) or is_runtime_library(path))
    )


SANITIZER_RUNTIME_HINTS = ("libasan", "libclang_rt.asan")


def _run_tool(command, path):
    try:
        completed = subprocess.run(command, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"{command[0]} is required to verify sanitizer instrumentation"
        ) from exc
    if completed.returncode != 0:
        raise RuntimeError(
            f"failed to inspect {path} with {command[0]}: {completed.stderr.strip()}"
        )
    return completed.stdout


def _sanitizer_symbols(path):
    # Linux lists both defined and undefined dynamic symbols with -D; macOS
    # -gU lists only undefined external symbols, which is where the ASan
    # instrumentation references appear in a shared object.
    if sys.platform.startswith("linux"):
        command = ["nm", "-D", str(path)]
    elif sys.platform == "darwin":
        command = ["nm", "-gU", str(path)]
    else:
        raise RuntimeError(
            f"Sanitizer instrumentation checks are not supported on {sys.platform}"
        )
    return _run_tool(command, path)


def _linked_libraries(path):
    if sys.platform.startswith("linux"):
        command = ["readelf", "-d", str(path)]
    elif sys.platform == "darwin":
        command = ["otool", "-L", str(path)]
    else:
        raise RuntimeError(f"Sanitizer link checks are not supported on {sys.platform}")
    return _run_tool(command, path)


def is_sanitizer_instrumented(path):
    return SANITIZER_SYMBOL_PREFIX in _sanitizer_symbols(path)


def links_sanitizer_runtime(path):
    """True when the binary records a dependency on the sanitizer runtime."""
    linked = _linked_libraries(path)
    return any(hint in linked for hint in SANITIZER_RUNTIME_HINTS)


def verify_sanitizer_instrumentation(paths, expect_instrumented):
    """Check each native binary and return the inspected (path, bool) pairs.

    Instrumented binaries must have both compile-time ASan references and a
    link-time dependency on the sanitizer runtime. Raises RuntimeError with
    every offending path when a binary does not match *expect_instrumented*.
    Printing happens here so build logs show which artifacts were verified.
    """
    inspected = []
    failures = []
    for path in paths:
        path = Path(path)
        if not path.is_file():
            raise RuntimeError(f"Missing native binary: {path}")
        instrumented = is_sanitizer_instrumented(path)
        if expect_instrumented:
            instrumented = instrumented and links_sanitizer_runtime(path)
        inspected.append((path, instrumented))
        if instrumented != expect_instrumented:
            failures.append(path)
        state = "instrumented" if instrumented else "not instrumented"
        print(f"sanitizer check: {state}: {path}")

    if failures:
        expected = "instrumented" if expect_instrumented else "uninstrumented"
        details = "\n".join(f"  {path}" for path in failures)
        raise RuntimeError(
            f"Expected every native binary to be {expected}:\n{details}\n"
            "Use '-Dtsfile.sanitizer=<checks>' for a coordinated sanitizer "
            "build, or run a clean normal build for release artifacts."
        )
    return inspected
