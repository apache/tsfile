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

import json
from pathlib import Path
import shutil
import subprocess

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
