#!/usr/bin/env python3
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

"""Reject portable Windows binaries that need an unshipped DLL or dynamic CRT."""

import argparse
import re
import subprocess
from pathlib import Path

# Windows 10 / Server 2022 inbox DLLs used by TsFile and the static CRT. This
# explicit list deliberately does not trust whatever happens to be on PATH or
# in the development runner's System32 (which also contains the VC redist).
SYSTEM_DLLS = {
    "advapi32.dll",
    "bcrypt.dll",
    "crypt32.dll",
    "kernel32.dll",
    "ntdll.dll",
    "ole32.dll",
    "oleaut32.dll",
    "rpcrt4.dll",
    "secur32.dll",
    "shell32.dll",
    "shlwapi.dll",
    "user32.dll",
    "version.dll",
    "ws2_32.dll",
}
DYNAMIC_CRT = re.compile(r"^(msvcp|msvcr|vcruntime|ucrtbase|api-ms-win-crt-)", re.I)


def parse_dependents(output: str) -> set[str]:
    imports = set(re.findall(r"^\s+([A-Za-z0-9_.-]+\.dll)\s*$", output, re.M | re.I))
    if not imports:
        raise ValueError("dumpbin did not report any DLL imports")
    return {name.lower() for name in imports}


def validate_dependents(binary: str, imports: set[str], colocated: set[str]) -> None:
    for name in sorted(imports):
        if DYNAMIC_CRT.match(name):
            raise ValueError(
                f"{binary} imports {name}; the ZIP requires a static MSVC runtime"
            )
        if (
            name not in SYSTEM_DLLS
            and not name.startswith("api-ms-win-")
            and name not in colocated
        ):
            raise ValueError(f"{binary} needs an unshipped non-system DLL: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prefix", type=Path)
    args = parser.parse_args()
    binaries = sorted(
        path
        for path in args.prefix.rglob("*")
        if path.suffix.lower() in (".exe", ".dll")
    )
    if not binaries:
        parser.error("the prefix contains no Windows binaries")
    for binary in binaries:
        output = subprocess.run(
            ["dumpbin", "/nologo", "/dependents", str(binary)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        ).stdout
        imports = parse_dependents(output)
        colocated = {
            path.name.lower()
            for path in binary.parent.iterdir()
            if path.suffix.lower() == ".dll"
        }
        validate_dependents(binary.name, imports, colocated)
        print(
            f"{binary.name}: static CRT; all {len(imports)} DLL dependencies are inbox or colocated"
        )


if __name__ == "__main__":
    main()
