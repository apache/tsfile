#!/usr/bin/env python3
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

"""Reject macOS packages that load external third-party dynamic libraries."""

import argparse
from pathlib import Path
import re
import subprocess

LOAD_COMMANDS = {
    "LC_LOAD_DYLIB",
    "LC_LOAD_WEAK_DYLIB",
    "LC_REEXPORT_DYLIB",
    "LC_LAZY_LOAD_DYLIB",
    "LC_LOAD_UPWARD_DYLIB",
}
MACHO_MAGICS = {
    bytes.fromhex(value)
    for value in (
        "feedface",
        "cefaedfe",
        "feedfacf",
        "cffaedfe",
        "cafebabe",
        "bebafeca",
        "cafebabf",
        "bfbafeca",
    )
}


def parse_dependencies(output):
    imports = set()
    for block in re.split(r"(?m)^Load command \d+\s*$", output):
        command = re.search(r"^\s*cmd (LC_\w+)\s*$", block, re.M)
        if command is None or command[1] not in LOAD_COMMANDS:
            continue
        name = re.search(r"^\s*name (.+?) \(offset \d+\)\s*$", block, re.M)
        if name is None:
            raise ValueError("otool reported a dylib load command without a name")
        imports.add(name[1])
    if not imports:
        raise ValueError("otool did not report any dylib dependencies")
    return imports


def validate_dependencies(binary, imports, prefix):
    binary = Path(binary).resolve()
    prefix = Path(prefix).resolve()
    for name in sorted(imports):
        if name.startswith(("/usr/lib/", "/System/Library/")):
            continue
        # BUNDLED codecs must not remain separate dylibs, even when installed
        # on the runner. The only non-system dylib is TsFile itself.
        if not re.fullmatch(r"libtsfile(?:\.[0-9]+)*\.dylib", Path(name).name):
            raise ValueError(
                f"{binary.name} needs an external third-party dylib: {name}"
            )
        if name.startswith("@rpath/"):
            suffix = name.removeprefix("@rpath/")
            candidates = [prefix / "lib" / suffix, prefix / suffix]
        elif name.startswith("@loader_path/"):
            candidates = [binary.parent / name.removeprefix("@loader_path/")]
        elif name.startswith("@executable_path/") and binary.name == "tsfile-cli":
            candidates = [binary.parent / name.removeprefix("@executable_path/")]
        elif Path(name).is_absolute():
            candidates = [Path(name)]
        else:
            candidates = []
        if not any(
            path.is_file() and prefix in path.resolve().parents for path in candidates
        ):
            raise ValueError(f"{binary.name} needs an unshipped TsFile dylib: {name}")


def verify_prefix(prefix):
    prefix = Path(prefix).resolve()
    binaries = set()
    for path in prefix.rglob("*"):
        if path.is_file():
            with path.open("rb") as stream:
                if stream.read(4) in MACHO_MAGICS:
                    binaries.add(path.resolve())
    if not binaries:
        raise ValueError(f"No Mach-O binaries found under {prefix}")
    for binary in sorted(binaries):
        output = subprocess.run(
            ["otool", "-l", str(binary)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        ).stdout
        imports = parse_dependencies(output)
        validate_dependencies(binary, imports, prefix)
        print(
            f"{binary.name}: all dylib dependencies are macOS system libraries or packaged TsFile"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prefix", type=Path)
    verify_prefix(parser.parse_args().prefix)


if __name__ == "__main__":
    main()
