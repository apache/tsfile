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

"""Run Go's own tests against the manually installed native package."""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def exported_c_symbols(output):
    symbols = re.findall(
        r"^\s*Name: ([A-Za-z_][A-Za-z0-9_]*)\s*$", output, re.MULTILINE
    )
    if not symbols:
        raise ValueError("No C exports found in the installed TsFile DLL")
    return sorted(set(symbols))


def race_supported(goos, goarch):
    return (goos, goarch) in {
        ("linux", "amd64"),
        ("linux", "arm64"),
        ("darwin", "amd64"),
        ("darwin", "arm64"),
        ("windows", "amd64"),
    }


def pkgconfig_file(prefix):
    candidates = sorted(
        {
            *prefix.glob("lib*/pkgconfig/libtsfile.pc"),
            *prefix.glob("lib*/*/pkgconfig/libtsfile.pc"),
        }
    )
    if len(candidates) != 1:
        raise ValueError(
            f"Expected one installed libtsfile.pc under {prefix}: {candidates}"
        )
    return candidates[0]


def run(command, **kwargs):
    print("+ " + subprocess.list2cmdline([str(item) for item in command]), flush=True)
    subprocess.run(command, check=True, timeout=900, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--goarch", choices=("amd64", "arm64"), required=True)
    parser.add_argument("--llvm-bin", type=Path)
    parser.add_argument("--build-dir", type=Path, default=Path("go-test-output"))
    arguments = parser.parse_args()
    prefix = arguments.prefix.resolve()
    header = prefix / "include/tsfile/cwrapper/tsfile_cwrapper.h"
    if not header.is_file():
        raise ValueError(f"Missing installed C header: {header}")
    goos, goarch = subprocess.check_output(
        ["go", "env", "GOOS", "GOARCH"], text=True
    ).split()
    if goarch != arguments.goarch:
        raise ValueError(f"Go architecture {goarch} does not match {arguments.goarch}")
    environment = os.environ.copy()
    environment["CGO_ENABLED"] = "1"
    if goos == "windows":
        if arguments.llvm_bin is None:
            raise ValueError("Windows cgo requires --llvm-bin")
        tools = arguments.llvm_bin.resolve()
        triple = {"amd64": "x86_64", "arm64": "aarch64"}[goarch] + "-w64-mingw32"
        environment["CC"] = (tools / f"{triple}-clang.exe").as_posix()
        environment["CXX"] = (tools / f"{triple}-clang++.exe").as_posix()
        dlls = [prefix / "bin" / name for name in ("tsfile.dll", "libtsfile.dll")]
        dlls = [dll for dll in dlls if dll.is_file()]
        if len(dlls) != 1:
            raise ValueError(f"Expected one installed TsFile DLL: {dlls}")
        dll = dlls[0]
        output = subprocess.check_output(
            [tools / "llvm-readobj.exe", "--coff-exports", dll], text=True
        )
        symbols = exported_c_symbols(output)
        imports = arguments.build_dir.resolve() / "imports"
        imports.mkdir(parents=True, exist_ok=True)
        definition = imports / "tsfile.def"
        definition.write_text(
            f"LIBRARY {dll.name}\nEXPORTS\n" + "\n".join(symbols) + "\n"
        )
        machine = {"amd64": "i386:x86-64", "arm64": "arm64"}[goarch]
        run(
            [
                tools / "llvm-dlltool.exe",
                "-m",
                machine,
                "-d",
                definition,
                "-l",
                imports / "libtsfile.dll.a",
            ]
        )
        cflags = [
            f"-I{prefix.as_posix()}/include",
            f"-I{prefix.as_posix()}/include/tsfile",
        ]
        ldflags = [f"-L{imports.as_posix()}", "-ltsfile"]
        environment["PATH"] = os.pathsep.join(
            [str(dll.parent), str(tools), environment.get("PATH", "")]
        )
    else:
        metadata = pkgconfig_file(prefix)
        environment["PKG_CONFIG_PATH"] = str(metadata.parent)
        cflags = shlex.split(
            subprocess.check_output(
                ["pkg-config", "--cflags", "libtsfile"], text=True, env=environment
            )
        )
        ldflags = shlex.split(
            subprocess.check_output(
                ["pkg-config", "--libs", "libtsfile"], text=True, env=environment
            )
        )
        ldflags.append(f"-Wl,-rpath,{metadata.parent.parent}")
    environment["CGO_CFLAGS"] = shlex.join(cflags)
    environment["CGO_LDFLAGS"] = shlex.join(ldflags)
    print(f"Testing installed SDK {prefix} with Go {goos}/{goarch}", flush=True)
    print(f"CGO_CFLAGS={environment['CGO_CFLAGS']}", flush=True)
    print(f"CGO_LDFLAGS={environment['CGO_LDFLAGS']}", flush=True)
    run(["go", "version"], env=environment)
    for command in (
        ["go", "vet", "./..."],
        ["go", "test", "-count=1", "-timeout=10m", "./..."],
    ):
        run(command, cwd=ROOT / "go", env=environment)
    if race_supported(goos, goarch):
        run(
            ["go", "test", "-race", "-count=1", "-timeout=10m", "./..."],
            cwd=ROOT / "go",
            env=environment,
        )
    else:
        print(
            f"NOT APPLICABLE: Go's race detector does not support {goos}/{goarch}",
            flush=True,
        )


if __name__ == "__main__":
    main()
