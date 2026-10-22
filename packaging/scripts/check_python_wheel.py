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

"""Reject build-only files, repeated native libraries and ELF debug sections."""

import argparse
import hashlib
from pathlib import Path
import struct
from zipfile import ZipFile


def elf_debug_sections(data):
    if data[:4] != b"\x7fELF":
        return []
    if data[4] != 2:
        raise ValueError("Expected a 64-bit ELF for the release wheel matrix")
    endian = "<" if data[5] == 1 else ">"
    offset = struct.unpack_from(endian + "Q", data, 40)[0]
    size, count, names_index = struct.unpack_from(endian + "HHH", data, 58)
    sections = [
        struct.unpack_from(endian + "IIQQQQIIQQ", data, offset + i * size)
        for i in range(count)
    ]
    names_header = sections[names_index]
    names = data[names_header[4] : names_header[4] + names_header[5]]
    result = []
    for section in sections:
        name = names[section[0] :].split(b"\0", 1)[0].decode("ascii")
        if name.startswith((".debug_", ".zdebug_")):
            result.append(name)
    return result


def check_wheel(path):
    with ZipFile(path) as wheel:
        files = [info for info in wheel.infolist() if not info.is_dir()]
        names = [info.filename for info in files]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate ZIP entries in {path}")
        forbidden = [
            name
            for name in names
            if name.startswith("tsfile/include/")
            or Path(name).suffix.lower()
            in {
                ".h",
                ".hpp",
                ".pxd",
                ".pxi",
                ".pyx",
                ".a",
                ".lib",
                ".pdb",
                ".cc",
                ".cpp",
            }
        ]
        if forbidden:
            raise ValueError(f"Build-only files in {path}: {forbidden[:8]}")
        native = [
            name
            for name in names
            if Path(name).name.startswith("libtsfile")
            or Path(name).name == "tsfile.dll"
        ]
        if len(native) != 1:
            raise ValueError(f"Expected one TsFile runtime library in {path}: {native}")
        hashes = {}
        for name in names:
            if not (name.endswith((".so", ".dylib", ".dll", ".pyd")) or ".so." in name):
                continue
            data = wheel.read(name)
            debug = elf_debug_sections(data)
            if debug:
                raise ValueError(f"ELF debug sections in {path}/{name}: {debug}")
            digest = hashlib.sha256(data).hexdigest()
            if digest in hashes:
                raise ValueError(f"Repeated binary in {path}: {hashes[digest]}, {name}")
            hashes[digest] = name
        raw = sum(info.file_size for info in files)
        print(
            f"PASS: {Path(path).name}: {Path(path).stat().st_size} bytes compressed, "
            f"{raw} bytes installed; one runtime library, no build-only files or ELF debug sections"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheelhouse", type=Path)
    parser.add_argument("wheels", type=Path, nargs="*")
    arguments = parser.parse_args()
    wheels = arguments.wheels
    if arguments.wheelhouse:
        wheels += sorted(arguments.wheelhouse.glob("*.whl"))
    if not wheels:
        raise ValueError("No wheels found for content validation")
    for wheel in wheels:
        check_wheel(wheel)


if __name__ == "__main__":
    main()
