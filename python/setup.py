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

import os
import platform
import shutil
import sys
import numpy as np

from pathlib import Path
from Cython.Build import cythonize
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext

ROOT = Path(__file__).parent.resolve()
PKG = ROOT / "tsfile"

version = "2.2.1.dev"


def _find_cpp_build():
    """Locate the C++ build output directory (one containing include/).

    The Maven-driven build emits ``cpp/target/build``; a direct CMake build
    emits ``cpp/build/<generator>``. Both layouts are supported, and an
    explicit override may be given via the ``TSFILE_CPP_BUILD`` env var.
    """
    candidates = []
    override = os.environ.get("TSFILE_CPP_BUILD")
    if override:
        candidates.append(Path(override))
    candidates += [
        ROOT / ".." / "cpp" / "target" / "build",
        ROOT / ".." / "cpp" / "build" / "msvc",
        ROOT / ".." / "cpp" / "build" / "Release",
        ROOT / ".." / "cpp" / "build" / "RelWithDebInfo",
        ROOT / ".." / "cpp" / "build" / "Debug",
    ]
    for cand in candidates:
        cand = cand.resolve()
        if (cand / "include").is_dir():
            return cand
    raise FileNotFoundError(
        "Could not locate the C++ build output (a directory containing "
        "include/). Build the C++ module first, or point TSFILE_CPP_BUILD "
        "at the build directory."
    )


def _find_lib(root, patterns):
    """Return the shortest-named file under ``root`` matching any pattern."""
    for pattern in patterns:
        hits = sorted(root.rglob(pattern), key=lambda p: len(p.name))
        if hits:
            return hits[0]
    return None


CPP_OUT = _find_cpp_build()
CPP_LIB = CPP_OUT / "lib"
CPP_INC = CPP_OUT / "include"

if not CPP_INC.exists():
    raise FileNotFoundError(f"missing C++ headers: {CPP_INC}")
if (PKG / "include").exists():
    shutil.rmtree(PKG / "include")
shutil.copytree(CPP_INC, PKG / "include")

# Windows toolchain: "msvc" links against an MSVC import library (tsfile.lib),
# "mingw" links against a MinGW import library (libtsfile.dll.a).
win_toolchain = None

if sys.platform.startswith("linux"):
    candidates = sorted(
        CPP_LIB.rglob("libtsfile.so*"), key=lambda p: len(p.name), reverse=True
    )
    if not candidates:
        raise FileNotFoundError("missing libtsfile.so* in build output")
    src = candidates[0]
    dst = PKG / src.name
    shutil.copy2(src, dst)
    link_name = PKG / "libtsfile.so"
    shutil.copy2(src, link_name)

elif sys.platform == "darwin":
    candidates = sorted(CPP_LIB.rglob("libtsfile.*.dylib")) or list(
        CPP_LIB.rglob("libtsfile.dylib")
    )
    if not candidates:
        raise FileNotFoundError("missing libtsfile*.dylib in build output")
    src = candidates[0]
    dst = PKG / src.name
    shutil.copy2(src, dst)
    link_name = PKG / "libtsfile.dylib"
    shutil.copy2(src, link_name)

elif sys.platform == "win32":
    # The shared library is named tsfile.dll (MSVC) or libtsfile.dll (MinGW).
    dll_src = _find_lib(CPP_LIB, ["tsfile*.dll", "libtsfile*.dll"])
    if dll_src is None:
        raise FileNotFoundError(f"missing tsfile DLL in build output: {CPP_LIB}")

    # Pick the import library and infer the toolchain from its kind.
    mingw_imp = _find_lib(CPP_LIB, ["libtsfile*.dll.a"])
    msvc_imp = _find_lib(CPP_LIB, ["tsfile*.lib", "libtsfile*.lib"])
    if mingw_imp is not None and msvc_imp is None:
        win_toolchain = "mingw"
        imp_src = mingw_imp
    elif msvc_imp is not None:
        win_toolchain = "msvc"
        imp_src = msvc_imp
    else:
        raise FileNotFoundError(
            f"missing tsfile import library (*.lib or *.dll.a) in {CPP_LIB}"
        )

    # For MSVC the DLL is already named tsfile.dll.  For MinGW, keep the original
    # name (libtsfile.dll): the import library (libtsfile.dll.a) has that name
    # baked in, so Cython .pyd files record "libtsfile.dll" in their PE import
    # table and Windows must find it by that exact name at runtime.
    dll_dst = PKG / dll_src.name
    shutil.copy2(dll_src, dll_dst)

    # Copy import library with a name matching the DLL.
    if win_toolchain == "mingw":
        imp_dst = PKG / "tsfile.dll.a"
    else:
        imp_dst = PKG / "tsfile.lib"
    shutil.copy2(imp_src, imp_dst)
    print(f"setup.py: Windows toolchain = {win_toolchain}")
    print(
        f"setup.py: copied {dll_src.name} -> {dll_dst.name} and {imp_src.name} -> {imp_dst.name}"
    )

    if win_toolchain == "mingw":
        # Copy MinGW runtime DLLs next to tsfile.dll so Python can find them.
        # Python 3.8+ does not search PATH for DLLs; they must sit in the
        # same directory as the .pyd extensions (os.add_dll_directory).
        # Prefer runtime DLLs captured from the same MSYS2 environment that
        # built libtsfile.dll.  Mixing MinGW runtime DLLs from a different
        # install can load successfully but fail later with WinError 127 when
        # libtsfile.dll imports a newer runtime symbol.
        _mingw_search_dirs = [CPP_LIB]
        for _dir in os.environ.get("PATH", "").split(os.pathsep):
            if _dir:
                _mingw_search_dirs.append(Path(_dir))
        _msys_prefix = os.environ.get("MSYSTEM_PREFIX")
        if _msys_prefix:
            _mingw_search_dirs.append(Path(_msys_prefix) / "bin")
        for _extra in (
            Path("C:/msys64/mingw64/bin"),
            Path("C:/ProgramData/mingw64/mingw64/bin"),
        ):
            if _extra.is_dir():
                _mingw_search_dirs.append(_extra)

        for _mingw_dll in (
            "libstdc++-6.dll",
            "libgcc_s_seh-1.dll",
            "libwinpthread-1.dll",
        ):
            for _dir in _mingw_search_dirs:
                _src = _dir / _mingw_dll
                if _src.is_file():
                    shutil.copy2(_src, PKG / _mingw_dll)
                    print(f"setup.py: copied {_mingw_dll} from {_src}")
                    break
            else:
                raise FileNotFoundError(
                    f"setup.py: MinGW runtime DLL {_mingw_dll} not found; "
                    "ensure mingw-w64-x86_64-gcc is on PATH or MSYSTEM_PREFIX is set"
                )
else:
    raise RuntimeError(f"Unsupported platform: {sys.platform}")


class BuildExt(build_ext):
    def run(self):
        super().run()

    def finalize_options(self):
        # MinGW must be requested explicitly; MSVC is the default Windows
        # compiler distutils selects, which matches an MSVC-built libtsfile.
        if sys.platform == "win32" and win_toolchain == "mingw":
            self.compiler = "mingw32"
        super().finalize_options()


extra_compile_args = []
extra_link_args = []
runtime_library_dirs = []
libraries = []
library_dirs = [str(PKG)]
include_dirs = [str(PKG), np.get_include(), str(PKG / "include")]

if sys.platform.startswith("linux"):
    libraries = ["tsfile"]
    extra_compile_args += ["-O3", "-std=c++11", "-fvisibility=hidden", "-fPIC"]
    runtime_library_dirs = ["$ORIGIN"]
    extra_link_args += ["-Wl,-rpath,$ORIGIN"]
elif sys.platform == "darwin":
    libraries = ["tsfile"]
    extra_compile_args += ["-O3", "-std=c++11", "-fvisibility=hidden", "-fPIC"]
    extra_link_args += ["-Wl,-rpath,@loader_path", "-stdlib=libc++"]
elif sys.platform == "win32":
    if win_toolchain == "mingw":
        # Resolve EH/runtime symbols from MinGW runtimes before libtsfile.dll.a.
        # Otherwise _Unwind_Resume is recorded against libtsfile.dll and loading
        # the .pyd fails with WinError 127 on Windows.
        libraries = ["gcc_s", "stdc++", "tsfile"]
        extra_compile_args += [
            "-O2",
            "-std=c++11",
            "-DSIZEOF_VOID_P=8",
            "-D__USE_MINGW_ANSI_STDIO=1",
            "-DMS_WIN64",
            "-D_WIN64",
        ]
    else:  # msvc
        libraries = ["tsfile"]
        # cl.exe rejects the GCC-style flags above. Mirror the options the
        # C++ module itself is built with (see cpp/CMakeLists.txt). C++17 is
        # required because Cython 3 emits inline variables (error C7525); the
        # C++11 headers compile cleanly under the newer standard.
        extra_compile_args += [
            "/O2",
            "/std:c++17",
            "/EHsc",
            "/bigobj",
            "/utf-8",
            "/Zc:__cplusplus",
            "/DNOMINMAX",
            "/D_CRT_SECURE_NO_WARNINGS",
            "/D_CRT_NONSTDC_NO_WARNINGS",
            "/D_SCL_SECURE_NO_WARNINGS",
            "/D_WINSOCK_DEPRECATED_NO_WARNINGS",
        ]
else:
    raise RuntimeError(f"Unsupported platform: {sys.platform}")

common = dict(
    language="c++",
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=libraries,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    runtime_library_dirs=runtime_library_dirs,
)

exts = [
    Extension("tsfile.tsfile_py_cpp", ["tsfile/tsfile_py_cpp.pyx"], **common),
    Extension("tsfile.tsfile_reader", ["tsfile/tsfile_reader.pyx"], **common),
    Extension("tsfile.tsfile_writer", ["tsfile/tsfile_writer.pyx"], **common),
]

setup(
    name="tsfile",
    version=version,
    packages=["tsfile", "tsfile.dataset"],
    package_dir={"": "."},
    include_package_data=True,
    ext_modules=cythonize(exts, compiler_directives={"language_level": 3}),
    cmdclass={"build_ext": BuildExt},
)
