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
import re
import shutil
import sys

import numpy as np

from pathlib import Path
from Cython.Build import cythonize
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py

ROOT = Path(__file__).parent.resolve()
PKG = ROOT / "tsfile"

# PEP 517 hooks execute setup.py without adding its directory to sys.path.
sys.path.insert(0, str(ROOT))
from _build_support import (
    copy_runtime_library,
    msvc_runtime_flag,
    native_binaries,
    read_sdk_config,
    strip_wheel_binaries,
    verify_sanitizer_instrumentation,
)

version = "2.5.1.dev"

# Comma-separated GCC/Clang sanitizer checks (for example "address"). Empty
# means a normal build. Maven sets this from -Dtsfile.sanitizer=<checks>.
SANITIZER = os.environ.get("TSFILE_SANITIZER", "").strip()
SANITIZER_SUPPORTED = sys.platform.startswith("linux") or sys.platform == "darwin"
if SANITIZER and not re.fullmatch(r"[A-Za-z0-9,_+-]+", SANITIZER):
    raise RuntimeError(
        f"Invalid TSFILE_SANITIZER value: {SANITIZER!r}; expected a "
        "comma-separated -fsanitize= list such as 'address'."
    )
if SANITIZER and not SANITIZER_SUPPORTED:
    raise RuntimeError(
        "Sanitizer builds of the Python bindings are supported on Linux and "
        "macOS only."
    )


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

# Destination of the copied C++ runtime library, used by the sanitizer check.
runtime_lib_dst = None

if sys.platform.startswith("linux"):
    candidates = sorted(
        CPP_LIB.rglob("libtsfile.so*"), key=lambda p: len(p.name), reverse=True
    )
    if not candidates:
        raise FileNotFoundError("missing libtsfile.so* in build output")
    src = candidates[0]
    sdk_config = read_sdk_config(CPP_OUT, src)
    runtime_lib_dst = copy_runtime_library(PKG, src, sdk_config)
    native_link_dir = src.parent

elif sys.platform == "darwin":
    candidates = sorted(CPP_LIB.rglob("libtsfile.*.dylib")) or list(
        CPP_LIB.rglob("libtsfile.dylib")
    )
    if not candidates:
        raise FileNotFoundError("missing libtsfile*.dylib in build output")
    src = candidates[0]
    sdk_config = read_sdk_config(CPP_OUT, src)
    runtime_lib_dst = copy_runtime_library(PKG, src, sdk_config)
    native_link_dir = src.parent

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
    sdk_config = read_sdk_config(CPP_OUT, dll_src)
    dll_dst = copy_runtime_library(PKG, dll_src, sdk_config)
    runtime_lib_dst = dll_dst
    native_link_dir = PKG

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


# Retain license/notice texts even though SDK headers are build inputs only.
license_dir = PKG / "licenses"
license_dir.mkdir(exist_ok=True)
for document in ("LICENSE", "NOTICE"):
    candidates = [CPP_OUT / "share/doc/tsfile" / document, ROOT.parent / document]
    source = next((path for path in candidates if path.is_file()), None)
    if source is None:
        raise FileNotFoundError(f"Missing SDK {document}: {candidates}")
    shutil.copy2(source, license_dir / document)


class BuildPy(build_py):
    def run(self):
        if "bdist_wheel" in self.distribution.script_args:
            # Reused build directories must not carry old SDK headers, import
            # libraries or binary aliases into a new runtime-only wheel.
            staging = Path(self.build_lib) / "tsfile"
            if staging.resolve() == PKG:
                raise ValueError("Wheel build_lib must be outside the source package")
            shutil.rmtree(staging, ignore_errors=True)
        super().run()


class BuildExt(build_ext):
    def run(self):
        wheel_build = (
            "bdist_wheel" in self.distribution.script_args and not self.inplace
        )
        if wheel_build and SANITIZER:
            raise RuntimeError(
                "Refusing to package a wheel from a sanitizer build. "
                "Unset TSFILE_SANITIZER and run a clean build for release "
                "artifacts; sanitizer binaries never ship."
            )

        super().run()

        if wheel_build:
            # Distutils can reuse object files from a previous sanitizer build
            # in the same directory; never let them into a wheel.
            staged = Path(self.build_lib) / "tsfile"
            if staged.is_dir() and SANITIZER_SUPPORTED:
                verify_sanitizer_instrumentation(
                    native_binaries(staged), expect_instrumented=False
                )
            strip_wheel_binaries(self.build_lib, sys.platform)
            return

        if not SANITIZER:
            return
        targets = [
            Path(self.get_ext_fullpath(extension.name)) for extension in self.extensions
        ]
        if runtime_lib_dst is not None:
            targets.append(Path(runtime_lib_dst))
        verify_sanitizer_instrumentation(targets, expect_instrumented=True)

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
library_dirs = [str(native_link_dir)]
include_dirs = [str(PKG), np.get_include(), str(PKG / "include")]
sdk_include = PKG / "include" / "tsfile"
if sdk_include.is_dir():
    include_dirs.append(str(sdk_include))

# Link arguments that carry the sanitizer runtime into every extension,
# including the ones that do not link libtsfile.
sanitizer_link_args = []

if sys.platform.startswith("linux"):
    libraries = ["tsfile"]
    extra_compile_args += [
        "-O1" if SANITIZER else "-O3",
        "-std=c++11",
        "-fvisibility=hidden",
        "-fPIC",
    ]
    runtime_library_dirs = ["$ORIGIN"]
    extra_link_args += ["-Wl,-rpath,$ORIGIN"]
elif sys.platform == "darwin":
    libraries = ["tsfile"]
    extra_compile_args += [
        "-O1" if SANITIZER else "-O3",
        "-std=c++11",
        "-fvisibility=hidden",
        "-fPIC",
    ]
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
        # The SDK records its resolved CRT; callers do not configure it twice.
        extra_compile_args.append(msvc_runtime_flag(sdk_config))
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

if SANITIZER:
    # -O1 keeps reports readable; frame pointers and debug info make the
    # allocation/free stacks actionable. Every extension is instrumented and
    # linked with the same check set as libtsfile.
    extra_compile_args += [
        "-g",
        "-fno-omit-frame-pointer",
        "-fno-optimize-sibling-calls",
        f"-fsanitize={SANITIZER}",
    ]
    sanitizer_link_args.append(f"-fsanitize={SANITIZER}")
    extra_link_args += sanitizer_link_args
    print(
        "setup.py: sanitizer build enabled: "
        f"checks={SANITIZER} (extensions use -O1, -g, -fno-omit-frame-pointer)"
    )

common = dict(
    language="c++",
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=libraries,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    runtime_library_dirs=runtime_library_dirs,
)

merge_common = common.copy()
merge_common.update(
    library_dirs=[],
    libraries=[],
    # These extensions do not link libtsfile, but they still contain
    # instrumented code and need the sanitizer runtime at link time.
    extra_link_args=list(sanitizer_link_args),
    runtime_library_dirs=[],
)

exts = [
    Extension("tsfile.dataset._merge", ["tsfile/dataset/_merge.pyx"], **merge_common),
    Extension(
        "tsfile.dataset._index",
        ["tsfile/dataset/_index.pyx"],
        **merge_common,
    ),
    Extension("tsfile.tsfile_py_cpp", ["tsfile/tsfile_py_cpp.pyx"], **common),
    Extension(
        "tsfile.tsfile_reader",
        ["tsfile/tsfile_reader.pyx", "tsfile/python_random_access_read_file.cc"],
        depends=["tsfile/python_random_access_read_file.h"],
        **common,
    ),
    Extension("tsfile.tsfile_writer", ["tsfile/tsfile_writer.pyx"], **common),
]

setup(
    name="tsfile",
    version=version,
    packages=["tsfile", "tsfile.dataset"],
    package_dir={"": "."},
    include_package_data=False,
    ext_modules=cythonize(exts, compiler_directives={"language_level": 3}),
    cmdclass={"build_ext": BuildExt, "build_py": BuildPy},
)
