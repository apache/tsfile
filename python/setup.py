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
CPP_OUT = ROOT / ".." / "cpp" / "target" / "build"
CPP_LIB = CPP_OUT / "lib"
CPP_INC = CPP_OUT / "include"

version = "2.2.1.dev"

if not CPP_INC.exists():
    raise FileNotFoundError(f"missing C++ headers: {CPP_INC}")
if (PKG / "include").exists():
    shutil.rmtree(PKG / "include")
shutil.copytree(CPP_INC, PKG / "include")
if sys.platform.startswith("linux"):
    candidates = sorted(
        CPP_LIB.glob("libtsfile.so*"), key=lambda p: len(p.name), reverse=True
    )
    if not candidates:
        raise FileNotFoundError("missing libtsfile.so* in build output")
    src = candidates[0]
    dst = PKG / src.name
    shutil.copy2(src, dst)
    link_name = PKG / "libtsfile.so"
    shutil.copy2(src, link_name)

elif sys.platform == "darwin":
    candidates = sorted(CPP_LIB.glob("libtsfile.*.dylib")) or list(
        CPP_LIB.glob("libtsfile.dylib")
    )
    if not candidates:
        raise FileNotFoundError("missing libtsfile*.dylib in build output")
    src = candidates[0]
    dst = PKG / src.name
    shutil.copy2(src, dst)
    link_name = PKG / "libtsfile.dylib"
    shutil.copy2(src, link_name)
elif sys.platform == "win32":
    for base_name in ("libtsfile",):
        dll_candidates = sorted(
            CPP_LIB.glob(f"{base_name}*.dll"), key=lambda p: len(p.name), reverse=True
        )
        dll_a_candidates = sorted(
            CPP_LIB.glob(f"{base_name}*.dll.a"), key=lambda p: len(p.name), reverse=True
        )

        if not dll_candidates:
            raise FileNotFoundError(f"missing {base_name}*.dll in build output")
        if not dll_a_candidates:
            raise FileNotFoundError(f"missing {base_name}*.dll.a in build output")

        dll_src = dll_candidates[0]
        dll_a_src = dll_a_candidates[0]

        shutil.copy2(dll_src, PKG / f"{base_name}.dll")
        shutil.copy2(dll_a_src, PKG / f"{base_name}.dll.a")

    # Copy MinGW runtime DLLs next to libtsfile.dll so Python can find them.
    # Python 3.8+ does not search PATH for DLLs; they must be in the same
    # directory as the .pyd extensions (registered via os.add_dll_directory).
    for _mingw_dll in ("libstdc++-6.dll", "libgcc_s_seh-1.dll", "libwinpthread-1.dll"):
        for _dir in os.environ.get("PATH", "").split(os.pathsep):
            _src = Path(_dir) / _mingw_dll
            if _src.is_file():
                shutil.copy2(_src, PKG / _mingw_dll)
                print(f"setup.py: copied {_mingw_dll} from {_src}")
                break
        else:
            print(f"setup.py: WARNING - {_mingw_dll} not found on PATH")
else:
    raise RuntimeError(f"Unsupported platform: {sys.platform}")


class BuildExt(build_ext):
    def run(self):
        super().run()

    def finalize_options(self):
        if sys.platform == "win32":
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
    libraries = ["tsfile"]
    extra_compile_args += [
        "-O2",
        "-std=c++11",
        "-DSIZEOF_VOID_P=8",
        "-D__USE_MINGW_ANSI_STDIO=1",
        "-DMS_WIN64",
        "-D_WIN64",
    ]
    extra_link_args += []
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
