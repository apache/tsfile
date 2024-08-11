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

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from Cython.Build import cythonize
import numpy as np
import platform
import shutil
import os


def copy_lib_files(source_dir, target_dir):
    if platform.system() == "Linux":
        lib_file_name = "libtsfile.so.1.0"
        link_name = os.path.join(target_dir, "libtsfile.so")
        if os.path.exists(link_name):
            os.remove(link_name)
        os.symlink(lib_file_name, link_name)
    elif platform.system() == "Darwin":
        lib_file_name = "libtsfile.1.0.dylib"
        link_name = os.path.join(target_dir, "libtsfile.dylib")
        if os.path.exists(link_name):
            os.remove(link_name)
        os.symlink(lib_file_name, link_name)
    else:
        lib_file_name = "libtsfile.dll"

    source = os.path.join(source_dir, lib_file_name)
    target = os.path.join(target_dir, lib_file_name)
    shutil.copyfile(source, target)


def copy_header(source, target):
    shutil.copyfile(source, target)


class BuildExt(build_ext):
    def build_extensions(self):
        numpy_include = np.get_include()
        for ext in self.extensions:
            ext.include_dirs.append(numpy_include)
        super().build_extensions()


project_dir = os.path.dirname(__file__)
libtsfile_shard_dir = os.path.join(project_dir, "..", "cpp", "target", "build", "lib")
libtsfile_dir = os.path.join(project_dir, "tsfile")
include_dir = os.path.join(project_dir, "tsfile")
source_file = os.path.join(project_dir, "tsfile", "tsfile_pywrapper.pyx")

copy_lib_files(libtsfile_shard_dir, libtsfile_dir)

source_include_dir = os.path.join(
    project_dir, "..", "cpp", "src", "cwrapper", "TsFile-cwrapper.h"
)
target_include_dir = os.path.join(project_dir, "tsfile", "TsFile-cwrapper.h")
copy_header(source_include_dir, target_include_dir)

system = platform.system()
is_windows = system == "Windows"

compile_args = ["-std=c++11"]
if is_windows:
    compile_args.append("-DMS_WIN64")

runtime_library_dirs = [libtsfile_dir] if not is_windows else None

ext_modules_tsfile = [
    Extension(
        "tsfile.tsfile_pywrapper",
        sources=[source_file],
        libraries=["tsfile"],
        library_dirs=[libtsfile_dir],
        include_dirs=[include_dir, np.get_include()],
        runtime_library_dirs=runtime_library_dirs,
        extra_compile_args=compile_args,
        language="c++",
    )
]

setup(
    name="tsfile",
    version="0.1",
    description="Tsfile reader and writer for python",
    url="https://tsfile.apache.org",
    author='"Apache TsFile"',
    packages=["tsfile"],
    license="Apache 2.0",
    ext_modules=cythonize(ext_modules_tsfile),
    cmdclass={"build_ext": BuildExt},
    include_dirs=[np.get_include()],
    package_data={
        "tsfile": [
            os.path.join("*tsfile", "*.so*"),
            os.path.join("*tsfile", "*.dylib"),
            os.path.join("*tsfile", "*.pyd"),
            os.path.join("*tsfile", "*.dll"),
            os.path.join("tsfile", "tsfile.py"),
        ]
    },
    include_package_data=True,
)
