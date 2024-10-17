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

system = platform.system()


def copy_lib_files(source_dir, target_dir, suffix):
    lib_file_name = f"libtsfile.{suffix}"
    source = os.path.join(source_dir, lib_file_name)
    target = os.path.join(target_dir, lib_file_name)

    if os.path.exists(source):
        shutil.copyfile(source, target)

    if system == "Linux":
        link_name = os.path.join(target_dir, "libtsfile.so")
        if os.path.exists(link_name):
            os.remove(link_name)
        os.symlink(lib_file_name, link_name)
    elif system == "Darwin":
        link_name = os.path.join(target_dir, "libtsfile.dylib")
        if os.path.exists(link_name):
            os.remove(link_name)
        os.symlink(lib_file_name, link_name)


def copy_header(source, target):
    if os.path.exists(source):
        shutil.copyfile(source, target)


class BuildExt(build_ext):
    def build_extensions(self):
        numpy_include = np.get_include()
        for ext in self.extensions:
            ext.include_dirs.append(numpy_include)
        super().build_extensions()

    def finalize_options(self):
        if platform.system() == "Windows":
            self.compiler = 'mingw32'
        super().finalize_options()


project_dir = os.path.dirname(os.path.abspath(__file__))

libtsfile_shard_dir = os.path.join(project_dir, "..", "cpp", "target", "build", "lib")
libtsfile_dir = os.path.join(project_dir, "tsfile")
include_dir = os.path.join(project_dir, "tsfile")
source_file = os.path.join("tsfile", "tsfile_pywrapper.pyx")

source_include_dir = os.path.join(
    project_dir, "..", "cpp", "src", "cwrapper", "TsFile-cwrapper.h"
)
target_include_dir = os.path.join(project_dir, "tsfile", "TsFile-cwrapper.h")
copy_header(source_include_dir, target_include_dir)

if system == "Darwin":
    copy_lib_files(libtsfile_shard_dir, libtsfile_dir, "1.0.dylib")
elif system == "Linux":
    copy_lib_files(libtsfile_shard_dir, libtsfile_dir, "so.1.0")
else:
    copy_lib_files(libtsfile_shard_dir, libtsfile_dir, "dll")

ext_modules_tsfile = [
    Extension(
        "tsfile.tsfile_pywrapper",
        sources=[source_file],
        libraries=["tsfile"],
        library_dirs=[libtsfile_dir],
        include_dirs=[include_dir, np.get_include()],
        runtime_library_dirs=[libtsfile_dir] if platform.system() != "Windows" else None,
        extra_compile_args=["-std=c++11"] if platform.system() != "Windows" else ["-std=c++11", "-DMS_WIN64"],
        language="c++",
    )
]

setup(
    name="tsfile",
    version="1.2.0.dev0",
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
            "libtsfile.*",
        ]
    },
    include_package_data=True,
)
