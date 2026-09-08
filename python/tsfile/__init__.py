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

import ctypes
import os
import sys

_pkg_dir = os.path.dirname(os.path.abspath(__file__))


def _preload_dll(path):
    if not os.path.isfile(path):
        return
    try:
        ctypes.CDLL(path)
    except OSError:
        pass


if sys.platform == "win32":
    # Keep the handle alive for the lifetime of this module. CPython's reference
    # counting frees the object immediately if not stored, which calls
    # RemoveDllDirectory and undoes the registration before any .pyd is loaded.
    _dll_dir = os.add_dll_directory(_pkg_dir)
    # Preload MinGW runtime DLLs before libtsfile.dll. Python 3.8+ does not
    # search PATH for DLL dependencies; they must live next to the extensions.
    for _mingw_dll in (
        "libwinpthread-1.dll",
        "libgcc_s_seh-1.dll",
        "libstdc++-6.dll",
    ):
        _preload_dll(os.path.join(_pkg_dir, _mingw_dll))
    # Preload the tsfile DLL so Windows finds it when loading Cython extensions.
    # MSVC builds produce "tsfile.dll"; MinGW builds produce "libtsfile.dll".
    _tsfile_cdll = None
    for _dll_name in ("tsfile.dll", "libtsfile.dll"):
        _tsfile_dll = os.path.join(_pkg_dir, _dll_name)
        if os.path.isfile(_tsfile_dll):
            _tsfile_cdll = ctypes.CDLL(_tsfile_dll)
            break
    else:
        raise FileNotFoundError(
            f"tsfile DLL (tsfile.dll or libtsfile.dll) not found in {_pkg_dir}. "
            "Re-build the C++ module and reinstall the Python package."
        )
elif sys.platform == "darwin":
    _tsfile_dylib = os.path.join(_pkg_dir, "libtsfile.dylib")
    if os.path.isfile(_tsfile_dylib):
        ctypes.CDLL(_tsfile_dylib, mode=os.RTLD_GLOBAL)

from .constants import *
from .schema import *
from .row_record import *
from .tablet import *
from .field import *
from .date_utils import *
from .exceptions import *
from .tsfile_reader import TsFileReaderPy as TsFileReader, ResultSetPy as ResultSet
from .tag_filter import (
    TagFilter,
    tag_eq,
    tag_neq,
    tag_lt,
    tag_lteq,
    tag_gt,
    tag_gteq,
    tag_regexp,
    tag_not_regexp,
    tag_is_null,
    tag_is_not_null,
    tag_between,
    tag_not_between,
)
from .tsfile_writer import TsFileWriterPy as TsFileWriter
from .tsfile_py_cpp import (
    get_file_read_backend,
    get_tsfile_config,
    set_file_read_backend,
    set_tsfile_config,
)
from .tsfile_table_writer import TsFileTableWriter
from .utils import to_dataframe, dataframe_to_tsfile
from .dataset import TsFileDataFrame, Timeseries, AlignedTimeseries, SeriesPath
