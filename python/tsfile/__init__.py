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

if sys.platform == "win32":
    _pkg_dir = os.path.dirname(os.path.abspath(__file__))
    os.add_dll_directory(_pkg_dir)
    # Preload libtsfile.dll with absolute path to bypass DLL search issues.
    # This ensures it's already in memory when .pyd extensions reference it.
    _tsfile_dll = os.path.join(_pkg_dir, "libtsfile.dll")
    if os.path.isfile(_tsfile_dll):
        ctypes.CDLL(_tsfile_dll)

from .constants import *
from .schema import *
from .row_record import *
from .tablet import *
from .field import *
from .date_utils import *
from .exceptions import *
from .tsfile_reader import TsFileReaderPy as TsFileReader, ResultSetPy as ResultSet
from .tsfile_writer import TsFileWriterPy as TsFileWriter
from .tsfile_py_cpp import get_tsfile_config, set_tsfile_config
from .tsfile_table_writer import TsFileTableWriter
from .utils import to_dataframe, dataframe_to_tsfile