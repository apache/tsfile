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
import platform
system = platform.system()
if system == "Windows":
    ctypes.WinDLL(os.path.join(os.path.dirname(__file__), "libtsfile.dll"), winmode=0)

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