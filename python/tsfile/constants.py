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

from enum import unique, IntEnum


@unique
class TSDataType(IntEnum):
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    FLOAT = 3
    DOUBLE = 4
    TEXT = 5
    TIMESTAMP = 8
    DATE = 9
    BLOB = 10
    STRING = 11

    def to_py_type(self):
        if self == TSDataType.BOOLEAN:
            return bool
        elif self == TSDataType.INT32:
            return int
        elif self == TSDataType.INT64:
            return int
        elif self == TSDataType.FLOAT:
            return float
        elif self == TSDataType.DOUBLE:
            return float
        elif self == TSDataType.TEXT or self == TSDataType.STRING:
            return str

    def to_pandas_dtype(self):
        """
        Convert datatype to pandas dtype
        """
        if self == TSDataType.BOOLEAN:
            return "bool"
        elif self == TSDataType.INT32:
            return "int32"
        elif self == TSDataType.INT64:
            return "int64"
        elif self == TSDataType.FLOAT:
            return "float32"
        elif self == TSDataType.DOUBLE:
            return "float64"
        elif self == TSDataType.TEXT or self == TSDataType.STRING:
            return "object"
        elif self == TSDataType.TIMESTAMP:
            return "datetime64[ns]"
        elif self == TSDataType.DATE:
            return "int32"
        elif self == TSDataType.BLOB:
            return "object"
        else:
            raise ValueError(f"Unknown data type: {self}")


@unique
class TSEncoding(IntEnum):
    PLAIN = 0
    DICTIONARY = 1
    RLE = 2
    DIFF = 3
    TS_2DIFF = 4
    BITMAP = 5
    GORILLA_V1 = 6
    REGULAR = 7
    GORILLA = 8
    ZIGZAG = 9
    CHIMP = 11
    SPRINTZ = 12
    RLBE = 13


@unique
class Compressor(IntEnum):
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    SDT = 4
    PAA = 5
    PLA = 6
    LZ4 = 7
    ZSTD = 8
    LZMA2 = 9


@unique
class ColumnCategory(IntEnum):
    TAG = 0
    FIELD = 1
