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

import numpy as np

TIME_COLUMN = "time"

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

    def is_compatible_with(self, other: 'TSDataType') -> bool:
        if self == other:
            return True
        return other in _TSDATATYPE_COMPATIBLE_SOURCES.get(self, ())

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
        elif self == TSDataType.BLOB:
            return bytes
        elif self == TSDataType.DATE:
            return object
        elif self == TSDataType.TIMESTAMP:
            return int

    def to_pandas_dtype(self):
        """
        Convert datatype to pandas dtype
        """
        if self == TSDataType.BOOLEAN:
            return "boolean"
        elif self == TSDataType.INT32:
            return "Int32"
        elif self == TSDataType.INT64:
            return "Int64"
        elif self == TSDataType.FLOAT:
            return "Float32"
        elif self == TSDataType.DOUBLE:
            return "Float64"
        elif self == TSDataType.TEXT or self == TSDataType.STRING:
            return "object"
        elif self == TSDataType.TIMESTAMP:
            return "Int64"
        elif self == TSDataType.DATE:
            return "object"
        elif self == TSDataType.BLOB:
            return "object"
        else:
            raise ValueError(f"Unknown data type: {self}")

    @classmethod
    def from_pandas_datatype(cls, dtype):
        if dtype is np.bool_:
            return cls.BOOLEAN
        elif dtype is np.int32:
            return cls.INT32
        elif dtype is np.int64:
            return cls.INT64
        elif dtype is np.float32:
            return cls.FLOAT
        elif dtype is np.float64:
            return cls.DOUBLE
        elif dtype is np.object_:
            return cls.STRING

        try:
            import pandas as pd
            if hasattr(pd, 'StringDtype') and isinstance(dtype, pd.StringDtype):
                return cls.STRING
        except (ImportError, AttributeError):
            pass

        if hasattr(dtype, 'type'):
            dtype = dtype.type
            if dtype is np.bool_:
                return cls.BOOLEAN
            elif dtype is np.int32:
                return cls.INT32
            elif dtype is np.int64:
                return cls.INT64
            elif dtype is np.float32:
                return cls.FLOAT
            elif dtype is np.float64:
                return cls.DOUBLE
            elif dtype is np.object_:
                return cls.STRING

        dtype_str = str(dtype)

        if 'stringdtype' in dtype_str.lower() or dtype_str.startswith('string'):
            return cls.STRING

        dtype_map = {
            'bool': cls.BOOLEAN,
            'boolean': cls.BOOLEAN,
            'int32': cls.INT32,
            'Int32': cls.INT32,
            'int64': cls.INT64,
            'Int64': cls.INT64,
            'float32': cls.FLOAT,
            'float64': cls.DOUBLE,
            'bytes': cls.BLOB,
            'object': cls.STRING,
            'string': cls.STRING,
        }

        if dtype_str in dtype_map:
            return dtype_map[dtype_str]

        dtype_lower = dtype_str.lower()
        if dtype_lower in dtype_map:
            return dtype_map[dtype_lower]

        if 'object_' in dtype_lower or dtype_str == "<class 'numpy.object_'>":
            return cls.STRING

        if dtype_str.startswith('datetime64'):
            return cls.TIMESTAMP

        return cls.STRING


_TSDATATYPE_COMPATIBLE_SOURCES = {
    TSDataType.INT64: (TSDataType.INT32, TSDataType.TIMESTAMP),
    TSDataType.STRING: (TSDataType.TEXT,),
    TSDataType.TEXT: (TSDataType.STRING,),
    TSDataType.DOUBLE: (TSDataType.FLOAT,),
    TSDataType.TIMESTAMP: (TSDataType.INT64, TSDataType.INT32)
}


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
    ATTRIBUTE = 2
    TIME = 3
