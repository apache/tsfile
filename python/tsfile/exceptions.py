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


from enum import IntEnum


class ErrorCode(IntEnum):
    OK = 0
    OOM = 1
    NOT_EXIST = 2
    ALREADY_EXIST = 3
    INVALID_ARGUMENT = 4
    OUT_OF_RANGE = 5
    PARTIAL_READ = 6
    INVALID_SCHEMA = 8
    OVERFLOW = 20
    NO_MORE_DATA = 21
    OUT_OF_ORDER = 22
    DATA_INCONSISTENCY = 24
    TYPE_NOT_SUPPORTED = 26
    TYPE_MISMATCH = 27
    FILE_OPEN_ERROR = 28
    FILE_CLOSE_ERROR = 29
    FILE_WRITE_ERROR = 30
    FILE_READ_ERROR = 31
    FILE_SYNC_ERROR = 32
    WRITER_METADATA_ERROR = 33
    FILE_STAT_ERROR = 34
    TSFILE_CORRUPTED = 35
    BUFFER_NOT_ENOUGH = 36
    INVALID_PATH = 37
    NOT_MATCH = 38
    NOT_SUPPORTED = 40
    INVALID_DATA_POINT = 43
    DEVICE_NOT_EXIST = 44
    MEASUREMENT_NOT_EXIST = 45
    COMPRESSION_ERROR = 48
    TABLE_NOT_EXIST = 49
    COLUMN_NOT_EXIST = 50
    UNSUPPORTED_ORDER = 51
    INVALID_NODE_TYPE = 52
    ENCODE_ERROR = 53
    DECODE_ERROR = 54
    FILE_MAP_ERROR = 55


class LibraryError(Exception):
    _default_message = "Unknown error occurred"
    _default_code = -1

    def __init__(self, code=None, context=None):
        self.code = code if code is not None else self._default_code
        self.message = context if context is not None else self._default_message
        super().__init__(f"[{self.code}] {self.message}")

    def __str__(self):
        return f"{self.code}: {self.message}"


class OOMError(LibraryError):
    _default_message = "Out of memory"
    _default_code = 1


class NotExistsError(LibraryError):
    _default_message = "Requested resource does not exist"
    _default_code = 2


class AlreadyExistsError(LibraryError):
    _default_message = "Resource already exists"
    _default_code = 3


class InvalidArgumentError(LibraryError):
    _default_message = "Invalid argument provided"
    _default_code = 4


class OutOfRangeError(LibraryError):
    _default_message = "Value out of valid range"
    _default_code = 5


class PartialReadError(LibraryError):
    _default_message = "Incomplete data read operation"
    _default_code = 6


class InvalidSchemaError(LibraryError):
    _default_message = "Invalid schema"
    _default_code = 8


class TsFileOverflowError(LibraryError):
    _default_message = "Buffer or value overflow"
    _default_code = 20


class NoMoreDataError(LibraryError):
    _default_message = "No more data"
    _default_code = 21


class OutOfOrderError(LibraryError):
    _default_message = "Data is out of order"
    _default_code = 22


class DataInconsistencyError(LibraryError):
    _default_message = "Data is inconsistent"
    _default_code = 24


class FileOpenError(LibraryError):
    _default_message = "Failed to open file"
    _default_code = 28


class FileCloseError(LibraryError):
    _default_message = "Failed to close file"
    _default_code = 29


class FileWriteError(LibraryError):
    _default_message = "Failed to write to file"
    _default_code = 30


class FileReadError(LibraryError):
    _default_message = "Failed to read from file"
    _default_code = 31


class FileSyncError(LibraryError):
    _default_message = "Failed to sync file contents"
    _default_code = 32


class MetadataError(LibraryError):
    _default_message = "Metadata inconsistency detected"
    _default_code = 33


class FileStatError(LibraryError):
    _default_message = "Failed to inspect file metadata"
    _default_code = 34


class TsFileCorruptedError(LibraryError):
    _default_message = "TsFile is corrupted"
    _default_code = 35


class BufferNotEnoughError(LibraryError):
    _default_message = "Insufficient buffer space"
    _default_code = 36


class InvalidPathError(LibraryError):
    _default_message = "Invalid path"
    _default_code = 37


class NotSupportedError(LibraryError):
    _default_message = "Not support yet"
    _default_code = 40


class DeviceNotExistError(LibraryError):
    _default_message = "Requested device does not exist"
    _default_code = 44


class MeasurementNotExistError(LibraryError):
    _default_message = "Specified measurement does not exist"
    _default_code = 45


class CompressionError(LibraryError):
    _default_message = "Data compression/decompression failed"
    _default_code = 48


class TableNotExistError(LibraryError):
    _default_message = "Requested table does not exist"
    _default_code = 49


class TypeNotSupportedError(LibraryError):
    _default_message = "Unsupported data type"
    _default_code = 26


class TypeMismatchError(LibraryError):
    _default_message = "Data type mismatch"
    _default_code = 27


class ColumnNotExistError(LibraryError):
    _default_message = "Column does not exist"
    _default_code = 50


class UnsupportedOrderError(LibraryError):
    _default_message = "Unsupported ordering"
    _default_code = 51


class InvalidNodeTypeError(LibraryError):
    _default_message = "Invalid node type"
    _default_code = 52


class EncodeError(LibraryError):
    _default_message = "Failed to encode data"
    _default_code = 53


class DecodeError(LibraryError):
    _default_message = "Failed to decode data"
    _default_code = 54


class FileMapError(LibraryError):
    _default_message = "Failed to memory-map file"
    _default_code = 55


ERROR_MAPPING = {
    1: OOMError,
    2: NotExistsError,
    3: AlreadyExistsError,
    4: InvalidArgumentError,
    5: OutOfRangeError,
    6: PartialReadError,
    8: InvalidSchemaError,
    20: TsFileOverflowError,
    21: NoMoreDataError,
    22: OutOfOrderError,
    24: DataInconsistencyError,
    26: TypeNotSupportedError,
    27: TypeMismatchError,
    28: FileOpenError,
    29: FileCloseError,
    30: FileWriteError,
    31: FileReadError,
    32: FileSyncError,
    33: MetadataError,
    34: FileStatError,
    35: TsFileCorruptedError,
    36: BufferNotEnoughError,
    37: InvalidPathError,
    40: NotSupportedError,
    44: DeviceNotExistError,
    45: MeasurementNotExistError,
    48: CompressionError,
    49: TableNotExistError,
    50: ColumnNotExistError,
    51: UnsupportedOrderError,
    52: InvalidNodeTypeError,
    53: EncodeError,
    54: DecodeError,
    55: FileMapError,
}


ERROR_MESSAGES = {
    error.value: error.name.replace("_", " ").lower() for error in ErrorCode
}
ERROR_MESSAGES.update(
    {
        code: exception_type._default_message
        for code, exception_type in ERROR_MAPPING.items()
    }
)


def get_exception(code: int, context: str = None):
    code = int(code)
    if code == ErrorCode.OK:
        return None

    exc_type = ERROR_MAPPING.get(code, LibraryError)
    message = ERROR_MESSAGES.get(code, "Unknown library error")
    if context:
        message = f"{context}: {message}"
    return exc_type(code=code, context=message)
