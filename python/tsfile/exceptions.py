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

class LibraryError(Exception):
    _default_message = "Unknown error occurred"
    _default_code = -1

    def __init__(self, code=None, context=None):
        self.code = code if code is not None else self._default_code
        self.message = context if context is not None else self._default_message
        super().__init__(f"[{code}] {self.message}")
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

class BufferNotEnoughError(LibraryError):
    _default_message = "Insufficient buffer space"
    _default_code = 36

class NotSupportedError(LibraryError):
    _default_message = "Not support yet"
    _default_code = 40

class DeviceNotExistError(LibraryError):
    _default_message = "Requested device does not exist"
    _default_code = 44

class MeasurementNotExistError(LibraryError):
    _default_message = "Specified measurement does not exist"
    _default_code = 45

class InvalidQueryError(LibraryError):
    _default_message = "Malformed query syntax"
    _default_code = 46

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

ERROR_MAPPING = {
    1: OOMError,
    2: NotExistsError,
    3: AlreadyExistsError,
    4: InvalidArgumentError,
    5: OutOfRangeError,
    6: PartialReadError,
    26: TypeNotSupportedError,
    27: TypeMismatchError,
    28: FileOpenError,
    29: FileCloseError,
    30: FileWriteError,
    31: FileReadError,
    32: FileSyncError,
    33: MetadataError,
    36: BufferNotEnoughError,
    40: NotSupportedError,
    44: DeviceNotExistError,
    45: MeasurementNotExistError,
    46: InvalidQueryError,
    48: CompressionError,
    49: TableNotExistError,
}

def get_exception(code : int, context : str = None):
    if code == 0:
        return None

    exc_type = ERROR_MAPPING.get(code)
    if not exc_type:
        return LibraryError(code=code, context=f"Unmapped error code: {code}, message: {context}")
    return exc_type(code=code, context=context)
