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

# cython: boundscheck=False, wraparound=False, cdivision=True, language_level=3

"""Cython accessors for the existing read-only Dataset Index mmap.

This module deliberately owns no index-sized data structures.  It receives the
already-mapped byte view and section directory from ``MappedDatasetIndex`` and
only removes Python tuple/bytes allocation from the binary-search loops.
"""

from libc.stdint cimport int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from cpython.unicode cimport PyUnicode_AsUTF8AndSize, PyUnicode_DecodeUTF8


cdef inline uint32_t _read_u32(
        const uint8_t[:] view, uint64_t offset) noexcept nogil:
    return (
        <uint32_t>view[offset]
        | (<uint32_t>view[offset + 1] << 8)
        | (<uint32_t>view[offset + 2] << 16)
        | (<uint32_t>view[offset + 3] << 24)
    )


cdef inline uint16_t _read_u16(
        const uint8_t[:] view, uint64_t offset) noexcept nogil:
    return (
        <uint16_t>view[offset]
        | (<uint16_t>view[offset + 1] << 8)
    )


cdef inline uint64_t _read_u64(
        const uint8_t[:] view, uint64_t offset) noexcept nogil:
    return (
        <uint64_t>view[offset]
        | (<uint64_t>view[offset + 1] << 8)
        | (<uint64_t>view[offset + 2] << 16)
        | (<uint64_t>view[offset + 3] << 24)
        | (<uint64_t>view[offset + 4] << 32)
        | (<uint64_t>view[offset + 5] << 40)
        | (<uint64_t>view[offset + 6] << 48)
        | (<uint64_t>view[offset + 7] << 56)
    )


cdef inline uint64_t _fnv1a(
        const char* data, Py_ssize_t length) noexcept nogil:
    cdef uint64_t result = 1469598103934665603
    cdef uint64_t prime = 1099511628211
    cdef Py_ssize_t index
    for index in range(length):
        result ^= <uint8_t>data[index]
        result = result * prime
    return result


cdef inline bint _string_equals(
        const uint8_t[:] view,
        uint64_t offsets_base,
        uint64_t strings_base,
        uint32_t string_count,
        uint32_t string_id,
        const char* data,
        Py_ssize_t length,
) noexcept nogil:
    cdef uint32_t start
    cdef uint32_t end
    cdef Py_ssize_t index
    if string_id + 1 >= string_count:
        return False
    start = _read_u32(view, offsets_base + <uint64_t>string_id * 4)
    end = _read_u32(view, offsets_base + <uint64_t>(string_id + 1) * 4)
    if end - start != length:
        return False
    for index in range(length):
        if view[strings_base + start + index] != <uint8_t>data[index]:
            return False
    return True


cdef inline int64_t _find_child(
        const uint8_t[:] view,
        uint64_t section_base,
        uint32_t record_size,
        uint32_t first,
        uint32_t count,
        uint32_t table_id,
        uint64_t target_hash,
        const char* data,
        Py_ssize_t length,
        uint64_t string_offsets_base,
        uint64_t string_bytes_base,
        uint32_t string_count,
) noexcept nogil:
    cdef uint32_t low = first
    cdef uint32_t high = first + count
    cdef uint32_t middle
    cdef uint64_t row_base
    cdef uint64_t row_hash
    cdef uint32_t row_table_id
    cdef uint32_t row_id
    cdef uint32_t row_string_id

    while low < high:
        middle = low + (high - low) // 2
        row_base = section_base + <uint64_t>middle * record_size
        row_hash = _read_u64(view, row_base + 8)
        if row_hash < target_hash:
            low = middle + 1
        else:
            high = middle

    high = first + count
    while low < high:
        row_base = section_base + <uint64_t>low * record_size
        if _read_u64(view, row_base + 8) != target_hash:
            break
        row_table_id = _read_u32(view, row_base)
        row_string_id = _read_u32(view, row_base + 16)
        if row_table_id == table_id and _string_equals(
                view,
                string_offsets_base,
                string_bytes_base,
                string_count,
                row_string_id,
                data,
                length,
        ):
            row_id = _read_u32(view, row_base + 4)
            return row_id
        low += 1
    return -1


cdef const char* _utf8_name(object name, Py_ssize_t* length):
    cdef const char* data
    if not isinstance(name, str):
        raise TypeError("Dataset Index names must be str")
    data = PyUnicode_AsUTF8AndSize(name, length)
    if data == NULL:
        raise UnicodeEncodeError("utf-8", name, 0, len(name), "invalid UTF-8")
    return data


cdef class IndexLookup:
    """Typed lookup kernel over a ``MappedDatasetIndex`` memoryview."""

    cdef const uint8_t[:] _view
    cdef uint64_t _section_offsets[14]
    cdef uint32_t _section_sizes[14]
    cdef uint32_t _section_counts[14]

    def __cinit__(self, object view, object entries):
        cdef int section
        self._view = view
        for section in range(1, 14):
            self._section_offsets[section] = entries[section][2]
            self._section_sizes[section] = entries[section][1]
            self._section_counts[section] = entries[section][4]

    def find_device_id(self, uint32_t table_id, object name):
        cdef const char* data
        cdef Py_ssize_t length
        cdef const uint8_t[:] view = self._view
        cdef uint64_t table_base
        cdef uint32_t first
        cdef uint32_t count
        cdef uint64_t target_hash
        cdef int64_t result
        data = _utf8_name(name, &length)
        if table_id >= self._section_counts[4]:
            raise IndexError(table_id)
        table_base = self._section_offsets[4] + <uint64_t>table_id * self._section_sizes[4]
        first = _read_u32(view, table_base + 8)
        count = _read_u32(view, table_base + 12)
        target_hash = _fnv1a(data, length)
        with nogil:
            result = _find_child(
                view,
                self._section_offsets[5],
                self._section_sizes[5],
                first,
                count,
                table_id,
                target_hash,
                data,
                length,
                self._section_offsets[1],
                self._section_offsets[2],
                self._section_counts[1],
            )
        if result < 0:
            raise KeyError(name)
        return result

    def find_column_id(self, uint32_t table_id, object name):
        cdef const char* data
        cdef Py_ssize_t length
        cdef const uint8_t[:] view = self._view
        cdef uint64_t table_base
        cdef uint32_t first
        cdef uint32_t count
        cdef uint64_t target_hash
        cdef int64_t result
        data = _utf8_name(name, &length)
        if table_id >= self._section_counts[4]:
            raise IndexError(table_id)
        table_base = self._section_offsets[4] + <uint64_t>table_id * self._section_sizes[4]
        first = _read_u32(view, table_base + 16)
        count = _read_u32(view, table_base + 20)
        target_hash = _fnv1a(data, length)
        with nogil:
            result = _find_child(
                view,
                self._section_offsets[7],
                self._section_sizes[7],
                first,
                count,
                table_id,
                target_hash,
                data,
                length,
                self._section_offsets[1],
                self._section_offsets[2],
                self._section_counts[1],
            )
        if result < 0:
            raise KeyError(name)
        return result

    def find_series_id(self, uint32_t device_id, uint32_t column_id):
        cdef const uint8_t[:] view = self._view
        cdef uint64_t device_base
        cdef uint32_t first
        cdef uint32_t count
        cdef uint32_t low
        cdef uint32_t high
        cdef uint32_t middle
        cdef uint64_t row_base
        cdef uint32_t row_column_id
        cdef uint32_t row_device_id
        cdef int64_t result = -1
        if device_id >= self._section_counts[6]:
            raise IndexError(device_id)
        device_base = self._section_offsets[6] + <uint64_t>device_id * self._section_sizes[6]
        first = _read_u32(view, device_base + 16)
        count = _read_u32(view, device_base + 20)
        low = first
        high = first + count
        with nogil:
            while low < high:
                middle = low + (high - low) // 2
                row_base = self._section_offsets[9] + <uint64_t>middle * self._section_sizes[9]
                row_column_id = _read_u32(view, row_base + 4)
                if row_column_id < column_id:
                    low = middle + 1
                else:
                    high = middle
            if low < first + count:
                row_base = self._section_offsets[9] + <uint64_t>low * self._section_sizes[9]
                row_device_id = _read_u32(view, row_base)
                row_column_id = _read_u32(view, row_base + 4)
                if row_device_id == device_id and row_column_id == column_id:
                    result = low
        if result < 0:
            raise KeyError(column_id)
        return result

    def describe_series(self, uint32_t series_id):
        """Expand one logical series without creating intermediate records."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t series_base
        cdef uint32_t device_id
        cdef uint32_t column_id
        cdef uint32_t first_span
        cdef uint32_t span_count
        cdef uint32_t index
        cdef uint32_t span_id
        cdef uint64_t span_base
        cdef uint32_t file_id
        cdef uint32_t locator_id
        cdef uint32_t device_span_id
        cdef uint64_t locator_base
        cdef uint64_t device_span_base
        cdef uint64_t span_length
        cdef uint64_t timeline_length
        cdef int64_t min_time
        cdef int64_t max_time
        cdef int64_t span_min_time
        cdef int64_t span_max_time
        cdef uint64_t count = 0
        cdef list shards = []

        if series_id >= self._section_counts[9]:
            raise IndexError(series_id)

        series_base = (
            self._section_offsets[9]
            + <uint64_t>series_id * self._section_sizes[9]
        )
        device_id = _read_u32(view, series_base)
        column_id = _read_u32(view, series_base + 4)
        first_span = _read_u32(view, series_base + 8)
        span_count = _read_u32(view, series_base + 12)
        min_time = <int64_t>_read_u64(view, series_base + 16)
        max_time = <int64_t>_read_u64(view, series_base + 24)

        if <uint64_t>first_span + span_count > self._section_counts[12]:
            raise IndexError(series_id)

        for index in range(span_count):
            span_id = first_span + index
            span_base = (
                self._section_offsets[12]
                + <uint64_t>span_id * self._section_sizes[12]
            )
            file_id = _read_u32(view, span_base + 4)
            locator_id = _read_u32(view, span_base + 8)
            span_min_time = <int64_t>_read_u64(view, span_base + 16)
            span_max_time = <int64_t>_read_u64(view, span_base + 24)
            span_length = _read_u64(view, span_base + 32)

            if locator_id >= self._section_counts[13]:
                raise IndexError(locator_id)
            locator_base = (
                self._section_offsets[13]
                + <uint64_t>locator_id * self._section_sizes[13]
            )
            device_span_id = _read_u32(view, locator_base)
            if device_span_id >= self._section_counts[11]:
                raise IndexError(device_span_id)
            device_span_base = (
                self._section_offsets[11]
                + <uint64_t>device_span_id * self._section_sizes[11]
            )
            if _read_u16(view, device_span_base + 20) == 1:
                timeline_length = _read_u64(view, device_span_base + 24)
            else:
                timeline_length = span_length
            count += timeline_length
            shards.append(
                (file_id, locator_id, timeline_length, span_min_time, span_max_time)
            )

        return device_id, column_id, min_time, max_time, count, shards

    def series_identity(self, uint32_t series_id):
        """Return device and column ids for one logical series."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t base
        if series_id >= self._section_counts[9]:
            raise IndexError(series_id)
        base = self._section_offsets[9] + <uint64_t>series_id * self._section_sizes[9]
        return _read_u32(view, base), _read_u32(view, base + 4)

    def find_series_span(self, uint32_t series_id, uint32_t file_id):
        """Find one series span and return locator/time/length scalars."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t series_base
        cdef uint32_t first_span
        cdef uint32_t span_count
        cdef uint32_t index
        cdef uint32_t span_id
        cdef uint64_t span_base

        if series_id >= self._section_counts[9]:
            raise IndexError(series_id)
        series_base = (
            self._section_offsets[9]
            + <uint64_t>series_id * self._section_sizes[9]
        )
        first_span = _read_u32(view, series_base + 8)
        span_count = _read_u32(view, series_base + 12)
        if <uint64_t>first_span + span_count > self._section_counts[12]:
            raise IndexError(series_id)
        for index in range(span_count):
            span_id = first_span + index
            span_base = (
                self._section_offsets[12]
                + <uint64_t>span_id * self._section_sizes[12]
            )
            if _read_u32(view, span_base + 4) == file_id:
                return (
                    _read_u32(view, span_base + 8),
                    <int64_t>_read_u64(view, span_base + 16),
                    <int64_t>_read_u64(view, span_base + 24),
                    _read_u64(view, span_base + 32),
                )
        raise KeyError((series_id, file_id))

    def locator_metadata(self, uint32_t locator_id):
        """Return locator and owning device-span scalars."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t locator_base
        cdef uint32_t device_span_id
        cdef uint64_t device_span_base

        if locator_id >= self._section_counts[13]:
            raise IndexError(locator_id)
        locator_base = (
            self._section_offsets[13]
            + <uint64_t>locator_id * self._section_sizes[13]
        )
        device_span_id = _read_u32(view, locator_base)
        if device_span_id >= self._section_counts[11]:
            raise IndexError(device_span_id)
        device_span_base = (
            self._section_offsets[11]
            + <uint64_t>device_span_id * self._section_sizes[11]
        )
        return (
            device_span_id,
            _read_u16(view, locator_base + 4),
            _read_u32(view, device_span_base + 4),
            _read_u16(view, device_span_base + 20),
            _read_u64(view, device_span_base + 24),
        )

    def prepared_locator_metadata(self, uint32_t file_id, uint32_t locator_id):
        """Return the generation and locator fields used by native prepare."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t locator_base
        cdef uint64_t device_span_base
        cdef uint64_t file_base
        cdef uint32_t device_span_id
        cdef uint32_t device_file_id

        if locator_id >= self._section_counts[13]:
            raise IndexError(locator_id)
        if file_id >= self._section_counts[10]:
            raise IndexError(file_id)
        locator_base = (
            self._section_offsets[13]
            + <uint64_t>locator_id * self._section_sizes[13]
        )
        device_span_id = _read_u32(view, locator_base)
        if device_span_id >= self._section_counts[11]:
            raise IndexError(device_span_id)
        device_span_base = (
            self._section_offsets[11]
            + <uint64_t>device_span_id * self._section_sizes[11]
        )
        device_file_id = _read_u32(view, device_span_base + 4)
        if device_file_id != file_id:
            raise ValueError("series locator points at another TsFile")
        file_base = (
            self._section_offsets[10]
            + <uint64_t>file_id * self._section_sizes[10]
        )
        return (
            _read_u64(view, file_base + 8),
            _read_u64(view, file_base + 16),
            _read_u16(view, locator_base + 4),
            _read_u16(view, locator_base + 6),
            _read_u64(view, locator_base + 8),
            _read_u32(view, locator_base + 16),
            _read_u64(view, device_span_base + 8),
            _read_u32(view, device_span_base + 16),
        )

    def device_route(self, uint32_t device_id):
        """Return table id and logical-path string id for one device."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t base
        if device_id >= self._section_counts[6]:
            raise IndexError(device_id)
        base = self._section_offsets[6] + <uint64_t>device_id * self._section_sizes[6]
        return _read_u32(view, base), _read_u32(view, base + 4)

    def table_name_id(self, uint32_t table_id):
        """Return the string-pool id for one table name."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t base
        if table_id >= self._section_counts[4]:
            raise IndexError(table_id)
        base = self._section_offsets[4] + <uint64_t>table_id * self._section_sizes[4]
        return _read_u32(view, base)

    def column_name_id(self, uint32_t column_id):
        """Return the string-pool id for one column name."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t base
        if column_id >= self._section_counts[8]:
            raise IndexError(column_id)
        base = self._section_offsets[8] + <uint64_t>column_id * self._section_sizes[8]
        return _read_u32(view, base + 4)

    def device_time_bounds(self, uint32_t device_id):
        """Return min/max timestamps for one device."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t base
        if device_id >= self._section_counts[6]:
            raise IndexError(device_id)
        base = self._section_offsets[6] + <uint64_t>device_id * self._section_sizes[6]
        return (
            <int64_t>_read_u64(view, base + 32),
            <int64_t>_read_u64(view, base + 40),
        )

    def string(self, uint32_t string_id):
        """Decode one string pool entry without a temporary bytes slice."""
        cdef const uint8_t[:] view = self._view
        cdef uint64_t offsets_base = self._section_offsets[1]
        cdef uint64_t strings_base = self._section_offsets[2]
        cdef uint32_t string_count = self._section_counts[1]
        cdef uint32_t start
        cdef uint32_t end
        cdef const char* data
        if string_id + 1 >= string_count:
            raise IndexError(string_id)
        start = _read_u32(view, offsets_base + <uint64_t>string_id * 4)
        end = _read_u32(view, offsets_base + <uint64_t>(string_id + 1) * 4)
        data = <const char*>&view[strings_base + start]
        return PyUnicode_DecodeUTF8(data, end - start, "strict")
