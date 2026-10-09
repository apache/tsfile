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

"""Narrow typed kernels for overlapping cross-shard merges."""

import numpy as np
cimport numpy as cnp
from libc.stdint cimport int64_t

cnp.import_array()


cdef inline bint _less(
        int left, int right, int64_t[:] times, cnp.intp_t[:] starts,
        cnp.intp_t[:] cursors) noexcept nogil:
    cdef int64_t left_time = times[starts[left] + cursors[left]]
    cdef int64_t right_time = times[starts[right] + cursors[right]]
    return left_time < right_time or (left_time == right_time and left < right)


cdef void _push(int[:] heap, int* heap_size, int part,
                int64_t[:] times, cnp.intp_t[:] starts,
                cnp.intp_t[:] cursors) noexcept nogil:
    cdef int pos = heap_size[0]
    cdef int parent
    heap_size[0] += 1
    while pos > 0:
        parent = (pos - 1) // 2
        if not _less(part, heap[parent], times, starts, cursors):
            break
        heap[pos] = heap[parent]
        pos = parent
    heap[pos] = part


cdef int _pop(int[:] heap, int* heap_size, int64_t[:] times,
              cnp.intp_t[:] starts, cnp.intp_t[:] cursors) noexcept nogil:
    cdef int result = heap[0]
    cdef int last
    cdef int pos = 0
    cdef int child
    heap_size[0] -= 1
    if heap_size[0] == 0:
        return result
    last = heap[heap_size[0]]
    while True:
        child = pos * 2 + 1
        if child >= heap_size[0]:
            break
        if child + 1 < heap_size[0] and _less(
                heap[child + 1], heap[child], times, starts, cursors):
            child += 1
        if not _less(heap[child], last, times, starts, cursors):
            break
        heap[pos] = heap[child]
        pos = child
    heap[pos] = last
    return result


cdef tuple _flatten(list time_parts):
    cdef Py_ssize_t count = len(time_parts)
    cdef cnp.ndarray[cnp.intp_t, ndim=1] starts = np.empty(count, dtype=np.intp)
    cdef cnp.ndarray[cnp.intp_t, ndim=1] lengths = np.empty(count, dtype=np.intp)
    cdef Py_ssize_t index
    cdef Py_ssize_t total = 0
    for index in range(count):
        starts[index] = total
        lengths[index] = len(time_parts[index])
        total += lengths[index]
    return np.concatenate(time_parts).astype(np.int64, copy=False), starts, lengths, total


cpdef merge_timestamp_parts_overlap(list time_parts, bint deduplicate,
                                    bint validate_unique):
    cdef object flat_object
    cdef cnp.ndarray[cnp.intp_t, ndim=1] starts_array
    cdef cnp.ndarray[cnp.intp_t, ndim=1] lengths_array
    cdef Py_ssize_t total
    flat_object, starts_array, lengths_array, total = _flatten(time_parts)
    cdef cnp.ndarray[cnp.int64_t, ndim=1] flat_array = flat_object
    cdef int64_t[:] flat = flat_array
    cdef cnp.intp_t[:] starts = starts_array
    cdef cnp.intp_t[:] lengths = lengths_array
    cdef cnp.ndarray[cnp.intp_t, ndim=1] cursors_array = np.zeros(len(time_parts), dtype=np.intp)
    cdef cnp.intp_t[:] cursors = cursors_array
    cdef cnp.ndarray[cnp.int32_t, ndim=1] heap_array = np.empty(len(time_parts), dtype=np.int32)
    cdef int[:] heap = heap_array
    cdef cnp.ndarray[cnp.int64_t, ndim=1] output_array = np.empty(total, dtype=np.int64)
    cdef int64_t[:] output = output_array
    cdef int heap_size = 0
    cdef int part
    cdef Py_ssize_t out_size = 0
    cdef int64_t timestamp
    cdef int64_t last_timestamp = 0
    cdef bint has_last = False
    cdef bint duplicate_found = False
    cdef int64_t duplicate_timestamp = 0
    cdef int part_count = len(time_parts)
    with nogil:
        for part in range(part_count):
            if lengths[part] > 0:
                _push(heap, &heap_size, part, flat, starts, cursors)
        while heap_size > 0:
            part = _pop(heap, &heap_size, flat, starts, cursors)
            timestamp = flat[starts[part] + cursors[part]]
            if has_last and timestamp == last_timestamp:
                if validate_unique:
                    duplicate_found = True
                    duplicate_timestamp = timestamp
                    break
                if not deduplicate:
                    output[out_size] = timestamp
                    out_size += 1
            else:
                output[out_size] = timestamp
                out_size += 1
                last_timestamp = timestamp
                has_last = True
            cursors[part] += 1
            if cursors[part] < lengths[part]:
                _push(heap, &heap_size, part, flat, starts, cursors)
    if duplicate_found:
        raise ValueError(
            f"Duplicate timestamp {duplicate_timestamp} found across shards."
        )
    return output_array[:out_size]


cpdef merge_time_value_parts_overlap(list time_parts, list value_parts):
    cdef object flat_object
    cdef cnp.ndarray[cnp.intp_t, ndim=1] starts_array
    cdef cnp.ndarray[cnp.intp_t, ndim=1] lengths_array
    cdef Py_ssize_t total
    flat_object, starts_array, lengths_array, total = _flatten(time_parts)
    cdef cnp.ndarray[cnp.int64_t, ndim=1] flat_times_array = flat_object
    cdef cnp.ndarray[cnp.float64_t, ndim=1] flat_values_array = np.concatenate(value_parts).astype(np.float64, copy=False)
    cdef int64_t[:] flat_times = flat_times_array
    cdef double[:] flat_values = flat_values_array
    cdef cnp.intp_t[:] starts = starts_array
    cdef cnp.intp_t[:] lengths = lengths_array
    cdef cnp.ndarray[cnp.intp_t, ndim=1] cursors_array = np.zeros(len(time_parts), dtype=np.intp)
    cdef cnp.intp_t[:] cursors = cursors_array
    cdef cnp.ndarray[cnp.int32_t, ndim=1] heap_array = np.empty(len(time_parts), dtype=np.int32)
    cdef int[:] heap = heap_array
    cdef cnp.ndarray[cnp.int64_t, ndim=1] output_times_array = np.empty(total, dtype=np.int64)
    cdef cnp.ndarray[cnp.float64_t, ndim=1] output_values_array = np.empty(total, dtype=np.float64)
    cdef int64_t[:] output_times = output_times_array
    cdef double[:] output_values = output_values_array
    cdef int heap_size = 0
    cdef int part
    cdef Py_ssize_t source
    cdef Py_ssize_t out_index = 0
    cdef int part_count = len(time_parts)
    cdef int64_t last_timestamp = 0
    cdef int64_t duplicate_timestamp = 0
    cdef bint has_last = False
    cdef bint duplicate_found = False
    with nogil:
        for part in range(part_count):
            if lengths[part] > 0:
                _push(heap, &heap_size, part, flat_times, starts, cursors)
        while heap_size > 0:
            part = _pop(heap, &heap_size, flat_times, starts, cursors)
            source = starts[part] + cursors[part]
            if has_last and flat_times[source] == last_timestamp:
                duplicate_found = True
                duplicate_timestamp = flat_times[source]
                break
            output_times[out_index] = flat_times[source]
            output_values[out_index] = flat_values[source]
            last_timestamp = flat_times[source]
            has_last = True
            out_index += 1
            cursors[part] += 1
            if cursors[part] < lengths[part]:
                _push(heap, &heap_size, part, flat_times, starts, cursors)
    if duplicate_found:
        raise ValueError(
            f"Duplicate timestamp {duplicate_timestamp} found across shards."
        )
    return output_times_array, output_values_array


cpdef scatter_timeline_columns(object union_timestamps, object source_timestamps,
                               list value_arrays, list column_indices,
                               object output_values):
    cdef cnp.ndarray[cnp.int64_t, ndim=1] union_array = np.ascontiguousarray(
        union_timestamps, dtype=np.int64
    )
    cdef cnp.ndarray[cnp.int64_t, ndim=1] source_array = np.ascontiguousarray(
        source_timestamps, dtype=np.int64
    )
    cdef cnp.ndarray[cnp.float64_t, ndim=2] output_array = output_values
    cdef cnp.ndarray[cnp.intp_t, ndim=1] positions_array = np.empty(
        len(source_array), dtype=np.intp
    )
    cdef const int64_t[:] union_view = union_array
    cdef const int64_t[:] source_view = source_array
    cdef cnp.intp_t[:] positions = positions_array
    cdef double[:, :] output = output_array
    cdef cnp.ndarray[cnp.float64_t, ndim=1] values_array
    cdef const double[:] values
    cdef Py_ssize_t row
    cdef Py_ssize_t low
    cdef Py_ssize_t high
    cdef Py_ssize_t middle
    cdef Py_ssize_t source_count = len(source_array)
    cdef Py_ssize_t union_count = len(union_array)
    cdef Py_ssize_t column_position
    cdef int column_index
    cdef bint missing = False

    with nogil:
        for row in range(source_count):
            low = 0
            high = union_count
            while low < high:
                middle = low + (high - low) // 2
                if union_view[middle] < source_view[row]:
                    low = middle + 1
                else:
                    high = middle
            if low == union_count or union_view[low] != source_view[row]:
                missing = True
                break
            positions[row] = low
    if missing:
        raise ValueError("source timestamp is absent from the aligned union")
    if len(value_arrays) != len(column_indices):
        raise ValueError("value arrays and column indices have different lengths")

    for column_position in range(len(value_arrays)):
        values_array = np.ascontiguousarray(
            value_arrays[column_position], dtype=np.float64
        )
        if len(values_array) != source_count:
            raise ValueError("value array length does not match its timeline")
        values = values_array
        column_index = int(column_indices[column_position])
        if column_index < 0 or column_index >= output.shape[1]:
            raise IndexError(column_index)
        with nogil:
            for row in range(source_count):
                output[positions[row], column_index] = values[row]
