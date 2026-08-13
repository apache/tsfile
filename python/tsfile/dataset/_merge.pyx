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
        cnp.intp_t[:] cursors) noexcept:
    cdef int64_t left_time = times[starts[left] + cursors[left]]
    cdef int64_t right_time = times[starts[right] + cursors[right]]
    return left_time < right_time or (left_time == right_time and left < right)


cdef void _push(int[:] heap, int* heap_size, int part,
                int64_t[:] times, cnp.intp_t[:] starts,
                cnp.intp_t[:] cursors) noexcept:
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
              cnp.intp_t[:] starts, cnp.intp_t[:] cursors) noexcept:
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
    for part in range(len(time_parts)):
        if lengths[part] > 0:
            _push(heap, &heap_size, part, flat, starts, cursors)
    while heap_size > 0:
        part = _pop(heap, &heap_size, flat, starts, cursors)
        timestamp = flat[starts[part] + cursors[part]]
        if has_last and timestamp == last_timestamp:
            if validate_unique:
                raise ValueError(f"Duplicate timestamp {timestamp} found across shards.")
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
    for part in range(len(time_parts)):
        if lengths[part] > 0:
            _push(heap, &heap_size, part, flat_times, starts, cursors)
    while heap_size > 0:
        part = _pop(heap, &heap_size, flat_times, starts, cursors)
        source = starts[part] + cursors[part]
        output_times[out_index] = flat_times[source]
        output_values[out_index] = flat_values[source]
        out_index += 1
        cursors[part] += 1
        if cursors[part] < lengths[part]:
            _push(heap, &heap_size, part, flat_times, starts, cursors)
    return output_times_array, output_values_array
