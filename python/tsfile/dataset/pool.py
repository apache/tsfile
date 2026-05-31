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

"""LRU pool of live C++ TsFile readers.

Per-shard ``TsFileSeriesReader`` instances always carry their materialized
catalog, but the underlying C++ ``TsFileReaderPy`` (one fd + footer state)
is expensive enough that we want to bound how many stay live at once.

The pool tracks readers in MRU order. When ``touch`` would push the size
above ``capacity``, the least-recently-used reader's native handle is
closed via ``_close_native()``; its Python wrapper and catalog stay valid
and the next read on that shard will reopen lazily through
``_ensure_open()``.

This is single-threaded by design — the dataframe's read paths are
expected to be called from one thread at a time.
"""

from collections import OrderedDict


class _FdPool:
    def __init__(self, capacity: int):
        if capacity < 1:
            raise ValueError(f"max_open_files must be >= 1, got {capacity}")
        self._capacity = capacity
        self._order: "OrderedDict[str, object]" = OrderedDict()

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._order)

    def touch(self, reader) -> None:
        """Mark ``reader`` as most-recently-used. Evict LRU if over capacity."""
        key = reader.file_path
        if key in self._order:
            self._order.move_to_end(key)
            return
        self._order[key] = reader
        while len(self._order) > self._capacity:
            evict_key, victim = next(iter(self._order.items()))
            if victim is reader:
                # Never evict the reader we just inserted.
                break
            self._order.pop(evict_key, None)
            victim._close_native()

    def discard(self, reader) -> None:
        """Remove ``reader`` from the pool without closing anything."""
        self._order.pop(reader.file_path, None)

    def close_all(self) -> None:
        """Close native handles for every reader still in the pool."""
        for reader in list(self._order.values()):
            reader._close_native()
        self._order.clear()
