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

"""On-disk cache for TsFileDataFrame's per-file metadata catalogs.

Building a :class:`~tsfile.dataset.metadata.MetadataCatalog` requires a full
native metadata walk (``get_timeseries_metadata``) whose cost scales with the
number of series -- slow and memory-hungry for datasets with many series that
are reloaded repeatedly (e.g. across training runs). The catalog itself is pure
metadata (strings + ints), so it serializes into a small file.

This module persists the *per-file* catalogs (one ``MetadataCatalog`` per
shard, in load order). On reload the dataframe reopens each file cheaply and
restores its catalog from here, skipping the walk, then replays the same
deterministic cross-file merge (``_register_reader``) to rebuild its global
view identically.

File layout (single file, written atomically)::

    [ 8 bytes ] magic  b"TSFIDX01"
    [ 4 bytes ] uint32 header_len (little-endian)
    [ header_len bytes ] pickled header dict (the string "sidecar")
    [ 4 bytes ] uint32 n_stats_arrays
    repeat n_stats_arrays times:
        [ .npy blob ]  one int64 structured array (np.save framing)

The bulk ``series_stats_by_ref`` (six int64 per series, can be millions of
rows) goes into a compact numpy structured array per catalog; the comparatively
tiny table/device tables go into the pickled header. Derived catalog fields
(``table_id_by_name``, ``device_id_by_key``, ``TableEntry._field_index_by_name``)
are never stored -- they are rebuilt by replaying ``add_table`` / ``add_device``
on load.
"""

import os
import pickle
import struct
from typing import List, Optional, Tuple

import numpy as np

from ..constants import TSDataType
from .metadata import MetadataCatalog, SeriesStats

# Bump when the on-disk layout changes; load_catalogs rejects mismatches so a
# stale cache is transparently rebuilt rather than mis-parsed.
CACHE_VERSION = 1

# Fixed cache file name written at the "dataset top" (the directory passed to
# TsFileDataFrame). The extension is deliberately NOT ".tsfile" so the
# directory walk in dataframe._expand_paths (which collects "*.tsfile") never
# picks the cache up as a data shard. Do NOT rename this to end in ".tsfile".
CACHE_FILENAME = ".tsfile_dataframe_index.tsfidx"

_MAGIC = b"TSFIDX01"

# One row per (device_id, field_idx) series: the two-int key followed by the
# six SeriesStats ints, all int64.
_STATS_DTYPE = np.dtype(
    [
        ("device_id", "<i8"),
        ("field_idx", "<i8"),
        ("length", "<i8"),
        ("min_time", "<i8"),
        ("max_time", "<i8"),
        ("timeline_length", "<i8"),
        ("timeline_min_time", "<i8"),
        ("timeline_max_time", "<i8"),
    ]
)


class IndexCacheError(Exception):
    """Raised when a cache file is unreadable, truncated, or version-mismatched.

    Callers treat this as "cache unusable" and fall back to a fresh build.
    """


def resolve_cache_path(paths_arg) -> Optional[str]:
    """Resolve where the fixed-name index cache lives for this ``paths`` input.

    The cache is only enabled when ``paths`` is a single directory string -- the
    unambiguous "dataset top". Single-file and list inputs return ``None`` (no
    caching; behavior is unchanged for those callers), because there is no
    single stable directory to key the cache to.

    ``paths_arg`` must be the ORIGINAL argument passed to
    ``TsFileDataFrame.__init__`` (before path expansion), since the rule keys
    off whether the user passed one directory.
    """
    if isinstance(paths_arg, str) and os.path.isdir(paths_arg):
        return os.path.join(os.path.abspath(paths_arg), CACHE_FILENAME)
    return None


def _catalog_to_header(catalog: MetadataCatalog) -> dict:
    """Serialize a catalog's string tables (everything except bulk stats)."""
    tables = [
        {
            "name": entry.table_name,
            "tag_columns": list(entry.tag_columns),
            "tag_types": [int(dtype) for dtype in entry.tag_types],
            "field_columns": list(entry.field_columns),
        }
        for entry in catalog.table_entries
    ]
    devices = [
        {
            "table_id": entry.table_id,
            "tag_values": list(entry.tag_values),
            "min_time": entry.min_time,
            "max_time": entry.max_time,
        }
        for entry in catalog.device_entries
    ]
    return {"tables": tables, "devices": devices}


def _catalog_to_stats_array(catalog: MetadataCatalog) -> np.ndarray:
    """Pack ``series_stats_by_ref`` into a structured array in insertion order.

    Insertion order is preserved because ``_register_reader`` and
    ``iter_series_paths`` iterate ``series_stats_by_ref`` to assign the global
    series order; the load path re-inserts rows in the same order.
    """
    items = catalog.series_stats_by_ref
    array = np.empty(len(items), dtype=_STATS_DTYPE)
    for row, ((device_id, field_idx), stats) in zip(array, items.items()):
        row["device_id"] = device_id
        row["field_idx"] = field_idx
        row["length"] = stats.length
        row["min_time"] = stats.min_time
        row["max_time"] = stats.max_time
        row["timeline_length"] = stats.timeline_length
        row["timeline_min_time"] = stats.timeline_min_time
        row["timeline_max_time"] = stats.timeline_max_time
    return array


def save_catalogs(
    cache_path: str, file_paths: List[str], catalogs: List[MetadataCatalog]
) -> None:
    """Persist per-file catalogs to ``cache_path`` atomically.

    ``file_paths`` and ``catalogs`` are parallel lists in dataframe load order;
    the reload replays them in this order so the merged view is rebuilt
    identically. Writes a temp file in the same directory then ``os.replace`` so
    a concurrent reader never sees a torn file.
    """
    header = {
        "version": CACHE_VERSION,
        "file_paths": list(file_paths),
        "catalogs": [_catalog_to_header(catalog) for catalog in catalogs],
    }
    stats_arrays = [_catalog_to_stats_array(catalog) for catalog in catalogs]

    header_bytes = pickle.dumps(header, protocol=pickle.HIGHEST_PROTOCOL)
    tmp_path = f"{cache_path}.tmp.{os.getpid()}"
    with open(tmp_path, "wb") as fh:
        fh.write(_MAGIC)
        fh.write(struct.pack("<I", len(header_bytes)))
        fh.write(header_bytes)
        fh.write(struct.pack("<I", len(stats_arrays)))
        for array in stats_arrays:
            # allow_pickle=False: arrays are pure int64, no object dtype.
            np.save(fh, array, allow_pickle=False)
    os.replace(tmp_path, cache_path)


def _header_to_catalog(
    header_catalog: dict, stats_array: np.ndarray
) -> MetadataCatalog:
    """Rebuild a catalog by replaying the public mutators (rebuilds derived state)."""
    catalog = MetadataCatalog()
    for table in header_catalog["tables"]:
        catalog.add_table(
            table["name"],
            table["tag_columns"],
            [TSDataType(value) for value in table["tag_types"]],
            table["field_columns"],
        )
    for device in header_catalog["devices"]:
        catalog.add_device(
            device["table_id"],
            tuple(device["tag_values"]),
            device["min_time"],
            device["max_time"],
        )
    # Set stats directly (no public adder), preserving the stored row order.
    for row in stats_array:
        catalog.series_stats_by_ref[(int(row["device_id"]), int(row["field_idx"]))] = (
            SeriesStats(
                length=int(row["length"]),
                min_time=int(row["min_time"]),
                max_time=int(row["max_time"]),
                timeline_length=int(row["timeline_length"]),
                timeline_min_time=int(row["timeline_min_time"]),
                timeline_max_time=int(row["timeline_max_time"]),
            )
        )
    return catalog


def load_catalogs(cache_path: str) -> Tuple[List[str], List[MetadataCatalog]]:
    """Load per-file paths and catalogs from ``cache_path``.

    Raises :class:`IndexCacheError` on a bad magic, version mismatch, or any
    structural corruption so the caller can fall back to a fresh build.
    """
    try:
        with open(cache_path, "rb") as fh:
            magic = fh.read(len(_MAGIC))
            if magic != _MAGIC:
                raise IndexCacheError(f"Bad index cache magic: {magic!r}")
            (header_len,) = struct.unpack("<I", fh.read(4))
            header = pickle.loads(fh.read(header_len))
            if header.get("version") != CACHE_VERSION:
                raise IndexCacheError(
                    f"Index cache version {header.get('version')} != {CACHE_VERSION}"
                )
            (n_arrays,) = struct.unpack("<I", fh.read(4))
            stats_arrays = [np.load(fh, allow_pickle=False) for _ in range(n_arrays)]
    except IndexCacheError:
        raise
    except Exception as exc:  # truncation, unpickle failure, bad struct, ...
        raise IndexCacheError(f"Failed to read index cache: {exc}") from exc

    header_catalogs = header["catalogs"]
    if len(header_catalogs) != len(stats_arrays):
        raise IndexCacheError(
            f"Index cache catalog/stats count mismatch: "
            f"{len(header_catalogs)} vs {len(stats_arrays)}"
        )

    catalogs = [
        _header_to_catalog(header_catalog, stats_array)
        for header_catalog, stats_array in zip(header_catalogs, stats_arrays)
    ]
    return list(header["file_paths"]), catalogs
