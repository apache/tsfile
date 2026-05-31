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

"""Per-dataset JSON manifest cache for :class:`MetadataCatalog`.

One JSON file describes the catalogs of every shard the dataframe was
constructed from. The manifest lives in the common parent directory of the
input paths and is keyed by each shard's absolute path. Each shard entry
carries its own ``(size, mtime_ns)`` fingerprint so that a single stale
shard does not invalidate the rest of the manifest.
"""

import json
import os
import tempfile
from typing import Dict, Iterable, List, Optional, Tuple

from ..constants import TSDataType
from .metadata import MetadataCatalog

_MANIFEST_FILENAME = "tsfile_dataset.metacache.json"
_MANIFEST_VERSION = 2


def manifest_path(paths: List[str]) -> str:
    """Return the dataset manifest path for a list of absolute shard paths.

    The manifest sits in the common parent directory of all shards. When the
    paths span unrelated trees (no common parent exists), we fall back to the
    first path's directory so we always have a writable location.
    """
    if not paths:
        raise ValueError("Cannot compute manifest path for empty paths.")
    try:
        common = os.path.commonpath(paths)
    except ValueError:
        common = os.path.dirname(paths[0])
    if os.path.isfile(common):
        common = os.path.dirname(common)
    return os.path.join(common, _MANIFEST_FILENAME)


def catalog_to_dict(catalog: MetadataCatalog) -> dict:
    return {
        "tables": [
            {
                "name": t.table_name,
                "tag_columns": list(t.tag_columns),
                "tag_types": [int(tt) for tt in t.tag_types],
                "field_columns": list(t.field_columns),
            }
            for t in catalog.table_entries
        ],
        "devices": [
            {
                "table_index": d.table_id,
                "tag_values": list(d.tag_values),
                "min_time": d.min_time,
                "max_time": d.max_time,
            }
            for d in catalog.device_entries
        ],
        "series_stats": [
            {
                "device_index": device_id,
                "field_index": field_idx,
                "length": stats["length"],
                "min_time": stats["min_time"],
                "max_time": stats["max_time"],
                "timeline_length": stats["timeline_length"],
                "timeline_min_time": stats["timeline_min_time"],
                "timeline_max_time": stats["timeline_max_time"],
            }
            for (device_id, field_idx), stats in catalog.series_stats_by_ref.items()
        ],
    }


def catalog_from_dict(data: dict) -> MetadataCatalog:
    catalog = MetadataCatalog()
    for table in data["tables"]:
        catalog.add_table(
            table["name"],
            table["tag_columns"],
            [TSDataType(v) for v in table["tag_types"]],
            table["field_columns"],
        )
    for device in data["devices"]:
        catalog.add_device(
            device["table_index"],
            tuple(device["tag_values"]),
            device["min_time"],
            device["max_time"],
        )
    for stats in data["series_stats"]:
        catalog.series_stats_by_ref[(stats["device_index"], stats["field_index"])] = {
            "length": stats["length"],
            "min_time": stats["min_time"],
            "max_time": stats["max_time"],
            "timeline_length": stats["timeline_length"],
            "timeline_min_time": stats["timeline_min_time"],
            "timeline_max_time": stats["timeline_max_time"],
        }
    return catalog


def _shard_entry(file_path: str, catalog: MetadataCatalog) -> Optional[dict]:
    """Build a shard manifest entry by stat'ing the file right before write."""
    try:
        stat = os.stat(file_path)
    except OSError:
        return None
    return {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "catalog": catalog_to_dict(catalog),
    }


def _entry_matches_file(entry: dict, file_path: str) -> bool:
    try:
        stat = os.stat(file_path)
    except OSError:
        return False
    return (
        entry.get("size") == stat.st_size and entry.get("mtime_ns") == stat.st_mtime_ns
    )


def read_manifest(path: str) -> Optional[Dict[str, dict]]:
    """Load and shallow-validate the manifest. Returns the shards dict, or None."""
    try:
        with open(path, "r") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("cache_version") != _MANIFEST_VERSION:
        return None
    shards = data.get("shards")
    if not isinstance(shards, dict):
        return None
    return shards


def extract_catalog(
    shards: Dict[str, dict], file_path: str
) -> Optional[MetadataCatalog]:
    """Return the cached catalog for one shard iff its fingerprint still matches."""
    entry = shards.get(file_path)
    if entry is None or not isinstance(entry, dict):
        return None
    if not _entry_matches_file(entry, file_path):
        return None
    try:
        return catalog_from_dict(entry["catalog"])
    except (KeyError, ValueError, TypeError):
        return None


def write_manifest(
    path: str,
    fresh_catalogs: Iterable[Tuple[str, MetadataCatalog]],
) -> None:
    """Atomically merge fresh catalogs into the manifest at ``path``.

    Reads the existing manifest first (if any), overwrites the entries for
    the shards in ``fresh_catalogs``, and writes the result back via temp
    file + ``os.replace``. Other shards' entries are preserved. OS errors
    (read-only filesystem, no space) are swallowed silently.
    """
    existing = read_manifest(path) or {}
    merged = dict(existing)
    for file_path, catalog in fresh_catalogs:
        entry = _shard_entry(file_path, catalog)
        if entry is not None:
            merged[file_path] = entry

    payload = {"cache_version": _MANIFEST_VERSION, "shards": merged}
    target_dir = os.path.dirname(path) or "."
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=_MANIFEST_FILENAME + ".",
            suffix=".tmp",
            dir=target_dir,
        )
    except OSError:
        return
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh)
        os.replace(tmp_path, path)
    except OSError:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
