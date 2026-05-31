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

"""Per-shard JSON sidecar cache for :class:`MetadataCatalog`.

The dataframe's expensive init step is reading TsFile footer + materializing
every device's TimeseriesIndex into Python objects. Opening the C++ reader
itself is just one ``open(2)``. So we cache the materialized catalog next to
each .tsfile and skip the metadata path on subsequent loads.
"""

import json
import os
import tempfile
from typing import Optional

from ..constants import TSDataType
from .metadata import MetadataCatalog

_CACHE_SUFFIX = ".metacache.json"
_CACHE_FORMAT_VERSION = 1


def cache_path(tsfile_path: str) -> str:
    return tsfile_path + _CACHE_SUFFIX


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


def read_sidecar(tsfile_path: str) -> Optional[MetadataCatalog]:
    """Read and validate the sidecar cache for one shard.

    Returns ``None`` for any of: missing/unreadable cache file, schema
    mismatch, size/mtime mismatch, or corrupt JSON.
    """
    try:
        tsfile_stat = os.stat(tsfile_path)
    except OSError:
        return None
    try:
        with open(cache_path(tsfile_path), "r") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("cache_version") != _CACHE_FORMAT_VERSION:
        return None
    file_meta = data.get("file") or {}
    if file_meta.get("size") != tsfile_stat.st_size:
        return None
    if file_meta.get("mtime_ns") != tsfile_stat.st_mtime_ns:
        return None
    try:
        return catalog_from_dict(data["catalog"])
    except (KeyError, ValueError, TypeError):
        return None


def write_sidecar(tsfile_path: str, catalog: MetadataCatalog) -> None:
    """Atomically write the sidecar cache. Disk errors are swallowed."""
    try:
        tsfile_stat = os.stat(tsfile_path)
    except OSError:
        return
    payload = {
        "cache_version": _CACHE_FORMAT_VERSION,
        "file": {
            "size": tsfile_stat.st_size,
            "mtime_ns": tsfile_stat.st_mtime_ns,
        },
        "catalog": catalog_to_dict(catalog),
    }
    target = cache_path(tsfile_path)
    target_dir = os.path.dirname(target) or "."
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=os.path.basename(target) + ".",
            suffix=".tmp",
            dir=target_dir,
        )
    except OSError:
        return
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh)
        os.replace(tmp_path, target)
    except OSError:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def remove_sidecar(tsfile_path: str) -> None:
    try:
        os.unlink(cache_path(tsfile_path))
    except OSError:
        pass
