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

"""Metadata assembly helpers for dataset loading."""

from typing import Dict, List, Tuple

import numpy as np


def register_reader(readers: Dict[str, object], series_map: Dict[str, list], file_path: str, reader) -> None:
    """Register a reader and append its series entries to the catalog."""
    readers[file_path] = reader
    for series_path in reader.series_paths:
        series_map.setdefault(series_path, []).append((reader, reader.series_info[series_path]))


def build_merged_entry(series_path: str, entries: list) -> Tuple[np.ndarray, dict]:
    """Build merged timestamps and structural metadata for one series."""
    _, first_info = entries[0]

    if len(entries) == 1:
        reader, info = entries[0]
        merged_timestamps = reader._timestamps_cache[series_path]
        merged_info = {
            "table_name": info["table_name"],
            "tag_columns": info["tag_columns"],
            "tag_values": info["tag_values"],
            "field": info["column_name"],
            "min_time": info["min_time"],
            "max_time": info["max_time"],
            "count": info["length"],
        }
        return merged_timestamps, merged_info

    merged_timestamps = np.unique(
        np.concatenate([reader._timestamps_cache[series_path] for reader, _ in entries])
    )
    merged_info = {
        "table_name": first_info["table_name"],
        "tag_columns": first_info["tag_columns"],
        "tag_values": first_info["tag_values"],
        "field": first_info["column_name"],
        "min_time": int(merged_timestamps[0]),
        "max_time": int(merged_timestamps[-1]),
        "count": len(merged_timestamps),
    }
    return merged_timestamps, merged_info


def collect_tag_columns(series_list: List[str], merged_info: Dict[str, dict]) -> List[str]:
    """Return all unique tag columns in first-seen order."""
    seen = {}
    for name in series_list:
        for column in merged_info[name]["tag_columns"]:
            seen.setdefault(column, True)
    return list(seen.keys())
