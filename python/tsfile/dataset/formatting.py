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

"""String formatting helpers for dataset objects."""

from datetime import datetime
from typing import Dict, List, Optional

import numpy as np


def format_timestamp(ts_ms: int) -> str:
    """Convert millisecond timestamp to human-readable string."""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000)
        if ts_ms % 1000 == 0:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except (OSError, ValueError, TypeError):
        return str(ts_ms)


def format_aligned_timeseries(
    timestamps: np.ndarray,
    values: np.ndarray,
    series_names: List[str],
    max_rows: Optional[int],
) -> str:
    """Render a table-like string for aligned query results.

    Uses head/tail truncation with '...' when rows exceed *max_rows*,
    consistent with format_dataframe_table.
    """
    n_rows, n_cols = values.shape
    if n_rows == 0:
        return f"AlignedTimeseries(0 rows, {n_cols} series)"

    # Determine which rows to render (head + tail when truncated)
    if max_rows is not None and n_rows > max_rows:
        head = max_rows // 2
        tail = max_rows - head
        show_indices = list(range(head)) + list(range(n_rows - tail, n_rows))
        truncated = True
    else:
        show_indices = list(range(n_rows))
        truncated = False

    # Only format the rows we will actually display
    ts_strs = {i: format_timestamp(int(timestamps[i])) for i in show_indices}
    ts_width = max(max((len(s) for s in ts_strs.values()), default=0), len("time"))

    col_widths = []
    rendered_values = []  # list of dicts: row_idx -> cell string
    for col_idx in range(n_cols):
        col_name = series_names[col_idx] if col_idx < len(series_names) else f"col_{col_idx}"
        width = len(col_name)
        column = {}
        for row_idx in show_indices:
            value = values[row_idx, col_idx]
            cell = "NaN" if np.isnan(value) else f"{value:.2f}"
            column[row_idx] = cell
            width = max(width, len(cell))
        rendered_values.append(column)
        col_widths.append(width)

    header = ["time".rjust(ts_width)]
    for col_idx, width in enumerate(col_widths):
        col_name = series_names[col_idx] if col_idx < len(series_names) else f"col_{col_idx}"
        header.append(col_name.rjust(width))
    lines = ["  ".join(header)]

    head_count = max_rows // 2 if truncated else len(show_indices)
    for i, row_idx in enumerate(show_indices):
        if truncated and i == head_count:
            lines.append("...")
        parts = [ts_strs[row_idx].rjust(ts_width)]
        for col_idx, width in enumerate(col_widths):
            parts.append(rendered_values[col_idx][row_idx].rjust(width))
        lines.append("  ".join(parts))

    return f"AlignedTimeseries({n_rows} rows, {n_cols} series)\n" + "\n".join(lines)


def format_dataframe_table(
    series_list: List[str],
    merged_info: Dict[str, dict],
    tag_columns: List[str],
    indices: Optional[List[int]] = None,
    max_rows: int = 20,
) -> str:
    """Render the metadata table used by TsFileDataFrame.__repr__."""
    if indices is None:
        indices = list(range(len(series_list)))
    else:
        indices = list(indices)

    total = len(indices)
    if total > max_rows:
        show_indices = list(indices[: max_rows // 2]) + list(indices[-max_rows // 2 :])
        truncated = True
    else:
        show_indices = indices
        truncated = False

    rows = []
    for idx in show_indices:
        name = series_list[idx]
        info = merged_info[name]
        row = {
            "index": idx,
            "table": info["table_name"],
            "field": info["field"],
            "start_time": format_timestamp(info["min_time"]),
            "end_time": format_timestamp(info["max_time"]),
            "count": info["count"],
        }
        for tag_col in tag_columns:
            row[tag_col] = info["tag_values"].get(tag_col, "")
        rows.append(row)

    if not rows:
        return "Empty TsFileDataFrame"

    headers = ["", "table"] + tag_columns + ["field", "start_time", "end_time", "count"]
    widths = {header: len(header) for header in headers}
    widths[""] = max(len(str(row["index"])) for row in rows)

    for row in rows:
        widths[""] = max(widths[""], len(str(row["index"])))
        widths["table"] = max(widths["table"], len(row["table"]))
        widths["field"] = max(widths["field"], len(row["field"]))
        widths["start_time"] = max(widths["start_time"], len(row["start_time"]))
        widths["end_time"] = max(widths["end_time"], len(row["end_time"]))
        widths["count"] = max(widths["count"], len(str(row["count"])))
        for tag_col in tag_columns:
            widths[tag_col] = max(widths[tag_col], len(str(row[tag_col])))

    lines = ["  ".join(header.rjust(widths[header]) for header in headers)]
    split = len(rows) // 2 if truncated else len(rows)
    for row_idx, row in enumerate(rows):
        if truncated and row_idx == split:
            lines.append("...")
        parts = [str(row["index"]).rjust(widths[""]), row["table"].rjust(widths["table"])]
        for tag_col in tag_columns:
            parts.append(str(row[tag_col]).rjust(widths[tag_col]))
        parts.extend(
            [
                row["field"].rjust(widths["field"]),
                row["start_time"].rjust(widths["start_time"]),
                row["end_time"].rjust(widths["end_time"]),
                str(row["count"]).rjust(widths["count"]),
            ]
        )
        lines.append("  ".join(parts))

    return "\n".join(lines)
