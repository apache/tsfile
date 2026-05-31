<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# `TsFileDataFrame` Sidecar Metadata Cache

`TsFileDataFrame` opens one C++ reader per shard during construction. Opening
the reader itself is cheap (one `open(2)` syscall); the expensive part is
materializing each shard's `MetadataCatalog` — reading the TsFile footer,
walking the index tree, deserializing every device's `TimeseriesIndex`, and
converting the result into Python objects. Repeating that work on every
process start is wasteful when the underlying `.tsfile` files have not
changed.

To skip that work, every shard gets a JSON sidecar file next to it that
records the catalog produced by the previous load. On subsequent loads the
sidecar is validated against the live file's `stat()` fingerprint and, if it
still matches, the catalog is rehydrated directly from JSON. The C++ reader
is still opened — that cost is negligible — but `get_all_table_schemas` and
`get_timeseries_metadata(None)` are skipped entirely.

## File layout

The sidecar lives next to the shard and reuses its full name plus a fixed
suffix:

```
data/2024-01-01.tsfile
data/2024-01-01.tsfile.metacache.json
```

Defined in `cache.py`:

| Constant                 | Value                |
|--------------------------|----------------------|
| `_CACHE_SUFFIX`          | `".metacache.json"`  |
| `_CACHE_FORMAT_VERSION`  | `1`                  |

One shard, one sidecar. There is no cross-shard manifest: each sidecar is
independent and can be deleted or rebuilt in isolation.

## JSON schema

The on-disk payload is a single JSON object:

```json
{
  "cache_version": 1,
  "file": {
    "size": 1234567,
    "mtime_ns": 1717050000000000000
  },
  "catalog": {
    "tables": [ ... ],
    "devices": [ ... ],
    "series_stats": [ ... ]
  }
}
```

### Top-level fields

| Field           | Type   | Purpose                                                                              |
|-----------------|--------|--------------------------------------------------------------------------------------|
| `cache_version` | int    | Sidecar schema version. Mismatch → invalidate.                                       |
| `file.size`     | int    | `os.stat(tsfile).st_size` at the time the sidecar was written. Mismatch → invalidate.|
| `file.mtime_ns` | int    | `os.stat(tsfile).st_mtime_ns` at the time the sidecar was written. Mismatch → invalidate. |
| `catalog`       | object | Serialized form of one shard's `MetadataCatalog`.                                    |

### `catalog.tables[]`

Each entry mirrors a `TableEntry`:

| Field           | Type     | Notes                                                  |
|-----------------|----------|--------------------------------------------------------|
| `name`          | string   | Table name.                                            |
| `tag_columns`   | string[] | Ordered tag column names.                              |
| `tag_types`     | int[]    | `TSDataType` integer values, parallel to `tag_columns`.|
| `field_columns` | string[] | Ordered numeric field column names.                    |

The position of a table in this array is its `table_index`, referenced by
`devices[].table_index` below.

### `catalog.devices[]`

Each entry mirrors a `DeviceEntry`:

| Field         | Type    | Notes                                                                                        |
|---------------|---------|----------------------------------------------------------------------------------------------|
| `table_index` | int     | Index into `catalog.tables[]`.                                                               |
| `tag_values`  | any[]   | Tag values in the order of the table's `tag_columns`. Types follow the table's `tag_types`.  |
| `min_time`    | int     | Minimum timestamp observed across the device's contributing measurements.                    |
| `max_time`    | int     | Maximum timestamp observed across the device's contributing measurements.                    |

Tag values are stored using their natural JSON representation:

| `TSDataType`            | JSON form |
|-------------------------|-----------|
| `BOOLEAN`               | bool      |
| `INT32`, `INT64`, `TIMESTAMP`, `DATE` | int |
| `FLOAT`, `DOUBLE`       | number    |
| `TEXT`, `STRING`, `BLOB`| string    |
| (missing trailing tag)  | `null`    |

The position of a device in this array is its `device_index`, referenced by
`series_stats[].device_index` below.

### `catalog.series_stats[]`

Each entry mirrors one `(device_id, field_idx) → stats` entry in
`MetadataCatalog.series_stats_by_ref`:

| Field                | Type         | Notes                                                                              |
|----------------------|--------------|------------------------------------------------------------------------------------|
| `device_index`       | int          | Index into `catalog.devices[]`.                                                    |
| `field_index`        | int          | Index into the device's table's `field_columns`.                                   |
| `length`             | int          | Row count derived from the value column's statistic.                               |
| `min_time`           | int \| null  | Minimum timestamp from the value column statistic. `null` when no statistic.       |
| `max_time`           | int \| null  | Maximum timestamp from the value column statistic. `null` when no statistic.       |
| `timeline_length`    | int          | Row count from the device's shared timeline statistic. Used for display + reads.   |
| `timeline_min_time`  | int \| null  | Minimum timestamp from the device's shared timeline statistic.                     |
| `timeline_max_time`  | int \| null  | Maximum timestamp from the device's shared timeline statistic.                     |

For series with no data, every stat field is `0` or `null` — the slot is
present so that `(device_index, field_index)` resolution always succeeds.

## Validation rules

When `cache_from_dict` and `read_sidecar` decide whether to trust a sidecar,
the checks below run in order. Any failure returns `None` and the dataframe
falls back to a full native metadata load.

1. The shard's `os.stat()` succeeds.
2. The sidecar exists and parses as JSON.
3. The top-level value is an object.
4. `cache_version` equals `_CACHE_FORMAT_VERSION` (currently `1`).
5. `file.size` equals the shard's current `st_size`.
6. `file.mtime_ns` equals the shard's current `st_mtime_ns`.
7. `catalog_from_dict` succeeds on `catalog` (well-formed tables, devices,
   and stats).

## Write semantics

`write_sidecar` writes through a temporary file in the same directory and
calls `os.replace` for an atomic swap. The `size` and `mtime_ns` recorded in
the payload come from a fresh `os.stat()` taken immediately before writing.

All disk-level errors (permission denied, read-only filesystem, no space) are
swallowed silently; the dataframe is still constructed successfully, the
sidecar is simply absent and the next load pays the full metadata cost.

## Cache modes

`TsFileDataFrame(paths, cache=...)` accepts:

| Mode      | Read sidecar? | Write sidecar after load? |
|-----------|---------------|---------------------------|
| `"auto"` (default) | yes  | yes, only when the read missed |
| `"off"`            | no   | no                              |
| `"rebuild"`        | no   | yes, always (overwrites any existing sidecar) |

`"rebuild"` exists for the case where the sidecar is suspected of being
stale or corrupt despite passing the size/mtime checks — for example after a
manual edit or an interrupted previous write.

## Worked example

The smallest possible sidecar — one table, one device, one field, three
rows:

```json
{
  "cache_version": 1,
  "file": {
    "size": 4096,
    "mtime_ns": 1717050000000000000
  },
  "catalog": {
    "tables": [
      {
        "name": "weather",
        "tag_columns": ["device"],
        "tag_types": [11],
        "field_columns": ["temperature"]
      }
    ],
    "devices": [
      {
        "table_index": 0,
        "tag_values": ["device_a"],
        "min_time": 0,
        "max_time": 2
      }
    ],
    "series_stats": [
      {
        "device_index": 0,
        "field_index": 0,
        "length": 3,
        "min_time": 0,
        "max_time": 2,
        "timeline_length": 3,
        "timeline_min_time": 0,
        "timeline_max_time": 2
      }
    ]
  }
}
```

`11` is the integer value of `TSDataType.STRING`.

## Operational notes

- The sidecar is **not** a portable export. It encodes positional indices
  into the same shard's footer ordering. Moving a sidecar between hosts is
  fine as long as the corresponding shard file moves with it; copying it to
  a different shard is not supported.
- Cleaning up sidecars is safe at any time. The next load will pay the
  metadata cost once and rewrite them.
- Bumping `_CACHE_FORMAT_VERSION` is the migration path when the schema
  needs to change. All older sidecars become invalidated automatically by
  the version check.
