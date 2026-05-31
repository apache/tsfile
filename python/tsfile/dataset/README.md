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

# `TsFileDataFrame` Dataset Metadata Manifest

`TsFileDataFrame` opens one C++ reader per shard during construction. Opening
the reader itself is cheap (one `open(2)` syscall); the expensive part is
materializing each shard's `MetadataCatalog` — reading the TsFile footer,
walking the index tree, deserializing every device's `TimeseriesIndex`, and
converting the result into Python objects. Repeating that work on every
process start is wasteful when the underlying `.tsfile` files have not
changed.

To skip that work, every dataset gets **one** JSON manifest file that
records the catalog of every shard the dataframe has seen. On subsequent
loads the manifest is consulted per shard: entries whose `(size, mtime_ns)`
still match are rehydrated directly from JSON; entries that miss (modified,
removed, or never recorded) trigger the normal native metadata load and are
then merged back into the manifest.

The C++ reader is still opened for every shard — that cost is negligible —
but `get_all_table_schemas` and `get_timeseries_metadata(None)` are skipped
on cache hits.

## File layout

One manifest covers an entire dataset. It sits in the common parent
directory of all the shard paths passed to `TsFileDataFrame`:

```
data/
├── part_0.tsfile
├── part_1.tsfile
├── part_2.tsfile
├── part_3.tsfile
└── tsfile_dataset.metacache.json   ← the manifest
```

When the shards span multiple subdirectories, the manifest lives in their
deepest common ancestor. When the inputs share no common path (rare cross-
filesystem case), the manifest falls back to the first shard's directory so
there is always a writable target.

Defined in `cache.py`:

| Constant              | Value                                |
|-----------------------|--------------------------------------|
| `_MANIFEST_FILENAME`  | `"tsfile_dataset.metacache.json"`    |
| `_MANIFEST_VERSION`   | `2`                                  |

## JSON schema

The on-disk payload is a single JSON object:

```json
{
  "cache_version": 2,
  "shards": {
    "/abs/path/data/part_0.tsfile": {
      "size": 1234567,
      "mtime_ns": 1717050000000000000,
      "catalog": { "tables": [...], "devices": [...], "series_stats": [...] }
    },
    "/abs/path/data/part_1.tsfile": { ... }
  }
}
```

### Top-level fields

| Field           | Type                | Purpose                                                        |
|-----------------|---------------------|----------------------------------------------------------------|
| `cache_version` | int                 | Manifest schema version. Mismatch → invalidate the entire manifest. |
| `shards`        | object              | Map from each shard's absolute path to its cache entry.        |

### `shards[<abs_path>]`

Each shard entry is independent. A stale fingerprint on one shard only
invalidates that shard; the other shards still hit.

| Field       | Type   | Notes                                                                                          |
|-------------|--------|------------------------------------------------------------------------------------------------|
| `size`      | int    | `os.stat(shard).st_size` at the time the entry was written. Mismatch → that shard misses.      |
| `mtime_ns`  | int    | `os.stat(shard).st_mtime_ns` at the time the entry was written. Mismatch → that shard misses.  |
| `catalog`   | object | Serialized form of the shard's `MetadataCatalog`. See below.                                   |

### `shards[<abs_path>].catalog.tables[]`

Each entry mirrors a `TableEntry`:

| Field           | Type     | Notes                                                  |
|-----------------|----------|--------------------------------------------------------|
| `name`          | string   | Table name.                                            |
| `tag_columns`   | string[] | Ordered tag column names.                              |
| `tag_types`     | int[]    | `TSDataType` integer values, parallel to `tag_columns`.|
| `field_columns` | string[] | Ordered numeric field column names.                    |

The position of a table in this array is its `table_index`, referenced by
`devices[].table_index` below.

### `shards[<abs_path>].catalog.devices[]`

Each entry mirrors a `DeviceEntry`:

| Field         | Type    | Notes                                                                                        |
|---------------|---------|----------------------------------------------------------------------------------------------|
| `table_index` | int     | Index into the same shard's `catalog.tables[]`.                                              |
| `tag_values`  | any[]   | Tag values in the order of the table's `tag_columns`. Types follow the table's `tag_types`.  |
| `min_time`    | int     | Minimum timestamp observed across the device's contributing measurements.                    |
| `max_time`    | int     | Maximum timestamp observed across the device's contributing measurements.                    |

Tag values are stored using their natural JSON representation:

| `TSDataType`                        | JSON form |
|-------------------------------------|-----------|
| `BOOLEAN`                           | bool      |
| `INT32`, `INT64`, `TIMESTAMP`, `DATE` | int     |
| `FLOAT`, `DOUBLE`                   | number    |
| `TEXT`, `STRING`, `BLOB`            | string    |
| (missing trailing tag)              | `null`    |

The position of a device in this array is its `device_index`, referenced by
`series_stats[].device_index` below.

### `shards[<abs_path>].catalog.series_stats[]`

Each entry mirrors one `(device_id, field_idx) → stats` entry in
`MetadataCatalog.series_stats_by_ref`:

| Field                | Type         | Notes                                                                              |
|----------------------|--------------|------------------------------------------------------------------------------------|
| `device_index`       | int          | Index into the same shard's `catalog.devices[]`.                                   |
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

Validation happens at two levels.

**Manifest-level**, by `read_manifest`:

1. The manifest file exists and parses as JSON.
2. The top-level value is an object.
3. `cache_version` equals `_MANIFEST_VERSION` (currently `2`).
4. `shards` is an object.

A manifest-level failure throws away the entire cache; every shard becomes
a miss and the manifest is rewritten from scratch.

**Per-shard**, by `extract_catalog`:

1. The shard has an entry in `shards`.
2. `os.stat(shard)` succeeds.
3. The entry's `size` equals the live `st_size`.
4. The entry's `mtime_ns` equals the live `st_mtime_ns`.
5. `catalog_from_dict` succeeds on the entry's `catalog`.

A per-shard failure only invalidates that one shard; other shards in the
same manifest continue to hit.

## Write semantics

After a load, `write_manifest` is called only if at least one shard missed
(or the user passed `cache="rebuild"`). It does:

1. Re-read the existing manifest from disk so concurrent writers' new
   entries are not lost.
2. Overwrite the entries for the shards we just loaded with fresh
   `(size, mtime_ns, catalog)` triples.
3. Write through a temporary file in the same directory and `os.replace`
   atomically.

Entries for shards not in the current dataframe are preserved verbatim:
loading a subset of a dataset does not prune the others.

All disk-level errors (permission denied, read-only filesystem, no space)
are swallowed silently; the dataframe is still constructed successfully,
the manifest is simply not updated.

When every shard hits, the manifest is **not** rewritten — pure-hit loads
do not touch the filesystem.

## Cache modes

`TsFileDataFrame(paths, cache=...)` accepts:

| Mode      | Read manifest? | Write manifest after load? |
|-----------|----------------|-----------------------------|
| `"auto"` (default) | yes  | yes, only if some shard missed    |
| `"off"`            | no   | no                                 |
| `"rebuild"`        | no   | yes — every shard counts as a miss |

`"rebuild"` exists for the case where the manifest is suspected of being
stale or corrupt despite passing the size/mtime checks — for example after
a manual edit or an interrupted previous write.

## File descriptor budget (`max_open_files`)

Every shard's `TsFileSeriesReader` opens a C++ reader eagerly during
construction — failures surface immediately rather than at the first
query. With many shards this could exhaust the process's fd budget
(`ulimit -n`), so the dataframe wraps the live readers in an LRU pool.

`TsFileDataFrame(paths, max_open_files=N)`:

| Behavior                                  | When `N >= len(paths)` | When `N < len(paths)` |
|-------------------------------------------|------------------------|------------------------|
| Native C++ reader opened at construction? | every shard, all stay  | every shard, but only N stay live |
| Catalog kept in memory?                   | every shard            | every shard            |
| Eviction trigger                          | never                  | each new touch pushes the LRU out |
| Reopen on eviction                        | n/a                    | first read on the evicted shard calls `_ensure_open()` |

Default is `1024`. Tune up when a single query routinely touches more
shards than that — every miss costs one `open(2)` plus a footer reload
on the C++ side, so a too-small pool causes thrashing. Tune down when
the host has a tight fd cap and you accept the reopen cost.

Eviction only closes the native handle. The Python wrapper and its
catalog (from the manifest or from native metadata) remain valid, so
nothing about query semantics changes — the next data access on the
evicted shard reopens transparently.

## Concurrency

Multiple processes loading the same dataset concurrently both end up
writing the manifest. Writes are atomic (temp file + `os.replace`) and
content is keyed by absolute shard path, so:

- If they cover the same shards: last writer wins; the manifest is
  identical either way.
- If they cover different overlapping shards: each writer re-reads the
  current manifest before merging, so neither side wipes the other's
  contributions.

A pathological race (both writers see an empty manifest, then both write
their disjoint subsets) can drop the earlier writer's entries. The next
load reproduces them. No explicit lock is taken.

## Worked example

The smallest possible manifest — one shard, one table, one device, one
field, three rows:

```json
{
  "cache_version": 2,
  "shards": {
    "/abs/path/data/weather.tsfile": {
      "size": 4096,
      "mtime_ns": 1717050000000000000,
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
  }
}
```

`11` is the integer value of `TSDataType.STRING`.

## Operational notes

- The manifest is **not** a portable export. Shard keys are absolute
  paths, and the inner `catalog` encodes positional indices into the
  same shard's footer. Moving the dataset to a different absolute path
  invalidates every entry; the next load rebuilds them.
- Deleting the manifest is safe at any time. The next load pays the full
  metadata cost once and rewrites it.
- Bumping `_MANIFEST_VERSION` is the migration path when the schema needs
  to change. All older manifests are discarded automatically by the
  version check.
