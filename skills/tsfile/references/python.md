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

# Python SDK

Use this reference only for Python binding work. The Python package depends on
the C++ module in this source line; resolve the package version from
`python/pyproject.toml`.

## Table-Model Write

```python
from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    Tablet,
    TSDataType,
    TsFileTableWriter,
)

columns = [
    ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
    ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
]
schema = TableSchema("sensors", columns)
tablet = Tablet(
    schema.get_column_names(),
    [column.get_data_type() for column in schema.get_columns()],
    2,
)
tablet.add_timestamp(0, 1)
tablet.add_value_by_name("device", 0, "d1")
tablet.add_value_by_name("temperature", 0, 20.5)

with TsFileTableWriter("data.tsfile", schema) as writer:
    writer.write_table(tablet)
```

`ColumnSchema` exposes `get_column_name()` and `get_data_type()`; do not assume
it has a public `name` attribute.

## Table-Model Read

```python
from tsfile import TsFileReader

with TsFileReader("data.tsfile") as reader:
    schemas = reader.get_all_table_schemas()
    schema = next(iter(schemas.values())) if isinstance(schemas, dict) else schemas[0]
    table_name = schema.get_table_name()
    with reader.query_table_by_row(
        table_name, ["device", "temperature"], limit=10
    ) as result:
        frame = result.read_data_frame(max_row_num=10)
```

## Guidance

- Writers accept `allow_non_decreasing_timestamps=False`; this is process-wide.
  See `advanced-apis.md` for opt-in semantics.
- For persistent dataset indexes and bounded caches, see `dataframe-arrow.md`.
- Parallel controls: `set_parallel_read_enabled`, `set_parallel_write_enabled`,
  `set_thread_count` and matching `get_*` functions. Flags default to `True`,
  pool size to 6; counts outside `1..64` raise `InvalidArgumentError`.
  Configure before operations and never resize an in-use pool. Equivalent
  config keys: `parallel_read_enabled_`, `parallel_write_enabled_`, `thread_count_`.
- `TsFileReader` also accepts binary objects with `seek`, `tell`, and `read(size)`
  (e.g. fsspec streams); it retains the object, preserves its cursor, and does
  not close it. The caller owns the source. File objects are served through a
  block cache with sequential read-ahead (`read_cache_enabled_`,
  `read_cache_block_size_`, `read_cache_capacity_`,
  `read_cache_max_readahead_blocks_`; defaults on/256 KiB/256 MiB/8), so a
  remote scan issues roughly `file_size / block_size` requests. Local paths are
  not cached this way.
- `TsFileReader("s3://bucket/key", s3={...})` reads object storage natively
  over signed HTTP range requests when the build has libcurl
  (`s3_available()`); `oss://` works through the same S3-compatible API.
  `s3` keys: `endpoint`, `region`, `access_key_id`, `secret_access_key`,
  `session_token`, `path_style` (default True), `connect_timeout_ms`,
  `request_timeout_ms`, `max_retries`, `prefetch_threads` (default 4, 0 =
  synchronous only). Environment fallbacks: TSFILE_S3_* / AWS_*; empty
  credentials mean anonymous access. Prefetch workers fetch contiguous block
  groups in parallel during scans.
- For local paths, `set_file_read_backend(FileReadBackend.MMAP)`
  affects subsequently opened files; PREAD is default. See `advanced-apis.md`
  for backend semantics. File objects do not use this setting.
- Current bindings expose `query_table` and `query_table_by_row`; do not reuse
  older `get_table_names` or `read_table` examples without checking the target
  version.
- Repeated batch `query_table` calls whose tag filter selects exactly one device
  reuse prepared series through a per-reader LRU
  (`TSFILE_QUERY_CACHE_SIZE`, default `1024`, `0` disables). TAG-column
  selections, row-by-row mode (`batch_size <= 0`) and filters matching zero or
  several devices keep the regular path, as do projections of 16 or more field
  columns. For explicit control,
  `prepare_table_series(table, column, tag_filter)` resolves one column and
  returns a handle for `query_prepared`; `query_prepared_multi([...])` serves
  several aligned columns. Both accept `block_rows` to bound each returned
  TsBlock (it does not limit rows returned).
- Schema collections may be lists or mappings depending on the binding layer;
  normalize them before reusable iteration.
- Use context managers for readers, writers, and results.
- Use `python/examples/example.py` and `python/tests/` as executable API
  documentation.
- Verify from the repository root with
  `./mvnw -P with-python clean verify`.
