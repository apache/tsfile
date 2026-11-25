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

# Python DataFrame and Arrow

Use the current Python binding and its tests as the API authority.

## Select the Surface

- Use `to_dataframe` for an eager pandas result, or batched pandas iterator,
  from one Tree- or Table-model file.
- Use `dataframe_to_tsfile` for an inferred Table-model schema and a complete
  pandas-to-TsFile conversion.
- Use `TsFileDataFrame` for a lazy unified numeric view across one or more
  TsFile shards, metadata selection, or timestamp-aligned series access.
- Use Arrow batches for columnar interoperability without Python row loops.
- Use `references/java-tools.md` instead for Arrow file import through the Java
  `arrow2tsfile` command.

TsFile `DATE` maps to Arrow `date32`. Conversion preserves the calendar date
across host time zones and daylight-saving changes.

## pandas Conversion

```python
from tsfile import dataframe_to_tsfile, to_dataframe

frame = to_dataframe(
    "input.tsfile", table_name="sensors", column_names=["device", "temp"],
    start_time=0, end_time=10_000, max_row_num=100_000,
)
dataframe_to_tsfile(
    frame, "output.tsfile", table_name="sensors",
    time_column="time", tag_column=["device"],
)
```

Set `as_iterator=True` on `to_dataframe` to consume pandas batches. The writer
lowercases column names, requires an integer time column when one is supplied,
and otherwise uses a `time` column or the DataFrame index. Validate inferred
object-column types before writing production data.

## Lazy Multi-file Dataset

```python
from tsfile import TsFileDataFrame

with TsFileDataFrame(["part-1.tsfile", "part-2.tsfile"], show_progress=False) as ds:
    metadata = ds.list_timeseries_metadata()
    selected = ds[ds["count"] > 0]
    aligned = selected.loc[0:10_000, [0, 1]]
```

A single dataset must not mix Tree- and Table-model files. The root
`TsFileDataFrame` owns readers; use its context manager and do not expect a
subset view to close shared readers. Boolean dataset selection is positional:
build the mask from `ds[...]`, as above; use `list_timeseries_metadata()` for
named inspection rather than as the mask source.

Table-model files may register only the columns they actually wrote, so the
same table can span files with different column sets. The dataset unions the
FIELD columns in first-seen order and reads a device/field missing from a file
as NaN. TAG columns are positional: files may append trailing tags, and devices
from files without them read back with a null tag, but files must agree on the
tag columns they share — a different name at the same position means the table
name points at a different tag layout and is rejected. A name that is a TAG in
one file and a readable (numeric) FIELD in another is rejected as well;
non-numeric fields are never exposed or indexed, so they do not take part in
that check. FIELD declarations are never compared otherwise. Duplicate
timestamps for one logical series across files remain rejected.

`use_index=True` builds/reuses an index (default `False`). Indexed mode assumes
immutable sources (`trust_index=True`); use `False` for generation checks and
stale-index rebuilds. It also accepts `max_prepared_series`, `descriptor_cache_size`,
`max_open_files`, `query_workers`, and `query_parallel_min_rows`; explicit values
override `TSFILE_DATAFRAME_<UPPERCASE_OPTION>` then defaults. Subsets share limits;
cache limits allow zero, others must be positive. Active queries may pin cache
entries above these entry-count limits. `query_workers=1` serializes dataset
groups, independently of the native pool. See `python/README.md` for details.

## Arrow Batch I/O

```python
with reader.query_table(
    "sensors", ["device", "temp"], batch_size=8192
) as result:
    while True:
        table = result.read_arrow_batch()
        if table is None:
            break
        consume(table)  # pyarrow.Table

with TsFileTableWriter("output.tsfile", schema) as writer:
    writer.write_arrow_batch(record_batch_or_table)
```

Use `batch_size > 0` for Arrow reads. It specifies returned result rows after
filtering and OFFSET/LIMIT: every batch has that many rows except the final
batch at end of results. Batches may cross devices, chunks, and pages. Internal
scan windows and scratch memory are independent of this setting.

C++ table scans build Arrow-layout buffers directly. Python imports these
buffers without a TsBlock conversion; batches retain ownership after the
result set or reader closes. Each result set reuses its first imported PyArrow schema
for subsequent batches. Retaining batches retains their memory. Native
`get_next_tsblock()` preserves the projected table column schemas, including TAG
and explicitly projected TIME columns. A projected INT64 FIELD named `time`
retains its stored values in its own output position after the implicit timestamp.

When parallel reads are enabled, the query schedules device metadata and column
page planning, then whole column read/decompress/decode/write tasks on the
shared pool. Device tasks are fetched in windows as the result is consumed.
A time range can exclude a whole chunk before its pages are read. Aligned scans
use the shared time chunk's range, including timestamps with NULL field values.
Output order is stable. Aligned FIELD predicates first prune by
page statistics, then decode only predicate columns to compute a shared row
selection; output-only columns are decoded only for pages with accepted rows.
Selection is separate from Arrow null validity: exported batches contain only
selected rows. A page decoder resumes across small output batches without
rereading the payload. Each column releases its page payload and decoder after
writing that page's last selected row (or reaching LIMIT), even when the output
batch spans many sparsely selected pages. Only a page that still has rows for a
later batch retains its decoder state. Unknown statistics remain candidates.
Non-aligned sources retain timestamp union semantics and use a bounded merge
window.

Unfiltered scans without OFFSET or LIMIT can reuse compressed input across page
planning and output. A query can retain up to 160 MiB of shared compressed ranges,
including contiguous ranges for wide aligned projections. This budget covers
cached input bytes. Decoders and the returned Arrow buffers use additional memory.
Time and field predicates use task-local read-ahead windows of up to 64 KiB.
Coalescing for filtered output stops at skipped pages and chunk boundaries.

Arrow writes accept a
`pyarrow.RecordBatch` or `pyarrow.Table`; include the registered time column and
match all remaining columns to the Table schema. Use the wrapper writer when
possible so it resolves the time-column index from the schema.

## Source Anchors

- `python/tsfile/utils.py`, `python/tsfile/dataset/`, and the current Python
  reader/writer bindings
- `python/tsfile/tsfile_table_writer.py`
- `python/tests/test_dataframe.py`, `python/tests/test_tsfile_dataset.py`,
  `python/tests/test_dataset_read_options.py`, `python/tests/test_batch_arrow.py`,
  and `python/tests/test_write_arrow.py`
