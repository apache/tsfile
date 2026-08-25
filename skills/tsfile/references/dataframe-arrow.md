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

Use `batch_size > 0` for Arrow reads. Arrow writes accept a
`pyarrow.RecordBatch` or `pyarrow.Table`; include the registered time column and
match all remaining columns to the Table schema. Use the wrapper writer when
possible so it resolves the time-column index from the schema.

## Source Anchors

- `python/tsfile/utils.py`, `python/tsfile/dataset/`, and the current Python
  reader/writer bindings
- `python/tsfile/tsfile_table_writer.py`
- `python/tests/test_dataframe.py`, `python/tests/test_tsfile_dataset.py`,
  `python/tests/test_batch_arrow.py`, and `python/tests/test_write_arrow.py`
