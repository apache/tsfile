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
# TsFileDataFrame

`TsFileDataFrame` lets you read the numeric measurements inside one or more TsFiles the
same way you would work with a pandas DataFrame — without having to care about
the underlying file format or data-loading details. It is part of the TsFile Python
package (`pip install tsfile`).

## Quick start

```python
from tsfile import TsFileDataFrame

df = TsFileDataFrame("table_data/")           # load every .tsfile under the directory
print(df)                                     # browse all series (metadata only)

ts = df["weather.Beijing.humidity"]           # pick one series (lazy handle)
window = ts[20:100]                           # slice by row index -> np.ndarray

data = df.loc[start:end, [                     # align multiple series on timestamps
    "weather.Beijing.temperature",
    "weather.Beijing.humidity",
]]
data.values                                   # -> np.ndarray, shape (N, 2): N timestamps × 2 series
```

## Core types

`TsFileDataFrame` is built around three types:

- **`TsFileDataFrame`** — the entry point. It loads one or more TsFiles and
  exposes a unified view. Construction only scans metadata; **no values are read**.
- **`Timeseries`** — a lazy handle to a single series, obtained from `df[...]`.
  It carries the series' metadata but reads nothing until you index it by row.
- **`AlignedTimeseries`** — the result of aligning several series on a common
  time axis, obtained from `df.loc[...]`. It reads the requested range into
  memory at once: the aligned timestamp array (`.timestamps`, length **N**) and a
  value matrix (`.values`, shape **(N, M)**) — **N** timestamps (rows) × **M**
  selected series (columns).

### TsFileDataFrame

In the table below, `df` is a `TsFileDataFrame` instance, created with
`df = TsFileDataFrame(paths)`.

| Example | Operation | Returns |
|---|---|---|
| `TsFileDataFrame(paths)` | Load a file / list of files / directory | `TsFileDataFrame` |
| `len(df)` | Number of time series | `int` |
| `df.list_timeseries("weather")` | Series names, optionally filtered by prefix | `List[str]` |
| `df["weather.Beijing.humidity"]`, `df[0]`, `df[-1]` | One series | `Timeseries` |
| `df["city"]` | A metadata column (a tag / `field` / `start_time` / `end_time` / `count`) | `pandas.Series` |
| `df[0:3]`, `df[[0, 2, 5]]` | Subset view by integer position: a contiguous range (`0:3`), or the listed positions (`[0, 2, 5]`); positions are the printed `index` column | `TsFileDataFrame` |
| `df[df["city"] == "Beijing"]` | Filter by a metadata column | `TsFileDataFrame` |
| `df.loc[start:end, series_list]` | Timestamp-aligned query | `AlignedTimeseries` |
| `df.show(max_rows=20)` / `print(df)` | Print the metadata table | — |
| `df.close()` | Release file handles | — |

### Timeseries

In the table below, `ts` is a `Timeseries`, obtained from `ts = df[...]`.

| Example | Operation | Returns |
|---|---|---|
| `ts.name` | Series name | `str` |
| `len(ts)` | Number of points | `int` |
| `ts.stats` | Series statistics | `dict` (`start_time`, `end_time`, `count`) |
| `ts[20]` | Single value | `float` (or `None` if null) |
| `ts[20:100]` | Row-range slice | `np.ndarray` |
| `ts.timestamps` | Timestamp array | `np.ndarray` |

### AlignedTimeseries

In the table below, `data` is an `AlignedTimeseries`, obtained from
`data = df.loc[...]`.

| Example | Operation | Returns |
|---|---|---|
| `data.shape` | Shape `(N, M)` — N timestamps, M series | `tuple` |
| `data.timestamps` | Timestamp array | `np.ndarray` |
| `data.values` | Value matrix | `np.ndarray`, shape `(N, M)` |
| `data.series_names` | Series names | `List[str]` |
| `len(data)` | Number of rows | `int` |
| `data[0]`, `data[0:10]`, `data[0, 1]` | Row / element indexing | `np.ndarray` / scalar |
| `data.show(50)` / `print(data)` | Formatted output (auto-truncated) | — |

## Series names

A series is uniquely identified by its **series name**, a string formed by
joining the **table name**, the **tag-column values**, and the **field name**
with `.`, in that order:

```text
{table_name}.{tag_value_1}.{tag_value_2}...{field_name}
```

`list_timeseries()` returns series names; name-based indexing (`df[...]`) and
series selection in `df.loc[...]` both take a series name.

Examples:

- `weather.Beijing.humidity` — table `weather`, tag `Beijing`, field `humidity`
- `sensor.s1.pressure` — table `sensor`, tag `s1`, field `pressure`

**Dots inside a name.** Because `.` separates the parts, a `.` that belongs to a
table, tag, or field name is escaped with a backslash. `list_timeseries()`
returns the escaped form — e.g. a `weather` table with tag value `Bei.jing` and
field `humidity` is rendered as `weather.Bei\.jing.humidity` (a literal `\`
becomes `\\`). Selecting it needs the same escaped form: the unescaped
`weather.Bei.jing.humidity` would be read as two tags `Bei` and `jing`. Reuse the
string `list_timeseries()` returns, or type it as a raw string so Python keeps
the backslash:

```python
df[r"weather.Bei\.jing.humidity"]     # selects the device whose tag is "Bei.jing"
```

> A series name can be obtained from `list_timeseries()` and need not be
> constructed by hand; a series may also be selected by integer index (`df[0]`)
> or metadata filter (`df[df["city"] == "Beijing"]`).

## Loading

A path may be a single file, a directory, or a list mixing files and directories:

```python
from tsfile import TsFileDataFrame

df = TsFileDataFrame(["data/weather.tsfile", "data/sensor.tsfile"])
df = TsFileDataFrame("data/")     # recursively find every .tsfile under the directory
print(df)
```

Construction only scans metadata; actual values are not read. When several files
are loaded, their metadata is scanned in parallel, using up to
`min(number_of_files, CPU cores)` threads; a single file is scanned serially.

Only numeric **field** columns hold readable data (`BOOLEAN`, `INT32`, `INT64`,
`FLOAT`, `DOUBLE`, `TIMESTAMP`); non-numeric fields (`STRING`, `TEXT`, `BLOB`,
`DATE`) are skipped during loading and never become series. **Tag** columns are
unaffected — string tags are fully supported as device identifiers and metadata
(series names, the `df["city"]` column, metadata filters).

If several files contain the **same series** (e.g. daily shards of
`weather.Beijing.humidity`), they are merged into one continuous series. Their
timestamps must not conflict across shards; a duplicate timestamp raises an error
when the series is read. Deduplicate during preprocessing.

### Displaying a DataFrame

`print(df)` (and `df.show(max_rows=...)`) prints series metadata, head/tail
truncated when large. The header is:

```text
index │ table │ <tag1> │ <tag2> │ ... │ field │ start_time │ end_time │ count
```

The tag columns shown are the union of every table's tag-column names (in
first-seen order). Each row fills only the tag columns its own table defines;
other tag columns are left blank, and a null tag value shows as `None`.

```text
TsFileDataFrame(table model, 972 time series, 5 files)
     table  ps_id                    sn  frac                 field           start_time             end_time  count
  0    pvf     10  30100194A00234H00572     1                   pac  2024-04-02 00:00:00  2024-10-28 23:45:00  20160
  1    pvf     10  30100194A00234H00572     1    tenmeterswindspeed  2024-04-02 00:00:00  2024-10-28 23:45:00  20160
...
```

## Browsing series

`list_timeseries(path_prefix="")` lists the series names in the loaded files,
optionally filtered by a prefix. Calling it with no argument returns all series.

```python
>>> df.list_timeseries("weather")
['weather.Beijing.humidity', 'weather.Beijing.temperature',
 'weather.Shanghai.humidity', 'weather.Shanghai.temperature']
>>> df.list_timeseries("weather.Beijing")
['weather.Beijing.humidity', 'weather.Beijing.temperature']
```

To inspect metadata such as start/end time and count, print the DataFrame (or a
subset of it) — see [Displaying a DataFrame](#displaying-a-dataframe).

## Selecting series

`df[...]` returns a lazy `Timeseries` handle (no data read) or a subset view:

```python
ts = df["weather.Beijing.humidity"]   # by name
ts = df[0]                            # by index (negative indices allowed)

sub_df = df[0:3]                      # slice           -> TsFileDataFrame (view)
sub_df = df[[0, 2, 5]]                # integer list    -> TsFileDataFrame (view)
sub_df = df[df["city"] == "Beijing"]  # metadata filter -> TsFileDataFrame (view)
```

```text
>>> df["weather.Beijing.humidity"]
Timeseries('weather.Beijing.humidity', count=2880, start=2026-01-27 00:00:00, end=2026-02-05 23:55:00)
```

Series metadata is served from cache (no I/O):

```python
>>> ts = df["weather.Beijing.humidity"]
>>> ts.name
'weather.Beijing.humidity'
>>> len(ts)
2880
>>> ts.stats
{'start_time': 1769443200000, 'end_time': 1770306900000, 'count': 2880}
```

## Reading data

Indexing a `Timeseries` by row triggers the actual file read:

```python
val = ts[20]            # -> float
window = ts[20:100]     # -> np.ndarray, shape = (80,)
last_ten = ts[-10:]     # -> np.ndarray
sampled = ts[::2]       # -> np.ndarray (strided sampling)
ts.timestamps[20:100]   # -> the timestamps for those rows, np.ndarray
```

```text
>>> ts[20]
46.1
>>> ts[20:100]
array([46.1 , 41.72, 52.94, ..., 76.3 , 84.35])
>>> ts.timestamps[20:100]
array([1769449200000, 1769449500000, ..., 1769472900000])
```

## Timestamp-aligned queries

When you need several series strictly aligned on one time axis, use `.loc`:

```python
data = df.loc[start_time:end_time, [
    "weather.Beijing.humidity",
    "weather.Beijing.temperature",
    "sensor.s1.pressure",
]]
```

The returned `AlignedTimeseries` aligns all series to the **union** of their
timestamps and fills missing positions with `NaN`:

```python
data.timestamps    # np.ndarray, millisecond timestamps
data.values        # np.ndarray, shape = (N, 3)
data.series_names  # ["weather.Beijing.humidity", ...]
data.shape         # (N, 3)
data[0:10]         # first 10 rows, np.ndarray shape = (10, 3)
data.show(50)      # show up to 50 rows
```

Series may be given by name or by index, mixed freely:

```python
df.loc[start_time:end_time, [0, 1, 4]]
df.loc[start_time:end_time, [0, "weather.Beijing.temperature", 4]]
```

```text
>>> df.loc[1769616000000:1769702100000,
...        ['weather.Beijing.temperature', 'weather.Beijing.humidity', 'sensor.s2.pressure']]
AlignedTimeseries(288 rows, 3 series)
          timestamp  weather.Beijing.temperature  weather.Beijing.humidity  sensor.s2.pressure
2026-01-29 00:00:00                        29.12                     92.87                 NaN
2026-01-29 00:05:00                         1.55                     87.34                 NaN
...
```

Printing the result shows the time column to the left of the values, but the
`.values` matrix holds only the value columns — read the aligned timestamps from
`df.loc[...].timestamps`.

## Closing

A `with` block closes file handles automatically; you can also close manually:

```python
with TsFileDataFrame("data/") as df:
    ...                       # handles released on exit

tsdf = TsFileDataFrame("data/")
tsdf.close()                  # or close it yourself
```
