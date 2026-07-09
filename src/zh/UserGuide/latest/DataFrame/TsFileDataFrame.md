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

`TsFileDataFrame` 让你像操作 pandas DataFrame 一样读取一个或多个 TsFile 中的数值型测点数据，
无需关心底层文件格式与数据加载细节。它是 TsFile Python 包的一部分（`pip install tsfile`）。

## 快速上手

```python
from tsfile import TsFileDataFrame

df = TsFileDataFrame("table_data/")           # 加载目录下所有 .tsfile
print(df)                                     # 浏览所有序列（仅元数据）

ts = df["weather.Beijing.humidity"]           # 取一条序列（懒加载句柄）
window = ts[20:100]                           # 按行号切片 -> np.ndarray

data = df.loc[start:end, [                     # 按时间戳对齐多条序列
    "weather.Beijing.temperature",
    "weather.Beijing.humidity",
]]
data.values                                   # -> np.ndarray, shape (N, 2)：N 个时间戳 × 2 条序列
```

## 核心类型

`TsFileDataFrame` 围绕三个核心类型：

- **`TsFileDataFrame`**：入口对象，加载一至多个 TsFile 并提供统一视图。初始化时只扫描元数据，
  **不读取实际数值**。
- **`Timeseries`**：单条序列的懒加载句柄，通过 `df[...]` 获得。它携带序列元信息，但在按行号索引前
  不读取任何数据。
- **`AlignedTimeseries`**：多条序列在同一时间轴上的对齐结果，通过 `df.loc[...]` 获得，会一次性将
  指定时间范围内的数据读入内存：对齐后的时间戳数组（`.timestamps`，长度 **N**）以及值矩阵
  （`.values`，形状 **(N, M)**）——**N** 行时间戳 × **M** 列序列。

### TsFileDataFrame

下表中 `df` 是一个 `TsFileDataFrame` 实例，由 `df = TsFileDataFrame(paths)` 创建。

| 示例 | 操作 | 返回类型 |
|---|---|---|
| `TsFileDataFrame(paths, show_progress=True)` | 加载文件 / 文件列表 / 目录。设置 `show_progress=False` 可关闭 stderr 上的加载进度 | `TsFileDataFrame` |
| `len(df)` | 时间序列总数 | `int` |
| `df.model` | 已加载文件模型：`"table"` 或 `"tree"` | `str` |
| `df.list_timeseries("weather")` | 获取序列名，可按前缀筛选 | `List[str]` |
| `df.list_timeseries_metadata("weather")` | 获取序列元数据，可按前缀筛选 | `pandas.DataFrame` |
| `df["weather.Beijing.humidity"]`、`df[0]`、`df[-1]` | 获取单条序列 | `Timeseries` |
| `df["city"]` | 获取某元数据列（标签 / `field` / `start_time` / `end_time` / `count`） | `pandas.Series` |
| `df[0:3]`、`df[[0, 2, 5]]` | 按整数位置取子集视图：连续区间（`0:3`）或所列位置（`[0, 2, 5]`）；位置即打印的 `index` 列 | `TsFileDataFrame` |
| `df[df["city"] == "Beijing"]` | 按元数据列过滤 | `TsFileDataFrame` |
| `df.loc[start:end, series_list]` | 按时间戳对齐查询 | `AlignedTimeseries` |
| `df.show(max_rows=20)` / `print(df)` | 打印元数据表格 | — |
| `df.close()` | 释放文件句柄 | — |

### Timeseries

下表中 `ts` 是一条 `Timeseries`，由 `ts = df[...]` 获得。

| 示例 | 操作 | 返回类型 |
|---|---|---|
| `ts.name` | 序列名 | `str` |
| `len(ts)` | 序列点数 | `int` |
| `ts.stats` | 序列统计信息 | `dict`（`start_time`、`end_time`、`count`） |
| `ts[20]` | 单值读取 | `float`（空值为 `None`） |
| `ts[20:100]` | 行范围切片 | `np.ndarray` |
| `ts.timestamps` | 时间戳数组 | `np.ndarray` |

### AlignedTimeseries

下表中 `data` 是一个 `AlignedTimeseries`，由 `data = df.loc[...]` 获得。

| 示例 | 操作 | 返回类型 |
|---|---|---|
| `data.shape` | 形状 `(N, M)`——N 为时间戳数，M 为序列数 | `tuple` |
| `data.timestamps` | 时间戳数组 | `np.ndarray` |
| `data.values` | 值矩阵 | `np.ndarray`，shape `(N, M)` |
| `data.series_names` | 序列名列表 | `List[str]` |
| `len(data)` | 行数 | `int` |
| `data[0]`、`data[0:10]`、`data[0, 1]` | 行 / 元素索引 | `np.ndarray` / 标量 |
| `data.show(50)` / `print(data)` | 格式化输出（自动截断） | — |

## 序列名

TsFileDataFrame 以**序列名**（一个字符串）作为序列的唯一标识。序列名由 **表名**、**各标签列的取值**、
**字段名** 三部分按此顺序经 `.` 连接构成：

```text
{表名}.{标签值1}.{标签值2}...{字段名}
```

`list_timeseries()` 返回 `SeriesPath` 对象——`str` 的子类，其字符串值即上面的转义路径，可直接用作 `df[...]`、`df.loc[...]` 的键；它还以 `.table`、`.tags`（元组，`None` 表示空标签）、`.field` 暴露各组成部分。

示例：

- `weather.Beijing.humidity` — 表 `weather`，标签 `Beijing`，字段 `humidity`
- `sensor.s1.pressure` — 表 `sensor`，标签 `s1`，字段 `pressure`

**名称中的转义。** `.` 用作分隔符，因此属于表名/标签/字段名本身的 `.` 会用反斜杠转义；空标签值写作 `\N`（与字面字符串 `"null"` 区分）。`list_timeseries()` 返回的就是这种转义形式——例如表 `weather` 中标签值 `Bei.jing`、字段 `humidity` 渲染为 `weather.Bei\.jing.humidity`（字面 `\` 转义为 `\\`）。选取时也要用这种转义形式，因此请直接复用 `list_timeseries()` 返回的 `SeriesPath`（或读取它的 `.table`/`.tags`/`.field`），不要手拼；若确需手写，请用 raw string 让 Python 保留反斜杠：

```python
df[r"weather.Bei\.jing.humidity"]     # 标签 "Bei.jing"（点号是值的一部分）
df[r"weather.\N.Beijing.humidity"]    # 标签 (null, "Beijing")
```

> 序列名可由 `list_timeseries()` 获取，无需手工构造；亦可改用整数索引（`df[0]`）或元数据过滤
> （`df[df["city"] == "Beijing"]`）选择序列。

## 加载

路径可以是单个文件、目录，或由文件与目录混合组成的列表：

```python
from tsfile import TsFileDataFrame

df = TsFileDataFrame(["data/weather.tsfile", "data/sensor.tsfile"])
df = TsFileDataFrame("data/")     # 递归查找目录下所有 .tsfile
df = TsFileDataFrame("data/", show_progress=False)
print(df)
```

初始化时只扫描元数据，不读取实际数值。加载多个文件时，会并行扫描元数据，使用 `min(文件数, CPU 核数)` 个线程；单个文件则串行扫描。

只有数值型 **field（字段）** 列承载可读数据（`BOOLEAN`、`INT32`、`INT64`、`FLOAT`、`DOUBLE`、`TIMESTAMP`）；非数值字段（`STRING`、`TEXT`、`BLOB`、`DATE`）在加载时被跳过，不会成为序列。**tag（标签）** 列不受此限——字符串标签作为设备标识与元数据（序列名、`df["city"]` 列、元数据过滤）是完全支持的。

如果多个文件包含 **同名序列**（如按日分片的 `weather.Beijing.humidity`），会自动合并为一条连续序列。
各分片的时间戳不能冲突（不可重复），否则读取该序列时会报错。请在预处理阶段去重。

### DataFrame 的展示

`print(df)`（以及 `df.show(max_rows=...)`）打印序列元信息，数据量大时头尾截断。表头为：

```text
index │ table │ <tag1> │ <tag2> │ ... │ field │ start_time │ end_time │ count
```

表头的标签列是所有表 tag 列名的并集（按首次出现顺序排列）。每行只填本表拥有的 tag 列，其余留空；仅当标签值本身为空时才显示 `None`。

```text
TsFileDataFrame(table model, 972 time series, 5 files)
     table  ps_id                    sn  frac                 field           start_time             end_time  count
  0    pvf     10  30100194A00234H00572     1                   pac  2024-04-02 00:00:00  2024-10-28 23:45:00  20160
  1    pvf     10  30100194A00234H00572     1    tenmeterswindspeed  2024-04-02 00:00:00  2024-10-28 23:45:00  20160
...
```

## 浏览序列

`list_timeseries(path_prefix="")` 列出已加载文件中的序列名，可按前缀筛选；不传参返回全部序列。

```python
>>> df.list_timeseries("weather")
['weather.Beijing.humidity', 'weather.Beijing.temperature',
 'weather.Shanghai.humidity', 'weather.Shanghai.temperature']
>>> df.list_timeseries("weather.Beijing")
['weather.Beijing.humidity', 'weather.Beijing.temperature']
```

若需查看起止时间、点数等元信息，可打印 DataFrame（或其子集）——见[DataFrame 的展示](#dataframe-的展示)——也可以调用
`list_timeseries_metadata()`。

## 查看元数据

`list_timeseries_metadata(path_prefix="")` 返回以序列名为索引的 pandas DataFrame。
其中包含 `field`、`start_time`、`end_time`、`count` 以及设备标签列。对于
table 模型文件，还会包含 `table` 列。

```python
meta = df.list_timeseries_metadata()
weather_meta = df.list_timeseries_metadata("weather")

meta[["field", "start_time", "end_time", "count"]].head()
```

## 选取序列

`df[...]` 返回懒加载的 `Timeseries` 句柄（不触发读取），或返回子集视图：

```python
ts = df["weather.Beijing.humidity"]   # 按名称
ts = df[0]                            # 按索引（支持负索引）

sub_df = df[0:3]                      # 切片         -> TsFileDataFrame（视图）
sub_df = df[[0, 2, 5]]                # 整数列表     -> TsFileDataFrame（视图）
sub_df = df[df["city"] == "Beijing"]  # 按元数据过滤 -> TsFileDataFrame（视图）
```

```text
>>> df["weather.Beijing.humidity"]
Timeseries('weather.Beijing.humidity', count=2880, start=2026-01-27 00:00:00, end=2026-02-05 23:55:00)
```

序列元信息从缓存读取（无 I/O）：

```python
>>> ts = df["weather.Beijing.humidity"]
>>> ts.name
'weather.Beijing.humidity'
>>> len(ts)
2880
>>> ts.stats
{'start_time': 1769443200000, 'end_time': 1770306900000, 'count': 2880}
```

## 读取数据

对 `Timeseries` 按行号索引时才触发实际的文件读取：

```python
val = ts[20]            # -> float
window = ts[20:100]     # -> np.ndarray, shape = (80,)
last_ten = ts[-10:]     # -> np.ndarray
sampled = ts[::2]       # -> np.ndarray（步长采样）
ts.timestamps[20:100]   # -> 对应行号的时间戳, np.ndarray
```

```text
>>> ts[20]
46.1
>>> ts[20:100]
array([46.1 , 41.72, 52.94, ..., 76.3 , 84.35])
>>> ts.timestamps[20:100]
array([1769449200000, 1769449500000, ..., 1769472900000])
```

## 多序列对齐查询

当需要多条序列在同一时间轴上严格对齐时，使用 `.loc`：

```python
data = df.loc[start_time:end_time, [
    "weather.Beijing.humidity",
    "weather.Beijing.temperature",
    "sensor.s1.pressure",
]]
```

返回的 `AlignedTimeseries` 将所有序列对齐到时间戳的 **并集**，缺失位置填充 `NaN`：

```python
data.timestamps    # np.ndarray，毫秒时间戳
data.values        # np.ndarray, shape = (N, 3)
data.series_names  # ["weather.Beijing.humidity", ...]
data.shape         # (N, 3)
data[0:10]         # 前 10 行, np.ndarray shape = (10, 3)
data.show(50)      # 最多显示 50 行
```

序列可按名称或索引指定，并可混用：

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

打印结果会在值列左侧显示时间列，但 `.values` 矩阵只含值列——对齐后的时间戳请用 `df.loc[...].timestamps` 读取。

## 关闭

`with` 语句会自动释放文件句柄，也可以手动关闭：

```python
with TsFileDataFrame("data/") as df:
    ...                       # 退出后自动关闭

tsdf = TsFileDataFrame("data/")
tsdf.close()                  # 也可以自己关闭
```
