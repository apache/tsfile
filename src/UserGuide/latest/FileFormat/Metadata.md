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

# Metadata

TsFile metadata falls into two categories:

- Chunk Headers, Page Headers, and Page Statistics stored with the data.
- Timeseries Metadata, Metadata Indexes, and File Metadata stored after the data section.

Footer metadata starts after `SEPARATOR (0x02)`. Ranges of `TimeseriesMetadata` and measurement
index nodes may be interleaved. The `offset` and `end_offset` fields in the indexes delimit each
range. Every offset is an absolute byte offset measured from the beginning of the file.

A lookup follows the hierarchy below. Each level narrows the search, from a table to the Pages
that may contain matching values:

| Level | Identifies | Primary use |
| --- | --- | --- |
| File metadata | Tables and device-index roots | Enter the correct index tree |
| Device index | Devices | Locate one device's measurement index |
| Measurement index | Measurement-name ranges | Locate Timeseries Metadata |
| Timeseries Metadata | One logical series | Select Chunk offsets and series Statistics |
| Chunk Metadata | One physical Chunk | Seek to data and prune by Chunk Statistics |
| Page Header | One Page | Prune by Page Statistics and read its payload |

## Statistics

Page-, Chunk-, and timeseries-level Statistics use the same type-specific structure:

```text
Statistics :=
    UVarInt  count
    int64    start_time
    int64    end_time
    ...      type-specific fields
```

`count` is the number of values covered by the structure. It equals the number of points in a
non-aligned series, the number of non-null values in an aligned value column, or the number of
timestamps in an aligned time column.

Type-specific fields immediately follow the common prefix in the order shown below:

| Data type | Type-specific fields |
| --- | --- |
| `BOOLEAN` | `bool first`, `bool last`, `int64 true_count` |
| `INT32`, `DATE` | `int32 min`, `int32 max`, `int32 first`, `int32 last`, `int64 sum` |
| `INT64`, `TIMESTAMP` | `int64 min`, `int64 max`, `int64 first`, `int64 last`, `float64 sum` |
| `FLOAT` | `float32 min`, `float32 max`, `float32 first`, `float32 last`, `float64 sum` |
| `DOUBLE` | `float64 min`, `float64 max`, `float64 first`, `float64 last`, `float64 sum` |
| `TEXT` | `Binary32 first`, `Binary32 last` |
| `STRING` | `Binary32 first`, `Binary32 last`, `Binary32 min`, `Binary32 max` |
| `BLOB`, `OBJECT`, `VECTOR` | No type-specific fields |

Statistics do not carry a data type identifier. The type used to parse Statistics comes from the
Chunk Header or Timeseries Metadata.

`first` and `last` describe value order, not the minimum and maximum. For an ordered timeseries,
they correspond to the values at the first and last covered timestamps. The `start_time` and
`end_time` fields are inclusive. Numeric `sum` fields are intended for aggregation; notably, an
`INT64` sum is stored as `float64` and can lose integer precision for large magnitudes.

Statistics help rule out data that cannot match a query. For example, a Page whose time range
falls entirely before the requested range can be skipped. Overlapping ranges only mean the Page
might match; the reader still needs to check its values. Types without minimum/maximum
fields, such as `BLOB`, `OBJECT`, and `TEXT`, cannot support range pruning through Statistics.

Chunk Statistics summarize all Pages in that Chunk; series Statistics summarize all of its
Chunks. Parent counts add the child counts, and parent time bounds cover the child ranges.

## Timeseries Metadata

Each timeseries has one `TimeseriesMetadata` structure:

```text
TimeseriesMetadata :=
    byte        timeseries_type
    VarString   measurement_id
    byte        data_type
    UVarInt     chunk_metadata_list_size
    Statistics  series_statistics
    byte        chunk_metadata_list[chunk_metadata_list_size]
```

The high bits of `timeseries_type` identify the column role, and the low six bits identify the
Chunk Metadata layout:

```text
bit 7 = 1: aligned time column
bit 6 = 1: aligned value column
low 6 bits = 0: exactly one Chunk; ChunkMetadata omits Statistics
low 6 bits = 1: more than one Chunk; every ChunkMetadata contains Statistics
```

## Chunk Metadata

When `(timeseries_type & 0x3f) == 0`, the timeseries has exactly one Chunk:

```text
ChunkMetadata := int64 offset_of_chunk_header
```

The Chunk uses `series_statistics` as its Statistics.

When `(timeseries_type & 0x3f) != 0`:

```text
ChunkMetadata :=
    int64      offset_of_chunk_header
    Statistics chunk_statistics
```

`offset_of_chunk_header` points to the Chunk marker, which is the first byte of the Chunk Header.

`chunk_metadata_list_size` is the serialized byte length of the list. Add it to the list start
to find the end, then read Chunk Metadata records until that end is reached exactly. The field
does not give the number of Chunks.

## Metadata Index

```text
MetadataIndexNode :=
    UVarInt          child_count
    MetadataEntry    child[child_count]
    int64            end_offset
    byte             node_type
```

`node_type` determines the node level, Entry encoding, and referenced data:

| Value | Name | Entry encoding | Entry refers to |
| ---: | --- | --- | --- |
| 0 | `INTERNAL_DEVICE` | `DeviceIndexEntry` | A child device-index node |
| 1 | `LEAF_DEVICE` | `DeviceIndexEntry` | The measurement-index root of a device |
| 2 | `INTERNAL_MEASUREMENT` | `MeasurementIndexEntry` | A child measurement-index node |
| 3 | `LEAF_MEASUREMENT` | `MeasurementIndexEntry` | A range of Timeseries Metadata |

The Entry structures are:

```text
DeviceIndexEntry :=
    DeviceID  first_device_id
    int64     offset

MeasurementIndexEntry :=
    VarString first_measurement_id
    int64     offset
```

An Entry describes a half-open byte range `[offset, next_offset)`. The next Entry supplies
`next_offset`; for the last Entry, use the node's `end_offset`. The end byte belongs to the next
range, not the current one.

```text
entry[0].offset <= entry[1].offset <= ... <= end_offset
```

An Entry key is the first key in the range it covers. Device indexes compare `DeviceID` values
segment by segment, with null before non-null and a shorter tuple before a longer tuple when their
common prefixes are equal. Measurement indexes use lexicographic measurement-name order.

### Index Lookup

To find key `K` in an index node, select the last Entry whose first key is less than or equal to
`K`. The selected byte range begins at that Entry's `offset` and ends at the next Entry's offset,
or at the node's `end_offset` for the last Entry. An internal node range contains another index
node; a leaf range contains the object named in the table above.

If no Entry key is less than or equal to `K`, the key is absent from this node. A measurement
leaf can cover several Timeseries Metadata records; read that range to find the exact measurement.

Before following a node, check that:

- `child_count` is consistent with the number of complete Entries in the node.
- Entry keys are sorted according to the node's key comparator.
- Entry offsets are monotonic and each referenced range is non-negative.
- The last Entry offset does not exceed `end_offset`.
- Child nodes have the level expected by the parent node type.

The first-key representation allows an implementation to binary-search Entries within a node.
The file format does not require a particular in-memory search algorithm.

## File Metadata

`TsFileMetadata` immediately precedes the file's trailing length field:

```text
TsFileMetadata :=
    UVarInt          table_index_count
    TableIndexRoot   table_index[table_index_count]
    UVarInt          table_schema_count
    NamedTableSchema table_schema[table_schema_count]
    int64            meta_offset
    BloomFilter      bloom_filter
    VarInt           property_count
    Property         property[property_count]

TableIndexRoot :=
    VarString         table_name
    MetadataIndexNode device_index_root

NamedTableSchema :=
    VarString    table_name
    TableSchema  schema

Property :=
    VarString key
    VarBinary value
```

Property keys are UTF-8 strings; values are nullable raw byte arrays, not UTF-8 strings.
For example, value bytes `ff fe 00 80` serialize as `08 ff fe 00 80`: `08` is `VarInt(4)`.
A null value is `01` (`VarInt(-1)`), while an empty value is `00`. Preserve unknown values
byte-for-byte, and decode them as text only when the property contract requires it.

`meta_offset` points to the `0x02` separator at the end of the data section. There is no marker
before `TsFileMetadata`; its starting position is determined by the 4-byte length stored at the
end of the file.

The physical order of map entries has no file-format meaning. `table_index` contains device-index
roots for both tree-model and table-model data. `table_schema` contains schemas for the explicit
table model.

The trailing four-byte metadata length is outside `TsFileMetadata`. For a file length `F` and
metadata length `L`, a reader obtains the exact byte interval using:

```text
file_metadata_start = F - 6 - 4 - L
file_metadata_end   = F - 6 - 4       # exclusive
```

The reader must consume exactly `L` bytes. This check detects a schema mismatch or malformed
length-delimited field before the trailing magic is accepted as the file terminator.

## Table Schema

```text
TableSchema :=
    UVarInt      column_count
    TableColumn  column[column_count]

TableColumn :=
    MeasurementSchema schema
    int32             column_category

MeasurementSchema :=
    String32       column_name
    byte           data_type
    byte           encoding_type
    byte           compression_type
    int32          property_count
    SchemaProperty property[property_count]

SchemaProperty :=
    String32 key
    String32 value
```

`MeasurementSchema` uses `String32`, rather than the `VarString` used by most other footer fields.

Column category identifiers are:

| Value | Category |
| ---: | --- |
| 0 | `TAG` |
| 1 | `FIELD` |
| 2 | `ATTRIBUTE` |
| 3 | `TIME` |

The schema preserves column order. In particular, TAG column order determines the order of
DeviceID segments. FIELD Chunks are identified by `measurement_id`; their physical order does not
have to match the FIELD column order in the schema.

`TAG` columns identify a device, `FIELD` columns hold measured values, `ATTRIBUTE` columns
describe an entity, and `TIME` identifies the time column. A column needs both its category and
its `data_type`: the category describes its role, while the type describes its values.
