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

The metadata hierarchy supports progressively finer pruning:

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

Statistics are conservative pruning metadata. A reader may skip a structure when its time range
does not intersect the query range, or when a supported value summary proves that no value can
match. It must not infer a match merely from overlapping ranges. Types without minimum/maximum
fields, such as `BLOB` and `OBJECT`, cannot support range pruning through Statistics.

Statistics at a parent level summarize all values represented by that parent. For a series with
multiple Chunks, series Statistics cover the union of the Chunk Statistics. For a multi-Page
Chunk, Chunk Statistics cover the union of Page Statistics. Counts and time bounds must be
consistent with those lower-level records.

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

When `timeseries_type & 0x3f == 0`, the timeseries has exactly one Chunk:

```text
ChunkMetadata := int64 offset_of_chunk_header
```

The Chunk uses `series_statistics` as its Statistics.

When `timeseries_type & 0x3f != 0`:

```text
ChunkMetadata :=
    int64      offset_of_chunk_header
    Statistics chunk_statistics
```

`offset_of_chunk_header` points to the Chunk marker, which is the first byte of the Chunk Header.

`chunk_metadata_list_size` is a byte boundary, not a Chunk count. A reader first computes the end
of the list, then deserializes Chunk Metadata records until it reaches exactly that end. Ending
early, crossing the boundary, or leaving trailing bytes is a format error.

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

The `offset` of an Entry is the start of its range. Its end is the `offset` of the next Entry; the
last Entry ends at the node's `end_offset`.

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

An index reader validates the node before following it:

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
    VarString value
```

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

Column order is significant because table records and aligned value columns refer to the schema
positionally. `TAG` columns participate in device identity, `FIELD` columns hold measured values,
`ATTRIBUTE` columns describe entities, and `TIME` identifies the time column. The category does
not replace `data_type`; both fields are required to interpret a column.
