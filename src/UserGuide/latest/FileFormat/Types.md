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

# Types

Chunk Headers, Timeseries Metadata, and Table Schemas use a single-byte `data_type` identifier:

| Value | Name | Physical or logical meaning |
| ---: | --- | --- |
| 0 | `BOOLEAN` | Boolean value |
| 1 | `INT32` | Signed 32-bit integer |
| 2 | `INT64` | Signed 64-bit integer |
| 3 | `FLOAT` | IEEE 754 binary32 |
| 4 | `DOUBLE` | IEEE 754 binary64 |
| 5 | `TEXT` | Length-prefixed byte string |
| 6 | `VECTOR` | Physical type marker used by an aligned Time Chunk |
| 7 | `UNKNOWN` | Unknown type marker |
| 8 | `TIMESTAMP` | Signed 64-bit integer; the upper layer defines the time unit |
| 9 | `DATE` | Signed 32-bit integer in decimal `YYYYMMDD` form |
| 10 | `BLOB` | Arbitrary byte string |
| 11 | `STRING` | UTF-8 string |
| 12 | `OBJECT` | Byte representation of an object value |

For example, the physical `DATE` value for 2026-07-27 is the decimal integer `20260727`, not a
Unix epoch day.

The data type determines the additional Statistics fields and the input type of the value
encoder. See [Encodings](DataPages/Encodings.md) for wire representations of values.

## Physical and Logical Types

The identifier determines the physical decoder. `TIMESTAMP` and `DATE` additionally carry a
logical interpretation:

- `TIMESTAMP` has the same eight-byte physical value domain as `INT64`. The time unit is supplied
  by the protocol or dataset using the file; TsFile v4 does not serialize that unit.
- `DATE` has the same integer encoder interface as `INT32`, but valid logical values use decimal
  `YYYYMMDD` form.
- `TEXT` is a length-delimited byte string. `STRING` uses the same physical binary container but
  has UTF-8 string semantics. `BLOB` is uninterpreted binary data.
- `OBJECT` is an opaque binary representation whose object contract must be agreed outside the
  file format.

`VECTOR` is a physical marker for the shared time column of aligned data. It is not a scalar value
type and does not define a standalone value-stream encoding. `UNKNOWN` is a sentinel, not a valid
type for a decodable measurement Page.

## Type Consistency

The same series type is repeated at different levels so a reader can enter the file through the
footer or through a sequential scan. These copies must agree:

- A non-aligned or aligned Value Chunk Header uses the measurement's scalar `data_type`.
- Its Timeseries Metadata uses the same `data_type`.
- Statistics are decoded using that type and contain no independent type byte.
- An aligned Time Chunk uses `VECTOR`; its decoded values are signed 64-bit timestamps.

Changing the interpretation of a previously assigned identifier is incompatible. A reader that
does not support a type identifier may still skip a complete Chunk using `data_size`, but it
cannot decode values or type-specific Statistics for that series.

## Ordering and Comparison

Integer and floating-point values use their numeric order for min/max Statistics. `STRING`
min/max values use binary lexicographic comparison of their serialized bytes. `TEXT` stores only
first and last values, while `BLOB` and `OBJECT` store no type-specific value Statistics.

Type promotion and application-level casts are not encoded in TsFile. A reader can expose such
conversions, but they do not change the stored `data_type` or the bytes used to compute
Statistics.
