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

# Nulls

A non-aligned timeseries stores only the `(timestamp, value)` points that exist. Its Pages do not
contain null markers.

An aligned value column records nulls in the bitmap of each Value Page:

```text
AlignedValuePagePayload :=
    int32  position_count
    byte   present_bitmap[ceil(position_count / 8)]
    byte   encoded_non_null_values[remaining bytes]
```

Position `i` corresponds to bit `7 - (i mod 8)` of `present_bitmap[i / 8]`:

- A set bit means that the position has a value.
- A clear bit means that the position is null.

The encoded value stream contains only values whose bitmap bits are set, in position order.
`position_count` equals the number of timestamps in the corresponding Time Page.

For example, with `position_count = 10`, bitmap bytes `10110000 01000000` indicate values at
positions `0`, `2`, `3`, and `9`. The value decoder therefore produces four values and places them
at those positions; the remaining six positions are null. Unused low-order bits in the final byte
are not logical positions and are ignored.

For a non-empty Value Page the following invariants hold:

```text
bitmap_bytes       = ceil(position_count / 8)
decoded_value_count = popcount(present_bitmap over position_count bits)
position_count      = decoded_timestamp_count of the corresponding Time Page
```

A mismatch is structural corruption; the reader must not infer additional positions from trailing
bitmap bits or encoded values.

If an entire Value Page is null, the Page consists only of an `uncompressed_size` equal to zero:

```text
EmptyPage := UVarInt(0)
```

An empty Page has no `compressed_size`, Statistics, bitmap, or value stream. Its position count is
obtained from the corresponding Time Page.

An absent Value Chunk and an empty Value Page are different. An absent Chunk means that the
measurement has no physical column in that Chunk Group. An empty Page preserves the Page-to-Page
alignment of an existing value column while stating that every position in that interval is null.
