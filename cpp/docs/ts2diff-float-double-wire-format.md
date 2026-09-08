<!-- Licensed to the Apache Software Foundation (ASF) under one
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
under the License. -->

# FLOAT/DOUBLE TS_2DIFF Wire Format (Java Canonical Layout)

This document specifies the canonical on-disk layout of FLOAT/DOUBLE TS_2DIFF
pages, derived from the Java reference implementation
(`FloatEncoder`, `FloatDecoder`, `DeltaBinaryEncoder`, `BitMap`).
The Java layout is the cross-language compatibility boundary. Other layouts
produced by earlier C++ writers (raw bit-cast, per-block wrapper metadata) are
implementation artifacts outside the compatibility scope; the C++ decoder
treats them as a format error.

## Encoding Pipeline

TS_2DIFF encodes integers. Floating-point values go through a wrapper that
converts each value to an integer, encodes the integers with
`IntDeltaEncoder` (FLOAT) or `LongDeltaEncoder` (DOUBLE), and emits page-wide
conversion metadata.

Given `maxPointNumber = mpn` and `maxPointValue = 10^mpn` (`mpn <= 0` implies
`maxPointValue = 1`), each value maps to one of three stored forms:

| Condition                              | Stored bits                 | Decoder action            |
| -------------------------------------- | --------------------------- | ------------------------- |
| `round(v * 10^mpn)` fits the int type  | `round(v * 10^mpn)`         | divide by `10^mpn`        |
| scaled overflows but `v` itself fits   | `round(v)`                  | divide by `1`             |
| `v` out of int range, or NaN           | `floatToIntBits(v)` / `doubleToLongBits(v)` | restore raw bits |

The three forms are tracked per page as a tri-state flag list
(`underflowFlags` in Java):

- `true`  -> scaled form
- `false` -> rounded form (scale overflow)
- `null`  -> raw IEEE 754 bits (value overflow or NaN)

`round` is Java `Math.round` semantics, `floor(x + 0.5)` — ties go towards
`+infinity` (`-2.5 -> -2`), unlike C `lround`'s ties-away-from-zero.

## Page Layout

```text
# Form 1: every value stored in scaled form (no bitmap at all)
[maxPointNumber varint]
[TS_2DIFF block 1][TS_2DIFF block 2]...[final block]

# Form 2: at least one value is 'false' (scale overflow), none is 'null'
[Integer.MAX_VALUE varint]                    # 0xFF 0xFF 0xFF 0xFF 0x07
[pageValueCount varint]
[scaled-bitmap, pageValueCount/8+1 bytes]     # marks 'true' entries
[maxPointNumber varint]
[TS_2DIFF block 1]...[final block]

# Form 3: at least one value is 'null' (raw bits)
[Integer.MAX_VALUE-1 varint]                  # 0xFF 0xFF 0xFF 0xFF 0x06
[pageValueCount varint]
[scaled-bitmap, pageValueCount/8+1 bytes]     # marks 'true' entries
[raw-bitmap, pageValueCount/8+1 bytes]        # marks 'null' entries
[maxPointNumber varint]
[TS_2DIFF block 1]...[final block]
```

Key invariants:

- `maxPointNumber` appears exactly once per page, before the first integer
  block (for Forms 2/3 it appears after the bitmaps).
- The bitmaps cover the entire page, not individual TS_2DIFF blocks. The
  decoder keeps one page-wide `position` that never resets between blocks;
  only a page-level `reset()` clears it.
- Bitmap byte length is always `size/8 + 1`, even when `size % 8 == 0`
  (`BitMap.getSizeOfBytes`).
- Bitmap bit order is LSB-first within each byte: position `p` maps to
  `bits[p / 8] & (1 << (p % 8))`.
- `pageValueCount` counts all values of the page (across blocks).
- A first-page byte of `0x00` is the normal encoding of `maxPointNumber = 0`
  (an explicit `max_point_number=0` property on the Java side; reachable
  but not the default — see Encoder Construction below). It is not a
  legacy marker.
- NaN handling is writer-dependent: Java `floatToIntBits` /
  `doubleToLongBits` canonicalize any NaN to `0x7fc00000` /
  `0x7ff8000000000000`, while the C++ encoder preserves the payload bits.
  Both are valid raw-bit entries; readers restore the bits as stored.

## Integer Block Layout

Identical to the integer TS_2DIFF format (`DeltaBinaryEncoder`):

```text
[writeIndex int32 BE]   # number of deltas in this block
[bitWidth  int32 BE]
[block-specific header] # first value; min delta
[packed data]           # writeIndex * bitWidth bits
```

A block stores `writeIndex + 1` values (first value + writeIndex deltas).
`BLOCK_DEFAULT_SIZE = 128` is only `DeltaBinaryEncoder`'s default buffer
size, not a wire-format limit: Java exposes block-size constructors
(`IntDeltaEncoder(int)`), so `writeIndex` is bounded only by the declared
page value count and the packed bytes available. A 300-value page from the
default encoder produces blocks of 129, 129, 42 values.

## Encoder Construction

The Java `TSEncodingBuilder.Ts2Diff` field initializes `maxPointNumber = 0`,
but the standard schema write path (`MeasurementSchema.getValueEncoder`)
always calls `initFromProps()`, which replaces it with the schema's
`max_point_number` property or, when the property is absent, with
`TSFileConfig.floatPrecision` (current default `2`). A writer that
explicitly sets `max_point_number = 0` produces Form 1 pages starting with
`0x00`. The C++ `FloatTS2DIFFEncoder` / `DoubleTS2DIFFEncoder` default to
`2`, matching the standard Java schema path. The value stored in the
stream is self-describing, so files written with other `maxPointNumber`
values remain readable.

## Decoder State Machine

Per page, exactly once, the decoder reads the leading marker:

1. Read varint `tag`.
2. `tag == Integer.MAX_VALUE`   -> read `count` varint, `count/8+1` bytes
   scaled-bitmap, then varint `maxPointNumber` (Form 2).
3. `tag == Integer.MAX_VALUE-1` -> additionally read a second
   `count/8+1` bytes raw-bitmap (Form 3).
4. Otherwise `tag` itself is `maxPointNumber` (Form 1); `mpn <= 0` means
   `maxPointValue = 1`.

Then values are decoded from the integer blocks. For value at page position
`p`:

- raw-bitmap (if present) marks `p` -> `intBitsToFloat` / `longBitsToDouble`
- else scaled-bitmap (if present) marks `p` -> `value / 10^mpn`
- else -> `value / 1`

Any input that does not conform to this grammar (for example, an integer
TS_2DIFF block header where the page metadata is expected) is a format error.
