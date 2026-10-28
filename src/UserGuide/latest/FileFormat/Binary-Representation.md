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

# Binary Representation

All offsets are measured from the first byte of the file, and all lengths are in bytes. Unless
otherwise specified, fixed-width integers and IEEE 754 floating-point values use big-endian byte
order. Strings use UTF-8.

## Fixed-width Values

| Notation | Bytes | Meaning |
| --- | ---: | --- |
| `byte` | 1 | An 8-bit raw value |
| `bool` | 1 | `0x00` for false and `0x01` for true |
| `int32` | 4 | Big-endian signed 32-bit integer |
| `int64` | 8 | Big-endian signed 64-bit integer |
| `float32` | 4 | Big-endian IEEE 754 binary32 |
| `float64` | 8 | Big-endian IEEE 754 binary64 |

## Unsigned Variable-length Integer

`UVarInt` is an unsigned LEB128-style variable-length 32-bit integer. The low seven bits of each
byte carry data. A set high bit indicates that another byte follows. The least-significant group
is written first.

```text
value = Σ ((byte[i] & 0x7f) << (7 * i))
```

The encoding uses the shortest representation, from one to five bytes. For example, decimal
`300` splits into the seven-bit groups `0x2c` and `0x02`, so its encoded bytes are `ac 02`.
A 32-bit decoder rejects an unterminated sequence after five bytes and checks for overflow.

## Signed Variable-length Integer

`VarInt` first maps a signed 32-bit integer with ZigZag encoding, then writes the result as a
`UVarInt`:

```text
zigzag(n) = (n << 1) XOR (n >> 31)
```

| Original value | Unsigned wire value | Byte |
| ---: | ---: | --- |
| 0 | 0 | `00` |
| -1 | 1 | `01` |
| 1 | 2 | `02` |
| -2 | 3 | `03` |

## Strings and Byte Arrays

| Notation | Format |
| --- | --- |
| `VarString` | `VarInt byte_length` + `byte[byte_length]` |
| `String32` | `int32 byte_length` + `byte[byte_length]` |
| `Binary32` | `int32 byte_length` + `byte[byte_length]` |
| `VarBinary` | `VarInt byte_length` + `byte[byte_length]` |

For `VarString`, `String32`, and `VarBinary`, a length of `-1` represents null; a length of `0`
represents an empty string or byte array. `Binary32` does not accept a negative length.
`VarString` and `String32` contain UTF-8; `Binary32` and `VarBinary` contain raw bytes.
`VarBinary` uses a signed ZigZag length, unlike the unsigned lengths of the binary components in
[FileEncryptionHeader](DataPages/Encryption.md#file-encryption-header).

String lengths count bytes after UTF-8 encoding. For example, `温` occupies three bytes, so its
`VarString` length field is `VarInt(3)`, encoded as `06`. A nullable field preserves the distinction
between null and an empty value.

## Boundaries and Defensive Decoding

Every length-delimited field is contained by a higher-level boundary: a string by its enclosing
record, a Page by its Chunk, and footer objects by metadata offsets or the trailing metadata
length. Before allocating or slicing a buffer for length `N`, a reader validates:

```text
N >= 0                         # except the documented null sentinel
current_offset + N >= current_offset
current_offset + N <= enclosing_end
```

The second comparison detects wraparound when offset arithmetic uses fixed-width integers.
Check these bounds before allocating memory, and apply an implementation-specific size limit
as well: a length can fit within the file and still be too large for the reader to allocate.

Fixed-width values require their complete byte width. A truncated `int64`, unterminated VarInt,
negative non-null length, invalid UTF-8 where a string is required, or bytes left over after a
length-delimited record are format errors.

## Offset Conventions

Offsets stored in metadata are signed 64-bit values but represent non-negative absolute byte
positions. An offset points to the first byte of the named structure: for example,
`offset_of_chunk_header` points to the Chunk marker, and `meta_offset` points to the `0x02`
separator. To find the end of a structure, add its length to its start offset using the bounds checks above.

## Reading Layout Examples

In layout examples, `|` separates alternatives, parentheses group a sequence, `?` marks an
optional field or structure, `*` allows zero or more repetitions, `+` requires one or more
repetitions, and `[n]` means n elements. For example, `(A | B)*` means zero or more entries, each
of which is either A or B. The surrounding description or a preceding length field determines
which case applies. `remaining bytes` means the unused bytes up to the enclosing payload boundary;
for a Page layout, that is the end of its decrypted, decompressed payload.
