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

# Bloom Filter

The optional Bloom filter in `TsFileMetadata` can save an index lookup when a path is absent.
A clear bit rules the path out; all tested bits being set means only “possibly present,” so the
reader still checks the index. This result is reliable only when the query uses the writer's
path encoding and hash behavior, described below.

## File Format

```text
BloomFilter :=
    UVarInt byte_length
    byte    bits[byte_length]
    UVarInt bit_count?           # present when byte_length > 0
    UVarInt hash_function_count? # present when byte_length > 0
```

When `byte_length = 0`, the file has no Bloom filter and the final two fields are absent.

Bit `i` is stored at bit `i mod 8` in `bits[i / 8]`; bits within each byte are numbered from the
least-significant bit. Trailing all-zero bytes are omitted from the file. `bit_count` records the
logical number of bits.

## Filter Input

For a writer using UTF-8, the filter input is the byte representation of the complete timeseries path:

```text
UTF8(join(DeviceID segments, ".") + "." + measurement_id)
```

A null device segment is represented by the ASCII text `null` in the path. The current Java
implementation calls `String.getBytes()` with the platform default charset; that charset is not
stored in the file. The examples below use UTF-8. A reader must match the writer's charset and
hashing behavior; if either is unknown, bypass the Bloom filter and use the metadata index.

## Hashing

TsFile uses the Java `Murmur128Hash` variant of MurmurHash3 x64 128-bit. Its complete 16-byte
blocks use the standard little-endian unsigned-byte assembly and mixing. Its incomplete final
block differs from standard MurmurHash3: **each tail byte is sign-extended from signed 8-bit to
64-bit before shifting**, and the shifted terms are XORed, not ORed.

For a tail of `n = byte_length mod 16` bytes `t`, assemble its words as follows. All shifts and
arithmetic are modulo 64 bits; `signed64(b)` maps byte values `0x80..0xff` to `-128..-1`.

```text
k1 = XOR(signed64(t[j]) << (8 * j))       for j = 0 .. min(n, 8) - 1
k2 = XOR(signed64(t[j]) << (8 * (j - 8))) for j = 8 .. n - 1
```

An empty XOR is zero. Apply the standard tail mixing to `k2` only when `n >= 9`, and to `k1`
only when `n >= 1`; then apply the standard MurmurHash3 length XOR and finalization. Add the
finalized `h1` and `h2` modulo 64 bits and interpret the low 32 bits as a signed integer `hash32`.
This signed-tail behavior must be preserved for existing files; substituting an ordinary
MurmurHash3 library can produce false negatives for non-ASCII paths.

```text
bit_index = (0 if hash32 == -2147483648 else abs(hash32)) mod bit_count
```

The hash seeds, in order, are:

```text
5, 7, 11, 19, 31, 37, 43, 59
```

`hash_function_count` selects the number of seeds used, with a maximum of 8.

### UTF-8 Test Vectors

These vectors use `seed = 5` and `bit_count = 256`. The last row also exercises a complete
16-byte block before the signed tail.

| Path | Signed `hash32` | Bit index |
| --- | ---: | ---: |
| `root.sg.d.s1` | -1267668613 | 133 |
| `root.设备.s1` | 1697017247 | 159 |
| `root.abcdefghijklmnop.设备.s1` | 866805081 | 89 |

For `root.设备.s1`, a standard MurmurHash3 implementation followed by the same final sum instead
produces `1978455175`, selecting bit `135`. A reader using that bit can reject a path actually
present in the file.

## Lookup Procedure

For an exact path lookup, a reader:

1. constructs the path bytes using the same `DeviceID` segment and measurement rules;
2. evaluates the first `hash_function_count` seeds;
3. tests the corresponding positions in the logical `bit_count`-bit vector;
4. returns **definitely absent** if any tested bit is clear, otherwise **possibly present**.

A positive result must still be checked in the metadata index. A negative result is reliable only
when the query uses the same path construction, charset, and signed-tail hash as the writer.
Otherwise, look up the metadata index directly.

## Space and Probability

Let `n` be the number of inserted paths, `m` the logical `bit_count`, and `k` the
`hash_function_count`. The usual Bloom-filter approximation is:

```text
false_positive_probability ≈ (1 - exp(-k * n / m))^k
optimal_k                  ≈ (m / n) * ln(2)
```

These equations explain the writer's space/accuracy tradeoff; they are not additional file fields.
The serialized `bit_count` and `hash_function_count` are authoritative for reading.

## Validation

When `byte_length > 0`, `bit_count` must be positive, `hash_function_count` must be in the range
`1..8`, and `byte_length` must not exceed `ceil(bit_count / 8)`. Omitted trailing zero bytes are
logically reconstructed as zero. The reader rejects a bit-vector length that crosses the
`TsFileMetadata` boundary.
